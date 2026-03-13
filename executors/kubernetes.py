"""Kubernetes executor using kubectl subprocess for cluster operations."""

import asyncio
import logging
import os
import re
import time
from typing import Any

from executors.base import BaseExecutor, truncate_output

logger = logging.getLogger("ec-im-agent.executors.kubernetes")

DEFAULT_K8S_TIMEOUT = 60
MAX_OUTPUT_SIZE = 1_048_576  # 1 MB
MAX_KUBECONFIG_SIZE = 1_048_576  # 1 MB

# RFC 1123 DNS label: lowercase alphanumeric, hyphens allowed in the middle, max 253 chars
_RESOURCE_NAME_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,251}[a-z0-9])?$")


def _validate_positive_int(value: Any, param_name: str) -> tuple[int | None, dict[str, Any] | None]:
    """Validate that a parameter is a positive integer.

    Args:
        value: The value to validate (may come as string from JSON).
        param_name: Parameter name for error messages.

    Returns:
        Tuple of (validated_int, None) on success, or (None, error_dict) on failure.
    """
    if value is None:
        return None, None
    try:
        int_val = int(value)
    except (ValueError, TypeError):
        return None, {
            "status": "error",
            "output": None,
            "error": f"Parameter '{param_name}' must be a valid integer, got: {value!r}",
            "exitCode": -1,
            "durationMs": 0,
        }
    if int_val < 0:
        return None, {
            "status": "error",
            "output": None,
            "error": f"Parameter '{param_name}' must be non-negative, got: {int_val}",
            "exitCode": -1,
            "durationMs": 0,
        }
    return int_val, None


def _validate_resource_name(name: str, resource_type: str) -> dict[str, Any] | None:
    """Validate a Kubernetes resource name against allowed characters.

    Args:
        name: The resource name to validate.
        resource_type: Human-readable type (e.g. 'deployment', 'pod') for error messages.

    Returns:
        An error result dict if invalid, or None if the name is valid.
    """
    if not name or not _RESOURCE_NAME_RE.match(name):
        return {
            "status": "error",
            "output": None,
            "error": (
                f"Invalid {resource_type} name: '{name}'. "
                f"Must be a valid RFC 1123 DNS label: lowercase alphanumeric and hyphens, "
                f"must start and end with alphanumeric, max 253 characters."
            ),
            "exitCode": -1,
            "durationMs": 0,
        }
    return None


class KubernetesExecutor(BaseExecutor):
    """Execute Kubernetes operations via kubectl subprocess.

    Supported actions:
    - restartDeployment: Rollout restart a deployment.
    - scaleDeployment: Scale a deployment to a replica count.
    - deletePod: Delete a specific pod.
    - rollbackDeployment: Undo the last deployment rollout.
    - getLogs: Retrieve logs from a pod.
    """

    async def execute(
        self,
        action: str,
        connection_id: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Dispatch to the appropriate kubectl action handler.

        Args:
            action: One of the supported Kubernetes actions.
            connection_id: Connection ID for kubeconfig credential lookup.
            params: Action-specific parameters (namespace, deployment, pod, etc.).

        Returns:
            Result dict with kubectl output.
        """
        handlers = {
            "restartDeployment": self._restart_deployment,
            "scaleDeployment": self._scale_deployment,
            "deletePod": self._delete_pod,
            "rollbackDeployment": self._rollback_deployment,
            "getLogs": self._get_logs,
            "testConnection": self._test_connection,
        }

        handler = handlers.get(action)
        if handler is None:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown Kubernetes action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

        try:
            return await handler(connection_id, params)
        except ValueError as exc:
            # Catches kubeconfig path validation errors (L2)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": 0,
            }

    def _get_kubeconfig_path(self, connection_id: str | None) -> str | None:
        """Get the kubeconfig path from vault credentials.

        Args:
            connection_id: Connection ID for credential lookup.

        Returns:
            Path to kubeconfig file, or None if not found.

        Raises:
            ValueError: If the kubeconfig path contains path traversal or is not absolute.
        """
        if not connection_id:
            return None

        credentials = self.vault.get_credential(connection_id)
        if credentials is None:
            return None

        kubeconfig_path = credentials.get("kubeconfigPath")
        # Validate kubeconfig path to prevent path traversal and symlink attacks
        if kubeconfig_path:
            if '..' in kubeconfig_path or not os.path.isabs(kubeconfig_path):
                raise ValueError(
                    "Invalid kubeconfig path: must be absolute with no '..' components"
                )
            # Resolve symlinks and verify the real path matches expected location
            try:
                real_path = os.path.realpath(kubeconfig_path)
                if real_path != os.path.normpath(kubeconfig_path):
                    logger.warning(
                        "Kubeconfig path %s resolves to %s via symlink",
                        kubeconfig_path, real_path,
                    )
                    raise ValueError(
                        f"Kubeconfig path contains symlinks: {kubeconfig_path} -> {real_path}. "
                        f"Use the real path directly for security."
                    )
            except OSError as exc:
                raise ValueError(f"Cannot resolve kubeconfig path: {exc}")

            # H7: Validate file size to prevent loading excessively large files
            try:
                file_size = os.path.getsize(kubeconfig_path)
                if file_size > MAX_KUBECONFIG_SIZE:
                    raise ValueError(
                        f"kubeconfig file too large: {file_size} bytes (max {MAX_KUBECONFIG_SIZE})"
                    )
            except OSError as exc:
                raise ValueError(f"Cannot read kubeconfig file: {exc}")

        return kubeconfig_path

    def _build_base_cmd(self, kubeconfig_path: str | None) -> list[str]:
        """Build the base kubectl command with optional kubeconfig.

        Re-verifies the kubeconfig path hasn't been replaced by a symlink
        since the initial validation (TOCTOU mitigation).

        Args:
            kubeconfig_path: Optional path to kubeconfig file.

        Returns:
            Base command list starting with 'kubectl'.

        Raises:
            ValueError: If the kubeconfig path changed between validation and use.
        """
        cmd = ["kubectl"]
        if kubeconfig_path:
            # Re-verify path right before execution (TOCTOU mitigation)
            try:
                final_path = os.path.realpath(kubeconfig_path)
            except OSError as exc:
                raise ValueError(f"Cannot resolve kubeconfig path at execution time: {exc}")
            if final_path != os.path.normpath(kubeconfig_path):
                raise ValueError(
                    f"Kubeconfig path changed during execution (possible symlink attack): "
                    f"{kubeconfig_path} -> {final_path}"
                )
            cmd.extend(["--kubeconfig", kubeconfig_path])
        return cmd

    async def _run_kubectl(
        self,
        cmd: list[str],
        timeout: int = DEFAULT_K8S_TIMEOUT,
    ) -> dict[str, Any]:
        """Execute a kubectl command and return the result.

        Args:
            cmd: Complete kubectl command as a list of strings.
            timeout: Command timeout in seconds.

        Returns:
            Result dict with stdout/stderr output.
        """
        start = time.monotonic_ns()

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout_bytes, stderr_bytes = await asyncio.wait_for(
                    process.communicate(),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
                return {
                    "status": "error",
                    "output": None,
                    "error": f"kubectl command timed out after {timeout}s",
                    "exitCode": -1,
                    "durationMs": duration_ms,
                }

            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            exit_code = process.returncode or 0

            raw_stdout = stdout_bytes.decode("utf-8", errors="replace")
            raw_stderr = stderr_bytes.decode("utf-8", errors="replace")
            output = truncate_output(raw_stdout, raw_stderr, MAX_OUTPUT_SIZE)

            return {
                "status": "success" if exit_code == 0 else "error",
                "output": output,
                "error": raw_stderr if exit_code != 0 else None,
                "exitCode": exit_code,
                "durationMs": duration_ms,
            }

        except FileNotFoundError:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "status": "error",
                "output": None,
                "error": "kubectl not found. Ensure kubectl is installed and in PATH.",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("kubectl command failed: %s", exc)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _restart_deployment(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Rollout restart a Kubernetes deployment.

        Args:
            connection_id: Connection ID for kubeconfig lookup.
            params: Must contain 'deployment'. Optional: 'namespace', 'timeout'.

        Returns:
            Result dict.
        """
        kubeconfig = self._get_kubeconfig_path(connection_id)
        deployment = params.get("deployment", "")
        namespace = params.get("namespace", "default")
        timeout = params.get("timeout", DEFAULT_K8S_TIMEOUT)

        if not deployment:
            return {
                "status": "error",
                "output": None,
                "error": "Deployment name is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_resource_name(deployment, "deployment")
        if validation_error:
            return validation_error
        validation_error = _validate_resource_name(namespace, "namespace")
        if validation_error:
            return validation_error

        cmd = self._build_base_cmd(kubeconfig)
        cmd.extend([
            "rollout", "restart", f"deployment/{deployment}",
            "-n", namespace,
        ])

        logger.info("Restarting deployment: %s in namespace: %s", deployment, namespace)
        return await self._run_kubectl(cmd, timeout=timeout)

    async def _scale_deployment(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Scale a Kubernetes deployment to a specified replica count.

        Args:
            connection_id: Connection ID for kubeconfig lookup.
            params: Must contain 'deployment', 'replicas'. Optional: 'namespace', 'timeout'.

        Returns:
            Result dict.
        """
        kubeconfig = self._get_kubeconfig_path(connection_id)
        deployment = params.get("deployment", "")
        replicas = params.get("replicas")
        namespace = params.get("namespace", "default")
        timeout = params.get("timeout", DEFAULT_K8S_TIMEOUT)

        if not deployment:
            return {
                "status": "error",
                "output": None,
                "error": "Deployment name is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_resource_name(deployment, "deployment")
        if validation_error:
            return validation_error
        validation_error = _validate_resource_name(namespace, "namespace")
        if validation_error:
            return validation_error

        if replicas is None:
            return {
                "status": "error",
                "output": None,
                "error": "Replica count is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        # H1: Validate replicas is a valid non-negative integer
        replicas, validation_error = _validate_positive_int(replicas, "replicas")
        if validation_error:
            return validation_error

        cmd = self._build_base_cmd(kubeconfig)
        cmd.extend([
            "scale", f"deployment/{deployment}",
            f"--replicas={replicas}",
            "-n", namespace,
        ])

        logger.info(
            "Scaling deployment %s to %d replicas in namespace %s",
            deployment, replicas, namespace,
        )
        return await self._run_kubectl(cmd, timeout=timeout)

    async def _delete_pod(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Delete a specific Kubernetes pod.

        Args:
            connection_id: Connection ID for kubeconfig lookup.
            params: Must contain 'pod'. Optional: 'namespace', 'timeout', 'gracePeriod'.

        Returns:
            Result dict.
        """
        kubeconfig = self._get_kubeconfig_path(connection_id)
        pod = params.get("pod", "")
        namespace = params.get("namespace", "default")
        timeout = params.get("timeout", DEFAULT_K8S_TIMEOUT)
        grace_period = params.get("gracePeriod")

        if not pod:
            return {
                "status": "error",
                "output": None,
                "error": "Pod name is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_resource_name(pod, "pod")
        if validation_error:
            return validation_error
        validation_error = _validate_resource_name(namespace, "namespace")
        if validation_error:
            return validation_error

        cmd = self._build_base_cmd(kubeconfig)
        cmd.extend(["delete", "pod", pod, "-n", namespace])

        if grace_period is not None:
            grace_period, validation_error = _validate_positive_int(grace_period, "gracePeriod")
            if validation_error:
                return validation_error
            cmd.extend([f"--grace-period={grace_period}"])

        logger.info("Deleting pod %s in namespace %s", pod, namespace)
        return await self._run_kubectl(cmd, timeout=timeout)

    async def _rollback_deployment(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Undo the last rollout of a Kubernetes deployment.

        Args:
            connection_id: Connection ID for kubeconfig lookup.
            params: Must contain 'deployment'. Optional: 'namespace', 'timeout', 'revision'.

        Returns:
            Result dict.
        """
        kubeconfig = self._get_kubeconfig_path(connection_id)
        deployment = params.get("deployment", "")
        namespace = params.get("namespace", "default")
        timeout = params.get("timeout", DEFAULT_K8S_TIMEOUT)
        revision = params.get("revision")

        if not deployment:
            return {
                "status": "error",
                "output": None,
                "error": "Deployment name is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_resource_name(deployment, "deployment")
        if validation_error:
            return validation_error
        validation_error = _validate_resource_name(namespace, "namespace")
        if validation_error:
            return validation_error

        cmd = self._build_base_cmd(kubeconfig)
        cmd.extend([
            "rollout", "undo", f"deployment/{deployment}",
            "-n", namespace,
        ])

        if revision is not None:
            revision, validation_error = _validate_positive_int(revision, "revision")
            if validation_error:
                return validation_error
            cmd.extend([f"--to-revision={revision}"])

        logger.info("Rolling back deployment %s in namespace %s", deployment, namespace)
        return await self._run_kubectl(cmd, timeout=timeout)

    async def _get_logs(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Retrieve logs from a Kubernetes pod.

        Args:
            connection_id: Connection ID for kubeconfig lookup.
            params: Must contain 'pod'. Optional: 'namespace', 'container',
                    'tailLines', 'sinceSeconds', 'timeout'.

        Returns:
            Result dict with log output.
        """
        kubeconfig = self._get_kubeconfig_path(connection_id)
        pod = params.get("pod", "")
        namespace = params.get("namespace", "default")
        container = params.get("container")
        tail_lines = params.get("tailLines")
        since_seconds = params.get("sinceSeconds")
        timeout = params.get("timeout", DEFAULT_K8S_TIMEOUT)

        if not pod:
            return {
                "status": "error",
                "output": None,
                "error": "Pod name is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_resource_name(pod, "pod")
        if validation_error:
            return validation_error
        validation_error = _validate_resource_name(namespace, "namespace")
        if validation_error:
            return validation_error
        if container:
            validation_error = _validate_resource_name(container, "container")
            if validation_error:
                return validation_error

        cmd = self._build_base_cmd(kubeconfig)
        cmd.extend(["logs", pod, "-n", namespace])

        if container:
            cmd.extend(["-c", container])
        if tail_lines is not None:
            tail_lines, validation_error = _validate_positive_int(tail_lines, "tailLines")
            if validation_error:
                return validation_error
            cmd.extend([f"--tail={tail_lines}"])
        if since_seconds is not None:
            since_seconds, validation_error = _validate_positive_int(since_seconds, "sinceSeconds")
            if validation_error:
                return validation_error
            cmd.extend([f"--since={since_seconds}s"])

        logger.info("Getting logs for pod %s in namespace %s", pod, namespace)
        return await self._run_kubectl(cmd, timeout=timeout)

    async def _test_connection(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Test Kubernetes connectivity by running 'kubectl version --short'.

        Args:
            connection_id: Connection ID for kubeconfig lookup.
            params: Optional 'timeout'.

        Returns:
            Result dict indicating cluster connectivity.
        """
        kubeconfig = self._get_kubeconfig_path(connection_id)
        timeout = params.get("timeout", DEFAULT_K8S_TIMEOUT)

        cmd = self._build_base_cmd(kubeconfig)
        cmd.extend(["version", "--client", "-o", "json"])

        logger.info("Testing Kubernetes connection")
        result = await self._run_kubectl(cmd, timeout=timeout)

        if result["status"] == "success":
            result["output"]["message"] = "Kubernetes connection successful"

        return result
