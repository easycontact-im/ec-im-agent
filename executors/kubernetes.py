"""Kubernetes executor using kubectl subprocess for cluster operations."""

import asyncio
import logging
import os
import re
import tempfile
import time
from typing import Any

from executors.base import BaseExecutor, truncate_output

logger = logging.getLogger("ec-im-agent.executors.kubernetes")

DEFAULT_K8S_TIMEOUT = 60
MAX_OUTPUT_SIZE = 1_048_576  # 1 MB
MAX_KUBECONFIG_SIZE = 1_048_576  # 1 MB
MAX_MULTI_POD_LOGS = 10          # Max pods for regex/selector mode
MAX_PARALLEL_LOG_FETCHES = 5     # Concurrent kubectl logs calls
MAX_REGEX_PATTERN_LENGTH = 200   # ReDoS protection
MAX_LABEL_SELECTOR_LENGTH = 500
MAX_REPLICAS = 10000

# RFC 1123 DNS label: lowercase alphanumeric, hyphens allowed in the middle, max 253 chars
_RESOURCE_NAME_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,251}[a-z0-9])?$")

# Whitelist for label selector characters (prevents injection)
_LABEL_SELECTOR_RE = re.compile(r"^[a-zA-Z0-9_./ \-=!,()]+$")


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


def _validate_regex_pattern(pattern: str) -> tuple[re.Pattern[str] | None, dict[str, Any] | None]:
    """Validate and compile a pod name regex pattern.

    Args:
        pattern: The regex pattern to validate.

    Returns:
        Tuple of (compiled_pattern, None) on success, or (None, error_dict) on failure.
    """
    if not pattern:
        return None, {
            "status": "error",
            "output": None,
            "error": "Pod pattern cannot be empty",
            "exitCode": -1,
            "durationMs": 0,
        }
    if len(pattern) > MAX_REGEX_PATTERN_LENGTH:
        return None, {
            "status": "error",
            "output": None,
            "error": f"Pod pattern too long: {len(pattern)} chars (max {MAX_REGEX_PATTERN_LENGTH})",
            "exitCode": -1,
            "durationMs": 0,
        }
    try:
        compiled = re.compile(pattern)
        return compiled, None
    except re.error as exc:
        return None, {
            "status": "error",
            "output": None,
            "error": f"Invalid regex pattern '{pattern}': {exc}",
            "exitCode": -1,
            "durationMs": 0,
        }


def _validate_label_selector(selector: str) -> dict[str, Any] | None:
    """Validate a Kubernetes label selector string.

    Args:
        selector: The label selector to validate (e.g. 'app=ec-ui,tier=frontend').

    Returns:
        An error result dict if invalid, or None if valid.
    """
    if not selector:
        return {
            "status": "error",
            "output": None,
            "error": "Label selector cannot be empty",
            "exitCode": -1,
            "durationMs": 0,
        }
    if len(selector) > MAX_LABEL_SELECTOR_LENGTH:
        return {
            "status": "error",
            "output": None,
            "error": f"Label selector too long: {len(selector)} chars (max {MAX_LABEL_SELECTOR_LENGTH})",
            "exitCode": -1,
            "durationMs": 0,
        }
    if not _LABEL_SELECTOR_RE.match(selector):
        return {
            "status": "error",
            "output": None,
            "error": (
                f"Invalid label selector: '{selector}'. "
                f"Only alphanumeric, '.', '_', '/', '-', '=', '!', ',', '(', ')', and spaces are allowed."
            ),
            "exitCode": -1,
            "durationMs": 0,
        }
    return None


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

    def _resolve_kubeconfig(self, connection_id: str | None) -> tuple[str | None, bool]:
        """Resolve kubeconfig — write vault content to secure temp file, or use legacy path.

        Returns:
            Tuple of (kubeconfig_path, is_temp).
            is_temp=True means the caller MUST clean up the file after use.

        Raises:
            ValueError: If credential content is too large or legacy path is invalid.
        """
        if not connection_id:
            return None, False

        credentials = self.vault.get_credential(connection_id)
        if credentials is None:
            return None, False

        # Primary: kubeconfig YAML content from vault → secure temp file
        kubeconfig_content = credentials.get("kubeconfig")
        if kubeconfig_content:
            content_size = len(kubeconfig_content.encode("utf-8"))
            if content_size > MAX_KUBECONFIG_SIZE:
                raise ValueError(
                    f"kubeconfig content too large: {content_size} bytes (max {MAX_KUBECONFIG_SIZE})"
                )
            fd, path = tempfile.mkstemp(suffix=".yaml", prefix="kubeconfig_")
            try:
                os.fchmod(fd, 0o600)
            except (AttributeError, OSError):
                # fchmod not available on Windows; best-effort
                pass
            with os.fdopen(fd, "w") as f:
                f.write(kubeconfig_content)
            logger.debug("Wrote kubeconfig content to temp file: %s", path)
            return path, True

        # Legacy fallback: kubeconfigPath (for existing connections that stored a file path)
        kubeconfig_path = credentials.get("kubeconfigPath")
        if kubeconfig_path:
            if ".." in kubeconfig_path or not os.path.isabs(kubeconfig_path):
                raise ValueError(
                    "Invalid kubeconfig path: must be absolute with no '..' components"
                )
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

            try:
                file_size = os.path.getsize(kubeconfig_path)
                if file_size > MAX_KUBECONFIG_SIZE:
                    raise ValueError(
                        f"kubeconfig file too large: {file_size} bytes (max {MAX_KUBECONFIG_SIZE})"
                    )
            except OSError as exc:
                raise ValueError(f"Cannot read kubeconfig file: {exc}")

            return kubeconfig_path, False

        return None, False

    @staticmethod
    def _cleanup_temp_kubeconfig(path: str | None, is_temp: bool) -> None:
        """Remove a temporary kubeconfig file if applicable."""
        if is_temp and path:
            try:
                os.unlink(path)
            except OSError:
                pass

    @staticmethod
    def _resolve_connection_params(params: dict[str, Any]) -> tuple[str | None, str]:
        """Extract context and namespace from params or nested connectionConfig.

        The backend passes connection config as ``connectionConfig`` in test jobs,
        and as top-level keys for regular jobs (merged by job_dispatcher).

        Returns:
            Tuple of (context, namespace).
        """
        conn_cfg = params.get("connectionConfig") or {}
        context = params.get("context") or conn_cfg.get("context") or None
        namespace = params.get("namespace") or conn_cfg.get("namespace") or "default"
        return context, namespace

    def _build_base_cmd(
        self,
        kubeconfig_path: str | None,
        context: str | None = None,
        is_temp: bool = False,
    ) -> list[str]:
        """Build the base kubectl command with optional kubeconfig and context.

        For non-temp (legacy path) files, re-verifies the kubeconfig path hasn't
        been replaced by a symlink since the initial validation (TOCTOU mitigation).
        Temp files are created by us and don't need symlink checks.

        Args:
            kubeconfig_path: Optional path to kubeconfig file.
            context: Optional Kubernetes context to use.
            is_temp: Whether the kubeconfig is a temp file we created.

        Returns:
            Base command list starting with 'kubectl'.

        Raises:
            ValueError: If the kubeconfig path changed between validation and use.
        """
        cmd = ["kubectl"]
        if kubeconfig_path:
            if not is_temp:
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
        if context:
            cmd.extend(["--context", context])
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
            params: Must contain 'deployment'. Optional: 'namespace', 'context', 'timeout'.

        Returns:
            Result dict.
        """
        kubeconfig, is_temp = self._resolve_kubeconfig(connection_id)
        try:
            context, namespace = self._resolve_connection_params(params)
            deployment = params.get("deployment", "")
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

            cmd = self._build_base_cmd(kubeconfig, context=context, is_temp=is_temp)
            cmd.extend([
                "rollout", "restart", f"deployment/{deployment}",
                "-n", namespace,
            ])

            logger.info("Restarting deployment: %s in namespace: %s", deployment, namespace)
            return await self._run_kubectl(cmd, timeout=timeout)
        finally:
            self._cleanup_temp_kubeconfig(kubeconfig, is_temp)

    async def _scale_deployment(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Scale a Kubernetes deployment to a specified replica count.

        Args:
            connection_id: Connection ID for kubeconfig lookup.
            params: Must contain 'deployment', 'replicas'. Optional: 'namespace', 'context', 'timeout'.

        Returns:
            Result dict.
        """
        kubeconfig, is_temp = self._resolve_kubeconfig(connection_id)
        try:
            context, namespace = self._resolve_connection_params(params)
            deployment = params.get("deployment", "")
            replicas = params.get("replicas")
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

            if replicas > MAX_REPLICAS:
                return {
                    "status": "error",
                    "output": None,
                    "error": f"Replica count {replicas} exceeds maximum allowed ({MAX_REPLICAS})",
                    "exitCode": -1,
                    "durationMs": 0,
                }

            cmd = self._build_base_cmd(kubeconfig, context=context, is_temp=is_temp)
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
        finally:
            self._cleanup_temp_kubeconfig(kubeconfig, is_temp)

    async def _delete_pod(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Delete a specific Kubernetes pod.

        Args:
            connection_id: Connection ID for kubeconfig lookup.
            params: Must contain 'pod'. Optional: 'namespace', 'context', 'timeout', 'gracePeriod'.

        Returns:
            Result dict.
        """
        kubeconfig, is_temp = self._resolve_kubeconfig(connection_id)
        try:
            context, namespace = self._resolve_connection_params(params)
            pod = params.get("pod", "")
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

            cmd = self._build_base_cmd(kubeconfig, context=context, is_temp=is_temp)
            cmd.extend(["delete", "pod", pod, "-n", namespace])

            if grace_period is not None:
                grace_period, validation_error = _validate_positive_int(grace_period, "gracePeriod")
                if validation_error:
                    return validation_error
                cmd.extend([f"--grace-period={grace_period}"])

            logger.info("Deleting pod %s in namespace %s", pod, namespace)
            return await self._run_kubectl(cmd, timeout=timeout)
        finally:
            self._cleanup_temp_kubeconfig(kubeconfig, is_temp)

    async def _rollback_deployment(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Undo the last rollout of a Kubernetes deployment.

        Args:
            connection_id: Connection ID for kubeconfig lookup.
            params: Must contain 'deployment'. Optional: 'namespace', 'context', 'timeout', 'revision'.

        Returns:
            Result dict.
        """
        kubeconfig, is_temp = self._resolve_kubeconfig(connection_id)
        try:
            context, namespace = self._resolve_connection_params(params)
            deployment = params.get("deployment", "")
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

            cmd = self._build_base_cmd(kubeconfig, context=context, is_temp=is_temp)
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
        finally:
            self._cleanup_temp_kubeconfig(kubeconfig, is_temp)

    async def _list_pods(
        self,
        kubeconfig: str | None,
        context: str | None,
        namespace: str,
        is_temp: bool,
        timeout: int,
        label_selector: str | None = None,
    ) -> tuple[list[str] | None, dict[str, Any] | None]:
        """List pod names in a namespace, optionally filtered by label selector.

        Args:
            kubeconfig: Path to kubeconfig file.
            context: Kubernetes context.
            namespace: Target namespace.
            is_temp: Whether kubeconfig is a temp file.
            timeout: Command timeout in seconds.
            label_selector: Optional label selector (e.g. 'app=ec-ui').

        Returns:
            Tuple of (pod_name_list, None) on success, or (None, error_dict) on failure.
        """
        cmd = self._build_base_cmd(kubeconfig, context=context, is_temp=is_temp)
        cmd.extend(["get", "pods", "-n", namespace, "-o", "name"])
        if label_selector:
            cmd.extend(["-l", label_selector])

        result = await self._run_kubectl(cmd, timeout=timeout)
        if result["status"] != "success":
            return None, result

        output = result.get("output", "")
        raw_text = output if isinstance(output, str) else output.get("stdout", "") if isinstance(output, dict) else ""
        # kubectl output: "pod/name-xxx\npod/name-yyy\n"
        pod_names = []
        for line in raw_text.strip().splitlines():
            line = line.strip()
            if line:
                # Strip "pod/" prefix
                pod_names.append(line.removeprefix("pod/"))
        return pod_names, None

    async def _get_logs_for_single_pod(
        self,
        kubeconfig: str | None,
        context: str | None,
        namespace: str,
        pod: str,
        container: str | None,
        tail_lines: int | None,
        since_seconds: int | None,
        is_temp: bool,
        timeout: int,
    ) -> dict[str, Any]:
        """Retrieve logs from a single Kubernetes pod.

        Args:
            kubeconfig: Path to kubeconfig file.
            context: Kubernetes context.
            namespace: Target namespace.
            pod: Pod name.
            container: Optional container name.
            tail_lines: Optional number of tail lines.
            since_seconds: Optional since duration in seconds.
            is_temp: Whether kubeconfig is a temp file.
            timeout: Command timeout in seconds.

        Returns:
            Result dict with log output.
        """
        cmd = self._build_base_cmd(kubeconfig, context=context, is_temp=is_temp)
        cmd.extend(["logs", pod, "-n", namespace])

        if container:
            cmd.extend(["-c", container])
        if tail_lines is not None:
            cmd.extend([f"--tail={tail_lines}"])
        if since_seconds is not None:
            cmd.extend([f"--since={since_seconds}s"])

        return await self._run_kubectl(cmd, timeout=timeout)

    async def _get_logs_multi(
        self,
        kubeconfig: str | None,
        context: str | None,
        namespace: str,
        pod_names: list[str],
        container: str | None,
        tail_lines: int | None,
        since_seconds: int | None,
        is_temp: bool,
        timeout: int,
        mode: str,
    ) -> dict[str, Any]:
        """Retrieve logs from multiple pods in parallel with a concurrency semaphore.

        Args:
            kubeconfig: Path to kubeconfig file.
            context: Kubernetes context.
            namespace: Target namespace.
            pod_names: List of pod names to fetch logs from.
            container: Optional container name.
            tail_lines: Optional number of tail lines.
            since_seconds: Optional since duration in seconds.
            is_temp: Whether kubeconfig is a temp file.
            timeout: Command timeout in seconds.
            mode: 'regex' or 'selector' (for output metadata).

        Returns:
            Result dict with multi-pod output format.
        """
        start = time.monotonic_ns()
        semaphore = asyncio.Semaphore(MAX_PARALLEL_LOG_FETCHES)

        async def fetch_one(pod_name: str) -> dict[str, Any]:
            async with semaphore:
                result = await self._get_logs_for_single_pod(
                    kubeconfig, context, namespace, pod_name,
                    container, tail_lines, since_seconds, is_temp, timeout,
                )
                output = result.get("output", "")
                if isinstance(output, dict):
                    logs_text = output.get("stdout", "")
                else:
                    logs_text = output or ""
                return {
                    "name": pod_name,
                    "logs": logs_text,
                    "error": result.get("error"),
                }

        pod_results = await asyncio.gather(
            *(fetch_one(name) for name in pod_names),
            return_exceptions=True,
        )

        duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

        pods_output = []
        has_error = False
        for i, res in enumerate(pod_results):
            if isinstance(res, Exception):
                has_error = True
                pods_output.append({
                    "name": pod_names[i],
                    "logs": "",
                    "error": str(res),
                })
            else:
                if res.get("error"):
                    has_error = True
                pods_output.append(res)

        all_failed = has_error and all(p.get("error") for p in pods_output)
        partial_failure = has_error and not all_failed

        if all_failed:
            status = "error"
            error_msg = "All pod log fetches failed"
            exit_code = 1
        elif partial_failure:
            failed_pods = [p["name"] for p in pods_output if p.get("error")]
            status = "success"
            error_msg = f"Partial failure: logs unavailable for pod(s): {', '.join(failed_pods)}"
            exit_code = 0
        else:
            status = "success"
            error_msg = None
            exit_code = 0

        return {
            "status": status,
            "output": {
                "pods": pods_output,
                "matchedCount": len(pod_names),
                "mode": mode,
                "partialFailure": partial_failure,
            },
            "error": error_msg,
            "exitCode": exit_code,
            "durationMs": duration_ms,
        }

    async def _get_logs(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Retrieve logs from Kubernetes pods. Supports three modes:

        - Exact: single pod by name (``pod`` param)
        - Regex: multiple pods matching a pattern (``podPattern`` param)
        - Label selector: multiple pods matching a selector (``selector`` param)

        Exactly one of ``pod``, ``podPattern``, or ``selector`` must be provided.

        Args:
            connection_id: Connection ID for kubeconfig lookup.
            params: Must contain exactly one of 'pod', 'podPattern', 'selector'.
                    Optional: 'namespace', 'context', 'container', 'tailLines',
                    'sinceSeconds', 'timeout'.

        Returns:
            Result dict with log output.
        """
        kubeconfig, is_temp = self._resolve_kubeconfig(connection_id)
        try:
            context, namespace = self._resolve_connection_params(params)
            pod = params.get("pod", "").strip() if params.get("pod") else ""
            pod_pattern = params.get("podPattern", "").strip() if params.get("podPattern") else ""
            selector = params.get("selector", "").strip() if params.get("selector") else ""
            container = params.get("container")
            tail_lines = params.get("tailLines")
            since_seconds = params.get("sinceSeconds")
            timeout = params.get("timeout", DEFAULT_K8S_TIMEOUT)

            # Exactly one mode must be specified
            mode_count = sum(1 for v in [pod, pod_pattern, selector] if v)
            if mode_count == 0:
                return {
                    "status": "error",
                    "output": None,
                    "error": "One of 'pod', 'podPattern', or 'selector' is required",
                    "exitCode": -1,
                    "durationMs": 0,
                }
            if mode_count > 1:
                return {
                    "status": "error",
                    "output": None,
                    "error": "Only one of 'pod', 'podPattern', or 'selector' may be specified",
                    "exitCode": -1,
                    "durationMs": 0,
                }

            # Validate namespace
            validation_error = _validate_resource_name(namespace, "namespace")
            if validation_error:
                return validation_error

            # Validate optional container
            if container:
                validation_error = _validate_resource_name(container, "container")
                if validation_error:
                    return validation_error

            # Validate optional numeric params
            if tail_lines is not None:
                tail_lines, validation_error = _validate_positive_int(tail_lines, "tailLines")
                if validation_error:
                    return validation_error
            if since_seconds is not None:
                since_seconds, validation_error = _validate_positive_int(since_seconds, "sinceSeconds")
                if validation_error:
                    return validation_error

            # --- Exact mode (backward compatible) ---
            if pod:
                validation_error = _validate_resource_name(pod, "pod")
                if validation_error:
                    return validation_error

                logger.info("Getting logs for pod %s in namespace %s", pod, namespace)
                return await self._get_logs_for_single_pod(
                    kubeconfig, context, namespace, pod,
                    container, tail_lines, since_seconds, is_temp, timeout,
                )

            # --- Regex mode ---
            if pod_pattern:
                compiled, validation_error = _validate_regex_pattern(pod_pattern)
                if validation_error:
                    return validation_error

                logger.info("Listing pods in namespace %s for pattern %s", namespace, pod_pattern)
                all_pods, list_error = await self._list_pods(
                    kubeconfig, context, namespace, is_temp, timeout,
                )
                if list_error:
                    return list_error

                matched = [p for p in all_pods if compiled.search(p)]

                if not matched:
                    return {
                        "status": "error",
                        "output": None,
                        "error": f"No pods matched pattern '{pod_pattern}' in namespace '{namespace}'",
                        "exitCode": -1,
                        "durationMs": 0,
                    }
                if len(matched) > MAX_MULTI_POD_LOGS:
                    return {
                        "status": "error",
                        "output": None,
                        "error": (
                            f"Pattern '{pod_pattern}' matched {len(matched)} pods, "
                            f"exceeding limit of {MAX_MULTI_POD_LOGS}. Narrow your pattern."
                        ),
                        "exitCode": -1,
                        "durationMs": 0,
                    }

                logger.info(
                    "Fetching logs for %d pods matching '%s' in namespace %s",
                    len(matched), pod_pattern, namespace,
                )
                return await self._get_logs_multi(
                    kubeconfig, context, namespace, matched,
                    container, tail_lines, since_seconds, is_temp, timeout,
                    mode="regex",
                )

            # --- Label selector mode ---
            validation_error = _validate_label_selector(selector)
            if validation_error:
                return validation_error

            logger.info("Listing pods with selector '%s' in namespace %s", selector, namespace)
            matched_pods, list_error = await self._list_pods(
                kubeconfig, context, namespace, is_temp, timeout,
                label_selector=selector,
            )
            if list_error:
                return list_error

            if not matched_pods:
                return {
                    "status": "error",
                    "output": None,
                    "error": f"No pods matched selector '{selector}' in namespace '{namespace}'",
                    "exitCode": -1,
                    "durationMs": 0,
                }
            if len(matched_pods) > MAX_MULTI_POD_LOGS:
                return {
                    "status": "error",
                    "output": None,
                    "error": (
                        f"Selector '{selector}' matched {len(matched_pods)} pods, "
                        f"exceeding limit of {MAX_MULTI_POD_LOGS}. Narrow your selector."
                    ),
                    "exitCode": -1,
                    "durationMs": 0,
                }

            logger.info(
                "Fetching logs for %d pods with selector '%s' in namespace %s",
                len(matched_pods), selector, namespace,
            )
            return await self._get_logs_multi(
                kubeconfig, context, namespace, matched_pods,
                container, tail_lines, since_seconds, is_temp, timeout,
                mode="selector",
            )
        finally:
            self._cleanup_temp_kubeconfig(kubeconfig, is_temp)

    async def _test_connection(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Test Kubernetes connectivity by running 'kubectl cluster-info'.

        Args:
            connection_id: Connection ID for kubeconfig lookup.
            params: Optional 'context', 'connectionConfig', 'timeout'.

        Returns:
            Result dict indicating cluster connectivity.
        """
        kubeconfig, is_temp = self._resolve_kubeconfig(connection_id)
        try:
            context, _ = self._resolve_connection_params(params)
            timeout = params.get("timeout", DEFAULT_K8S_TIMEOUT)

            cmd = self._build_base_cmd(kubeconfig, context=context, is_temp=is_temp)
            cmd.extend(["cluster-info"])

            logger.info("Testing Kubernetes connection")
            result = await self._run_kubectl(cmd, timeout=timeout)

            if result["status"] == "success":
                if isinstance(result["output"], dict):
                    result["output"]["message"] = "Kubernetes connection successful"
                else:
                    result["output"] = {
                        "message": "Kubernetes connection successful",
                        "clusterInfo": result["output"],
                    }

            return result
        finally:
            self._cleanup_temp_kubeconfig(kubeconfig, is_temp)
