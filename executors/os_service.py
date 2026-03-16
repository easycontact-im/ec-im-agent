"""OS service executor for managing system services."""

import asyncio
import logging
import platform
import re
import time
from typing import Any

from config import settings
from executors.base import BaseExecutor

logger = logging.getLogger("ec-im-agent.executors.os_service")

DEFAULT_SERVICE_TIMEOUT = 60
MAX_OUTPUT_SIZE = 1_048_576  # 1 MB
MAX_SHUTDOWN_DELAY_SECONDS = 86400  # 1 day

ALLOWED_EXECUTABLES = frozenset({
    "systemctl",
    "sc.exe",
    "service",
    "shutdown",
})

# Slightly more permissive for systemd unit names (allows @ and :)
_SERVICE_NAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._@:\-]{0,252}$")


def _validate_service_name(name: str) -> dict[str, Any] | None:
    """Validate a service name against allowed characters.

    Args:
        name: The service name to validate.

    Returns:
        An error result dict if invalid, or None if the name is valid.
    """
    if not name or not _SERVICE_NAME_RE.match(name):
        return {
            "status": "error",
            "output": None,
            "error": (
                f"Invalid service name: '{name}'. "
                f"Must match ^[a-zA-Z0-9][a-zA-Z0-9._@:-]{{0,252}}$"
            ),
            "exitCode": -1,
            "durationMs": 0,
        }
    return None


class OSServiceExecutor(BaseExecutor):
    """Manage system services via systemctl (Linux) or sc.exe (Windows).

    Supported actions:
    - restartService: Restart a system service.
    - stopService: Stop a system service.
    - startService: Start a system service.
    - restartOS: Restart the operating system (requires explicit approval).
    """

    async def execute(
        self,
        action: str,
        connection_id: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Dispatch to the appropriate OS service action handler.

        Args:
            action: One of the supported OS service actions.
            connection_id: Unused for local OS operations.
            params: Action parameters (serviceName, approved, timeout).

        Returns:
            Result dict with command output.
        """
        handlers = {
            "restartService": self._restart_service,
            "stopService": self._stop_service,
            "startService": self._start_service,
            "restartOS": self._restart_os,
        }

        handler = handlers.get(action)
        if handler is None:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown OS action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

        return await handler(params)

    def _is_windows(self) -> bool:
        """Check if the current platform is Windows."""
        return platform.system() == "Windows"

    async def _run_command(
        self,
        cmd: list[str],
        timeout: int = DEFAULT_SERVICE_TIMEOUT,
    ) -> dict[str, Any]:
        """Execute an OS command and return the result.

        Args:
            cmd: Command as list of strings. Never uses shell=True.
            timeout: Command timeout in seconds.

        Returns:
            Result dict with stdout/stderr output.
        """
        start = time.monotonic_ns()

        # H6: Validate that the executable is in the allowed whitelist
        if cmd[0] not in ALLOWED_EXECUTABLES:
            logger.error("Blocked execution of disallowed command: %s", cmd[0])
            return {
                "status": "error",
                "output": None,
                "error": f"Command '{cmd[0]}' is not in the allowed executables list",
                "exitCode": -1,
                "durationMs": 0,
            }

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
                    "error": f"Command timed out after {timeout}s",
                    "exitCode": -1,
                    "durationMs": duration_ms,
                }

            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            exit_code = process.returncode or 0

            stdout = stdout_bytes.decode("utf-8", errors="replace")[:MAX_OUTPUT_SIZE]
            stderr = stderr_bytes.decode("utf-8", errors="replace")[:MAX_OUTPUT_SIZE]

            return {
                "status": "success" if exit_code == 0 else "error",
                "output": {
                    "stdout": stdout,
                    "stderr": stderr,
                },
                "error": stderr if exit_code != 0 else None,
                "exitCode": exit_code,
                "durationMs": duration_ms,
            }

        except FileNotFoundError as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "status": "error",
                "output": None,
                "error": f"Command not found: {cmd[0]}",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("OS command failed: %s", exc)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _restart_service(self, params: dict[str, Any]) -> dict[str, Any]:
        """Restart a system service.

        Args:
            params: Must contain 'serviceName'. Optional: 'timeout'.

        Returns:
            Result dict.
        """
        service_name = params.get("serviceName", "")
        timeout = params.get("timeout", DEFAULT_SERVICE_TIMEOUT)

        if not service_name:
            return {
                "status": "error",
                "output": None,
                "error": "Service name is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_service_name(service_name)
        if validation_error:
            return validation_error

        logger.info("Restarting service: %s", service_name)

        if self._is_windows():
            # sc.exe has no "restart" subcommand — stop then start
            stop_result = await self._run_command(
                ["sc.exe", "stop", service_name], timeout=timeout
            )
            if stop_result["exitCode"] != 0 and "not started" not in (stop_result.get("error") or "").lower():
                return stop_result
            # Brief pause to let service stop
            await asyncio.sleep(2)
            return await self._run_command(
                ["sc.exe", "start", service_name], timeout=timeout
            )
        else:
            return await self._run_command(
                ["systemctl", "restart", service_name], timeout=timeout
            )

    async def _stop_service(self, params: dict[str, Any]) -> dict[str, Any]:
        """Stop a system service.

        Args:
            params: Must contain 'serviceName'. Optional: 'timeout'.

        Returns:
            Result dict.
        """
        service_name = params.get("serviceName", "")
        timeout = params.get("timeout", DEFAULT_SERVICE_TIMEOUT)

        if not service_name:
            return {
                "status": "error",
                "output": None,
                "error": "Service name is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_service_name(service_name)
        if validation_error:
            return validation_error

        if self._is_windows():
            cmd = ["sc.exe", "stop", service_name]
        else:
            cmd = ["systemctl", "stop", service_name]

        logger.info("Stopping service: %s", service_name)
        return await self._run_command(cmd, timeout=timeout)

    async def _start_service(self, params: dict[str, Any]) -> dict[str, Any]:
        """Start a system service.

        Args:
            params: Must contain 'serviceName'. Optional: 'timeout'.

        Returns:
            Result dict.
        """
        service_name = params.get("serviceName", "")
        timeout = params.get("timeout", DEFAULT_SERVICE_TIMEOUT)

        if not service_name:
            return {
                "status": "error",
                "output": None,
                "error": "Service name is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_service_name(service_name)
        if validation_error:
            return validation_error

        if self._is_windows():
            cmd = ["sc.exe", "start", service_name]
        else:
            cmd = ["systemctl", "start", service_name]

        logger.info("Starting service: %s", service_name)
        return await self._run_command(cmd, timeout=timeout)

    async def _restart_os(self, params: dict[str, Any]) -> dict[str, Any]:
        """Restart the operating system.

        Requires explicit approval via the 'approved' parameter to prevent
        accidental reboots. Also requires ALLOW_OS_RESTART=true in agent config.

        Args:
            params: Must contain 'approved' (bool). Optional: 'delaySeconds'.

        Returns:
            Result dict.
        """
        # M3: Config guard — OS restart must be explicitly enabled in agent config
        if not settings.ALLOW_OS_RESTART:
            return {
                "status": "error",
                "output": None,
                "error": "OS restart is disabled. Set ALLOW_OS_RESTART=true in agent config to enable.",
                "exitCode": -1,
                "durationMs": 0,
            }

        approved = params.get("approved", False)

        if not approved:
            return {
                "status": "error",
                "output": None,
                "error": "OS restart requires explicit approval (approved=true)",
                "exitCode": -1,
                "durationMs": 0,
            }

        # M2: Validate delaySeconds is a non-negative integer
        delay_seconds = params.get("delaySeconds", 0)
        try:
            delay_seconds = int(delay_seconds)
            if delay_seconds < 0:
                delay_seconds = 0
        except (ValueError, TypeError):
            delay_seconds = 0

        if delay_seconds > MAX_SHUTDOWN_DELAY_SECONDS:
            return {
                "status": "error",
                "output": None,
                "error": f"Shutdown delay {delay_seconds}s exceeds maximum allowed ({MAX_SHUTDOWN_DELAY_SECONDS}s / 1 day)",
                "exitCode": -1,
                "durationMs": 0,
            }

        if self._is_windows():
            cmd = ["shutdown", "/r", "/t", str(delay_seconds)]
        else:
            if delay_seconds > 0:
                # Linux shutdown -r expects minutes; round up to avoid truncation
                # (e.g. 59s should be +1, not +0 which means immediate)
                delay_minutes = max(1, -(-delay_seconds // 60))  # ceiling division
                cmd = ["shutdown", "-r", f"+{delay_minutes}"]
            else:
                cmd = ["shutdown", "-r", "now"]

        logger.warning("Initiating OS restart (approved=true, delay=%ds)", delay_seconds)
        return await self._run_command(cmd, timeout=30)
