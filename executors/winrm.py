"""WinRM executor using pywinrm for remote PowerShell execution on Windows hosts."""

import asyncio
import logging
import time
from typing import Any

import winrm

from executors.base import BaseExecutor, truncate_output

logger = logging.getLogger("ec-im-agent.executors.winrm")

DEFAULT_WINRM_PORT_SSL = 5986
DEFAULT_WINRM_PORT_HTTP = 5985
DEFAULT_WINRM_TIMEOUT = 30
DEFAULT_WINRM_CONNECT_TIMEOUT = 15
MAX_OUTPUT_SIZE = 1_048_576  # 1 MB


def _friendly_error(exc: Exception, host: str, port: int, use_ssl: bool) -> str:
    """Convert raw WinRM exceptions into actionable error messages."""
    msg = str(exc)
    scheme = "HTTPS" if use_ssl else "HTTP"

    if "NewConnectionError" in msg or "Max retries" in msg or "10061" in msg or "Connection refused" in msg:
        return (
            f"Connection refused at {host}:{port} ({scheme}). "
            f"Verify that: (1) WinRM is enabled on the target — run "
            f"'Enable-PSRemoting -Force' in an elevated PowerShell, "
            f"(2) the WinRM {scheme} listener exists — run "
            f"'winrm enumerate winrm/config/listener', "
            f"(3) port {port} is open in Windows Firewall"
        )

    if "401" in msg or "Unauthorized" in msg or "InvalidCredential" in msg or "credentials were rejected" in msg:
        return (
            f"Authentication failed for {host}:{port}. "
            f"Check username/password and ensure the user has WinRM access. "
            f"Username must include the machine or domain name — "
            f"use '.\\Username' for local accounts or 'DOMAIN\\Username' for domain accounts."
        )

    if "500" in msg and "WinRMTransport" in msg:
        return (
            f"WinRM transport error at {host}:{port}. "
            f"If using HTTPS, verify the target has an SSL certificate configured for WinRM."
        )

    if "timed out" in msg.lower() or "TimeoutError" in msg:
        return (
            f"Connection timed out at {host}:{port}. "
            f"Check network connectivity and firewall rules."
        )

    return f"WinRM error at {host}:{port} ({scheme}): {exc}"


class WinRMExecutor(BaseExecutor):
    """Execute PowerShell commands and scripts on remote Windows hosts via WinRM.

    Supported actions:
    - testConnection: Verify WinRM connectivity.
    - executeCommand: Run a single PowerShell command.
    - executeScript: Run a multi-line PowerShell script.
    """

    async def execute(
        self,
        action: str,
        connection_id: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Dispatch to the appropriate WinRM action handler.

        Args:
            action: 'testConnection', 'executeCommand', or 'executeScript'.
            connection_id: Connection ID for WinRM credential lookup.
            params: Action parameters (command, script, timeout, etc.).

        Returns:
            Result dict with stdout/stderr output.
        """
        if action == "testConnection":
            return await self._test_connection(connection_id, params)
        elif action == "executeCommand":
            return await self._execute_command(connection_id, params)
        elif action == "executeScript":
            return await self._execute_script(connection_id, params)
        else:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown WinRM action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

    def _get_credentials(self, connection_id: str | None) -> dict[str, Any] | None:
        """Retrieve WinRM credentials from the vault.

        Args:
            connection_id: The connection ID to look up.

        Returns:
            Credential dict or None if not found.
        """
        if not connection_id:
            return None
        return self.vault.get_credential(connection_id)

    def _build_session(self, credentials: dict[str, Any]) -> "winrm.Session":
        """Build a pywinrm Session from vault credentials.

        Args:
            credentials: Dict with host, port, username, password, useSsl, transport.

        Returns:
            A configured winrm.Session instance.
        """
        host = credentials["host"]
        use_ssl = credentials.get("useSsl", False)
        default_port = DEFAULT_WINRM_PORT_SSL if use_ssl else DEFAULT_WINRM_PORT_HTTP
        port = credentials.get("port", default_port)
        scheme = "https" if use_ssl else "http"
        endpoint = f"{scheme}://{host}:{port}/wsman"

        username = credentials.get("username", "")
        password = credentials.get("password", "")
        transport = credentials.get("transport", "ntlm")

        session_kwargs: dict[str, Any] = {
            "read_timeout_sec": DEFAULT_WINRM_TIMEOUT,
            "operation_timeout_sec": DEFAULT_WINRM_TIMEOUT - 5,
        }

        # Only set server_cert_validation for HTTPS connections
        if use_ssl:
            session_kwargs["server_cert_validation"] = "ignore"

        session = winrm.Session(
            endpoint,
            auth=(username, password),
            transport=transport,
            **session_kwargs,
        )
        return session

    async def _run_ps(
        self,
        session: "winrm.Session",
        script: str,
        timeout: int = DEFAULT_WINRM_TIMEOUT,
    ) -> "winrm.Response":
        """Run a PowerShell script via WinRM, wrapped in asyncio.to_thread.

        Args:
            session: A configured winrm.Session.
            script: PowerShell script content.
            timeout: Execution timeout in seconds.

        Returns:
            winrm.Response with status_code, std_out, std_err.
        """
        return await asyncio.wait_for(
            asyncio.to_thread(session.run_ps, script),
            timeout=timeout,
        )

    async def _test_connection(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Test WinRM connectivity by running $PSVersionTable.PSVersion.

        Args:
            connection_id: Connection ID for credential lookup.
            params: Optional 'timeout'.

        Returns:
            Result dict indicating connection success or failure.
        """
        credentials = self._get_credentials(connection_id)
        if credentials is None:
            return {
                "status": "error",
                "output": None,
                "error": f"No credentials found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        timeout = params.get("timeout", DEFAULT_WINRM_CONNECT_TIMEOUT)
        start = time.monotonic_ns()
        try:
            session = self._build_session(credentials)
            result = await self._run_ps(
                session,
                "$PSVersionTable.PSVersion | Out-String",
                timeout=timeout,
            )

            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            stdout = (result.std_out or b"").decode("utf-8", errors="replace").strip()

            use_ssl = credentials.get("useSsl", False)
            default_port = DEFAULT_WINRM_PORT_SSL if use_ssl else DEFAULT_WINRM_PORT_HTTP
            conn_port = credentials.get("port", default_port)

            if result.status_code == 0:
                return {
                    "status": "success",
                    "output": {
                        "host": credentials.get("host"),
                        "port": conn_port,
                        "username": credentials.get("username"),
                        "psVersion": stdout,
                        "message": "WinRM connection successful",
                    },
                    "error": None,
                    "exitCode": 0,
                    "durationMs": duration_ms,
                }
            else:
                stderr = (result.std_err or b"").decode("utf-8", errors="replace").strip()
                return {
                    "status": "error",
                    "output": None,
                    "error": f"WinRM test command failed: {stderr or stdout}",
                    "exitCode": result.status_code,
                    "durationMs": duration_ms,
                }

        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            host = credentials.get("host", "unknown")
            use_ssl = credentials.get("useSsl", False)
            default_port = DEFAULT_WINRM_PORT_SSL if use_ssl else DEFAULT_WINRM_PORT_HTTP
            port = credentials.get("port", default_port)
            return {
                "status": "error",
                "output": None,
                "error": _friendly_error(exc, host, port, use_ssl),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _execute_command(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Run a PowerShell command on the remote host.

        Args:
            connection_id: Connection ID for credential lookup.
            params: Must contain 'command'. Optional: 'timeout'.

        Returns:
            Result dict with stdout/stderr output.
        """
        credentials = self._get_credentials(connection_id)
        if credentials is None:
            return {
                "status": "error",
                "output": None,
                "error": f"No credentials found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        command = params.get("command", "")
        if not command or not command.strip():
            return {
                "status": "error",
                "output": None,
                "error": "Command is required and cannot be empty",
                "exitCode": -1,
                "durationMs": 0,
            }
        timeout = params.get("timeout", DEFAULT_WINRM_TIMEOUT)

        start = time.monotonic_ns()
        try:
            session = self._build_session(credentials)
            result = await self._run_ps(session, command, timeout=timeout)

            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            exit_code = result.status_code

            raw_stdout = (result.std_out or b"").decode("utf-8", errors="replace")
            raw_stderr = (result.std_err or b"").decode("utf-8", errors="replace")
            output = truncate_output(raw_stdout, raw_stderr, MAX_OUTPUT_SIZE)

            return {
                "status": "success" if exit_code == 0 else "error",
                "output": output,
                "error": raw_stderr if exit_code != 0 else None,
                "exitCode": exit_code,
                "durationMs": duration_ms,
            }

        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error(
                "WinRM command failed for connection %s: %s",
                connection_id, exc,
            )
            host = credentials.get("host", "unknown")
            use_ssl = credentials.get("useSsl", False)
            default_port = DEFAULT_WINRM_PORT_SSL if use_ssl else DEFAULT_WINRM_PORT_HTTP
            port = credentials.get("port", default_port)
            return {
                "status": "error",
                "output": None,
                "error": _friendly_error(exc, host, port, use_ssl),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _execute_script(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Run a multi-line PowerShell script on the remote host.

        Args:
            connection_id: Connection ID for credential lookup.
            params: Must contain 'script'. Optional: 'timeout'.

        Returns:
            Result dict with stdout/stderr output.
        """
        credentials = self._get_credentials(connection_id)
        if credentials is None:
            return {
                "status": "error",
                "output": None,
                "error": f"No credentials found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        script = params.get("script", "")
        if not script or not script.strip():
            return {
                "status": "error",
                "output": None,
                "error": "Script is required and cannot be empty",
                "exitCode": -1,
                "durationMs": 0,
            }
        timeout = params.get("timeout", DEFAULT_WINRM_TIMEOUT)

        start = time.monotonic_ns()
        try:
            session = self._build_session(credentials)
            result = await self._run_ps(session, script, timeout=timeout)

            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            exit_code = result.status_code

            raw_stdout = (result.std_out or b"").decode("utf-8", errors="replace")
            raw_stderr = (result.std_err or b"").decode("utf-8", errors="replace")
            output = truncate_output(raw_stdout, raw_stderr, MAX_OUTPUT_SIZE)

            return {
                "status": "success" if exit_code == 0 else "error",
                "output": output,
                "error": raw_stderr if exit_code != 0 else None,
                "exitCode": exit_code,
                "durationMs": duration_ms,
            }

        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error(
                "WinRM script execution failed for connection %s: %s",
                connection_id, exc,
            )
            host = credentials.get("host", "unknown")
            use_ssl = credentials.get("useSsl", False)
            default_port = DEFAULT_WINRM_PORT_SSL if use_ssl else DEFAULT_WINRM_PORT_HTTP
            port = credentials.get("port", default_port)
            return {
                "status": "error",
                "output": None,
                "error": _friendly_error(exc, host, port, use_ssl),
                "exitCode": -1,
                "durationMs": duration_ms,
            }
