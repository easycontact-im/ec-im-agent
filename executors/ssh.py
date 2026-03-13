"""SSH executor using asyncssh for remote command and script execution."""

import asyncio
import logging
import os
import secrets
import shlex
import time
from pathlib import Path
from typing import Any

import asyncssh

from executors.base import BaseExecutor, truncate_output

logger = logging.getLogger("ec-im-agent.executors.ssh")

DEFAULT_SSH_PORT = 22
DEFAULT_SSH_TIMEOUT = 30
DEFAULT_SSH_CONNECT_TIMEOUT = 15
MAX_OUTPUT_SIZE = 1_048_576  # 1 MB

# Whitelist of allowed interpreters for script execution (C3: command injection prevention)
ALLOWED_INTERPRETERS = frozenset({
    "/bin/bash",
    "/bin/sh",
    "/usr/bin/bash",
    "/usr/bin/sh",
    "/usr/bin/python3",
    "/usr/bin/python",
    "/usr/bin/env",
    "/usr/bin/perl",
    "/usr/bin/ruby",
    "/usr/bin/node",
})


class SSHExecutor(BaseExecutor):
    """Execute commands and scripts on remote hosts via SSH.

    Supported actions:
    - executeCommand: Run a single command on the remote host.
    - executeScript: Upload and run a script on the remote host.
    """

    async def execute(
        self,
        action: str,
        connection_id: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Dispatch to the appropriate SSH action handler.

        Args:
            action: 'executeCommand' or 'executeScript'.
            connection_id: Connection ID for SSH credential lookup.
            params: Action parameters (command, script, timeout, etc.).

        Returns:
            Result dict with stdout/stderr output.
        """
        if action == "executeCommand":
            return await self._execute_command(connection_id, params)
        elif action == "executeScript":
            return await self._execute_script(connection_id, params)
        elif action == "testConnection":
            return await self._test_connection(connection_id, params)
        else:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown SSH action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

    def _get_credentials(self, connection_id: str | None) -> dict[str, Any] | None:
        """Retrieve SSH credentials from the vault.

        Args:
            connection_id: The connection ID to look up.

        Returns:
            Credential dict or None if not found.
        """
        if not connection_id:
            return None
        return self.vault.get_credential(connection_id)

    async def _connect(
        self, credentials: dict[str, Any], connect_timeout: int = DEFAULT_SSH_CONNECT_TIMEOUT
    ) -> asyncssh.SSHClientConnection:
        """Establish an SSH connection using vault credentials.

        Args:
            credentials: Dict with host, port, username, and password or privateKey.
            connect_timeout: Connection timeout in seconds.

        Returns:
            An active SSH client connection.
        """
        host = credentials["host"]
        port = credentials.get("port", DEFAULT_SSH_PORT)
        username = credentials.get("username", "root")

        # Determine known_hosts setting
        known_hosts: str | None = None
        if "knownHostsPath" in credentials:
            raw_path = credentials["knownHostsPath"]
            # Validate path: no traversal, must be absolute, no symlink tricks
            real_path = os.path.realpath(raw_path)
            if ".." in raw_path or not os.path.isabs(raw_path):
                raise ConnectionError(
                    f"Invalid knownHostsPath: must be an absolute path without '..' segments"
                )
            if real_path != raw_path and os.path.islink(raw_path):
                raise ConnectionError(
                    f"knownHostsPath '{raw_path}' is a symlink (resolves to '{real_path}'). "
                    "Symlinks are rejected to prevent symlink attacks. Use the resolved path directly."
                )
            # H7/A5: Re-verify at use time to prevent TOCTOU race between
            # the symlink check above and the actual file open by asyncssh.
            # On systems with O_NOFOLLOW, use it to reject symlinks at open time.
            # On systems without O_NOFOLLOW (e.g. Windows), fall back to a
            # realpath re-check which is the best available mitigation.
            o_nofollow = getattr(os, 'O_NOFOLLOW', 0)
            if o_nofollow:
                try:
                    fd = os.open(real_path, os.O_RDONLY | o_nofollow)
                    os.close(fd)
                except OSError as exc:
                    raise ConnectionError(
                        f"knownHostsPath '{raw_path}' changed or is a symlink at use time: {exc}"
                    )
            else:
                # O_NOFOLLOW unavailable — re-check realpath as best-effort TOCTOU mitigation
                recheck_real = os.path.realpath(raw_path)
                if recheck_real != real_path:
                    raise ConnectionError(
                        f"knownHostsPath '{raw_path}' resolved path changed between checks "
                        f"(was '{real_path}', now '{recheck_real}'). Possible symlink attack."
                    )
                logger.warning(
                    "O_NOFOLLOW not available on this platform; "
                    "known_hosts symlink protection is best-effort for path: %s",
                    raw_path,
                )
            known_hosts = real_path
        elif credentials.get("strictHostKeyChecking") is False:
            known_hosts = None  # Explicit opt-out
        else:
            # Default to system known_hosts if it exists
            system_known_hosts = Path.home() / ".ssh" / "known_hosts"
            if system_known_hosts.exists():
                known_hosts = str(system_known_hosts)
            else:
                # H2: Fail secure — reject connections when host key cannot be verified
                raise ConnectionError(
                    f"SSH known_hosts file not found at {system_known_hosts}. "
                    f"Cannot verify host key for {host}. Either create a known_hosts file, "
                    f"provide 'knownHostsPath' in credentials, or set 'strictHostKeyChecking: false' "
                    f"to explicitly opt out of host key verification."
                )

        connect_kwargs: dict[str, Any] = {
            "host": host,
            "port": port,
            "username": username,
            "known_hosts": known_hosts,
        }

        # Support both password and key-based auth
        if "privateKey" in credentials:
            private_key = credentials["privateKey"]
            passphrase = credentials.get("passphrase")
            connect_kwargs["client_keys"] = [
                asyncssh.import_private_key(private_key, passphrase)
            ]
        elif "password" in credentials:
            connect_kwargs["password"] = credentials["password"]

        return await asyncio.wait_for(
            asyncssh.connect(**connect_kwargs),
            timeout=connect_timeout,
        )

    async def _test_connection(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Test SSH connectivity by establishing a connection and running a simple command.

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

        timeout = params.get("timeout", DEFAULT_SSH_CONNECT_TIMEOUT)
        start = time.monotonic_ns()
        try:
            async with await self._connect(credentials, connect_timeout=timeout) as conn:
                result = await conn.run("echo ok", timeout=10)
                duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
                return {
                    "status": "success",
                    "output": {
                        "host": credentials.get("host"),
                        "port": credentials.get("port", DEFAULT_SSH_PORT),
                        "username": credentials.get("username"),
                        "message": "SSH connection successful",
                    },
                    "error": None,
                    "exitCode": result.exit_status or 0,
                    "durationMs": duration_ms,
                }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "status": "error",
                "output": None,
                "error": f"SSH connection test failed: {exc}",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _execute_command(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Run a command on the remote host.

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
        timeout = params.get("timeout", DEFAULT_SSH_TIMEOUT)

        start = time.monotonic_ns()
        try:
            async with await self._connect(credentials) as conn:
                result = await conn.run(command, timeout=timeout)

                duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
                exit_code = result.exit_status or 0

                raw_stdout = result.stdout or ""
                raw_stderr = result.stderr or ""
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
                "SSH command failed for connection %s: %s",
                connection_id, exc,
            )
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _execute_script(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Upload and execute a script on the remote host.

        Args:
            connection_id: Connection ID for credential lookup.
            params: Must contain 'script'. Optional: 'interpreter', 'timeout'.

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
        interpreter = params.get("interpreter", "/bin/bash")
        timeout = params.get("timeout", DEFAULT_SSH_TIMEOUT)

        # C3: Validate interpreter against whitelist to prevent command injection
        if interpreter not in ALLOWED_INTERPRETERS:
            return {
                "status": "error",
                "output": None,
                "error": (
                    f"Interpreter '{interpreter}' is not allowed. "
                    f"Allowed interpreters: {', '.join(sorted(ALLOWED_INTERPRETERS))}"
                ),
                "exitCode": -1,
                "durationMs": 0,
            }

        start = time.monotonic_ns()
        remote_path: str | None = None
        try:
            async with await self._connect(credentials) as conn:
                # Upload script to a temp path with unpredictable name
                remote_path = f"/tmp/easyalert_{secrets.token_hex(16)}.sh"

                async with conn.start_sftp_client() as sftp:
                    async with sftp.open(remote_path, "w") as f:
                        await f.write(script)

                # Make executable and run (shlex.quote to prevent shell interpolation)
                await conn.run(f"chmod +x {shlex.quote(remote_path)}", timeout=10)
                try:
                    result = await conn.run(
                        f"{shlex.quote(interpreter)} {shlex.quote(remote_path)}", timeout=timeout
                    )
                finally:
                    try:
                        await conn.run(f"rm -f {shlex.quote(remote_path)}", timeout=10)
                    except Exception:
                        logger.warning(
                            "Failed to clean up remote script %s on connection %s",
                            remote_path, connection_id,
                        )

                duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
                exit_code = result.exit_status or 0

                raw_stdout = result.stdout or ""
                raw_stderr = result.stderr or ""
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
                "SSH script execution failed for connection %s: %s",
                connection_id, exc,
            )
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }
