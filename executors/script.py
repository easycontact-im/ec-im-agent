"""Script executor for running local bash, PowerShell, and Python scripts."""

import asyncio
import logging
import os
import platform
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from executors.base import BaseExecutor, truncate_output

logger = logging.getLogger("ec-im-agent.executors.script")

DEFAULT_SCRIPT_TIMEOUT = 300  # 5 minutes
MAX_OUTPUT_SIZE = 1_048_576  # 1 MB
MAX_SCRIPT_CONTENT_BYTES = 1 * 1024 * 1024  # 1 MB

# M5: Use a dedicated temp directory under the agent's data directory
# instead of the system /tmp to reduce risk of temp file attacks.
SCRIPT_TEMP_DIR = Path.home() / ".easyalert" / "tmp"

# Environment variable names that must never be overridden by user-supplied values.
# All entries MUST be uppercase — the filtering logic normalises user-supplied
# keys to uppercase before comparing (case-insensitive blocking).
BLOCKED_ENV_VARS = frozenset({
    "PATH", "LD_PRELOAD", "LD_LIBRARY_PATH", "DYLD_LIBRARY_PATH",
    "PYTHONPATH", "HOME", "USER", "SHELL",
})


class ScriptExecutor(BaseExecutor):
    """Execute scripts locally on the agent host.

    Supported actions:
    - bash: Run a bash script.
    - powershell: Run a PowerShell script.
    - python: Run a Python script.
    """

    async def execute(
        self,
        action: str,
        connection_id: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Dispatch to the appropriate script runner.

        Args:
            action: 'bash', 'powershell', or 'python'.
            connection_id: Unused for local script execution.
            params: Must contain 'script'. Optional: 'timeout', 'env'.

        Returns:
            Result dict with stdout/stderr output.
        """
        if action == "bash":
            return await self._run_script(params, interpreter="bash", extension=".sh")
        elif action == "powershell":
            return await self._run_script(
                params,
                interpreter=self._resolve_powershell(),
                extension=".ps1",
            )
        elif action == "python":
            return await self._run_script(
                params, interpreter="python3", extension=".py"
            )
        else:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown script action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

    def _resolve_powershell(self) -> str:
        """Resolve the PowerShell executable path.

        Returns:
            'pwsh' on Linux/macOS (PowerShell Core), 'powershell' on Windows.
        """
        if platform.system() == "Windows":
            return "powershell"
        return "pwsh"

    async def _run_script(
        self,
        params: dict[str, Any],
        interpreter: str,
        extension: str,
    ) -> dict[str, Any]:
        """Write script to temp file, execute, and capture output.

        Args:
            params: Must contain 'script'. Optional: 'timeout', 'env', 'args'.
            interpreter: Path or name of the script interpreter.
            extension: File extension for the temp script file.

        Returns:
            Result dict with stdout/stderr output.
        """
        script = params.get("script", "")
        if not script or not script.strip():
            return {
                "status": "error",
                "output": None,
                "error": "Script content is empty",
                "exitCode": -1,
                "durationMs": 0,
            }

        # Validate script content size to prevent memory exhaustion
        if isinstance(script, str):
            content_bytes = len(script.encode("utf-8"))
            if content_bytes > MAX_SCRIPT_CONTENT_BYTES:
                return {
                    "status": "error",
                    "output": None,
                    "error": f"Script content exceeds maximum size ({content_bytes} bytes > {MAX_SCRIPT_CONTENT_BYTES} bytes)",
                    "exitCode": 1,
                    "durationMs": 0,
                }

        timeout = params.get("timeout", DEFAULT_SCRIPT_TIMEOUT)
        extra_env = params.get("env", {})
        script_args = params.get("args", [])

        start = time.monotonic_ns()

        # Ensure the dedicated temp directory exists before writing
        SCRIPT_TEMP_DIR.mkdir(parents=True, exist_ok=True)

        # Write script to a temporary file in the agent's dedicated temp directory
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=extension,
                delete=False,
                prefix="easyalert_",
                dir=str(SCRIPT_TEMP_DIR),
            ) as f:
                f.write(script)
                script_path = f.name
            # M5: Restrict file permissions on non-Windows platforms
            if sys.platform != "win32":
                os.chmod(script_path, 0o700)
        except OSError as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "status": "error",
                "output": None,
                "error": f"Failed to write script to temp file: {exc}",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

        try:
            # Build environment
            env = os.environ.copy()
            if extra_env:
                # C4: Case-insensitive filtering of dangerous env vars.
                # Normalise user-supplied keys to uppercase before checking
                # against BLOCKED_ENV_VARS. This prevents bypass via mixed-case
                # variants (e.g. "Path", "ld_preload") which on Linux would
                # create separate env vars that some tools still respect.
                safe_env: dict[str, str] = {}
                blocked_found: list[str] = []
                for key, value in extra_env.items():
                    if key.upper() in BLOCKED_ENV_VARS:
                        blocked_found.append(key)
                        continue
                    safe_env[key] = value
                if blocked_found:
                    logger.warning(
                        "Blocked dangerous environment variables: %s",
                        ", ".join(blocked_found),
                    )
                env.update(safe_env)

            # Build command: interpreter + script path + args
            # Only allow scalar types as args; skip dicts/lists to avoid injection
            safe_args = []
            for a in script_args:
                if isinstance(a, (str, int, float, bool)):
                    safe_args.append(str(a))
                else:
                    logger.warning("Skipping non-scalar script arg of type %s", type(a).__name__)
            cmd = [interpreter, script_path] + safe_args

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
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
                    "error": f"Script timed out after {timeout}s",
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
                "error": f"Interpreter not found: {interpreter}",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Script execution failed: %s", exc)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

        finally:
            # Clean up temp file
            try:
                os.unlink(script_path)
            except OSError as cleanup_exc:
                logger.debug("Failed to clean up temp file %s: %s", script_path, cleanup_exc)
