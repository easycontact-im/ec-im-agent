"""Base executor interface for all automation action executors."""

from abc import ABC, abstractmethod
from typing import Any

from vault import Vault


def truncate_output(stdout: str, stderr: str, max_size: int = 65536) -> dict[str, Any]:
    """Truncate command output to max_size bytes, preserving line boundaries.

    Returns a dict with stdout, stderr, truncated flag, and original sizes.
    """
    result: dict[str, Any] = {}
    truncated = False

    if len(stdout) > max_size:
        truncated = True
        cut = stdout[:max_size].rfind("\n")
        result["stdout"] = stdout[:cut + 1] if cut > 0 else stdout[:max_size]
    else:
        result["stdout"] = stdout

    if len(stderr) > max_size:
        truncated = True
        cut = stderr[:max_size].rfind("\n")
        result["stderr"] = stderr[:cut + 1] if cut > 0 else stderr[:max_size]
    else:
        result["stderr"] = stderr

    result["truncated"] = truncated
    if truncated:
        result["originalStdoutSize"] = len(stdout)
        result["originalStderrSize"] = len(stderr)

    return result


class BaseExecutor(ABC):
    """Abstract base class for job executors.

    All executors receive a Vault instance for credential access and must
    implement the execute() method.

    The execute() method must always return a result dict with:
    - status: "success" | "error"
    - output: Any (action-specific output data)
    - error: str | None (error message on failure, None on success)
    - exitCode: int (0 for success, non-zero for failure)
    - durationMs: int (execution time in milliseconds, set by worker)
    """

    def __init__(self, vault: Vault) -> None:
        """Initialize the executor with a vault reference.

        Args:
            vault: Vault instance for accessing connection credentials.
        """
        self.vault = vault

    @abstractmethod
    async def execute(
        self,
        action: str,
        connection_id: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute an action and return the result.

        Args:
            action: The specific action to perform (e.g., 'executeCommand').
            connection_id: Optional connection ID for credential lookup.
            params: Action parameters dict.

        Returns:
            Result dict with: status, output, error, exitCode, durationMs.
        """
        pass

    async def close(self) -> None:
        """Close any resources held by the executor. Override in subclasses."""
        pass
