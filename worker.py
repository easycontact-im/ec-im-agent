import asyncio
import json as _json
import logging
import sys
import time
from typing import Any

from executors import EXECUTOR_REGISTRY
from vault import Vault

logger = logging.getLogger("ec-im-agent.worker")

MIN_JOB_TIMEOUT = 5
MAX_JOB_TIMEOUT = 3600  # 1 hour
MAX_PARAMS_SIZE_BYTES = 10_485_760  # 10MB max params size
MAX_SEMAPHORE_WAIT = 60  # seconds — max time to wait for an execution slot
JOB_DEDUP_TTL_SECONDS = MAX_JOB_TIMEOUT + 60  # Job timeout + 1min buffer — must exceed max job duration
JOB_DEDUP_MAX_SIZE = 10_000  # Maximum entries in the dedup set


class Worker:
    """Job executor with semaphore-based concurrency control.

    Dispatches jobs to the appropriate executor based on actionType,
    enforcing a maximum concurrency limit via asyncio.Semaphore.
    """

    def __init__(self, vault: Vault, max_concurrent_jobs: int) -> None:
        """Initialize the worker.

        Args:
            vault: Vault instance for credential access.
            max_concurrent_jobs: Maximum number of jobs that can run concurrently.
        """
        self._vault = vault
        self._semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self._executor_cache: dict[str, Any] = {}
        self._executor_cache_lock = asyncio.Lock()
        # Job deduplication: {jobId: monotonic_timestamp}
        self._processed_jobs: dict[str, float] = {}
        self._dedup_lock = asyncio.Lock()

    async def close(self) -> None:
        """Close all cached executors that have a close method."""
        for key, executor in self._executor_cache.items():
            if hasattr(executor, "close"):
                try:
                    await executor.close()
                except Exception as exc:
                    logger.warning("Failed to close executor %s: %s", key, exc)
        self._executor_cache.clear()

    async def _get_executor(self, executor_key: str) -> Any:
        """Get or create an executor instance by key.

        Uses an asyncio.Lock to prevent concurrent creation of the same executor.

        Args:
            executor_key: The executor category (e.g., 'ssh', 'http', 'kubernetes').

        Returns:
            An executor instance, or None if the key is unknown.
        """
        async with self._executor_cache_lock:
            if executor_key in self._executor_cache:
                return self._executor_cache[executor_key]

            executor_cls = EXECUTOR_REGISTRY.get(executor_key)
            if executor_cls is None:
                return None

            executor = executor_cls(self._vault)
            self._executor_cache[executor_key] = executor
            return executor

    def _resolve_executor_key(self, action_type: str) -> str:
        """Extract the executor category from an actionType string.

        ActionType format: '<category>.<action>' (e.g., 'ssh.executeCommand').

        Args:
            action_type: The full action type string.

        Returns:
            The executor category key.
        """
        return action_type.split(".")[0] if "." in action_type else action_type

    def _resolve_action(self, action_type: str) -> str:
        """Extract the action name from an actionType string.

        Args:
            action_type: The full action type string (e.g., 'ssh.executeCommand').

        Returns:
            The action name (e.g., 'executeCommand').
        """
        parts = action_type.split(".", 1)
        return parts[1] if len(parts) > 1 else action_type

    async def _is_duplicate_job(self, job_id: str) -> bool:
        """Check if a job has already been processed recently.

        Also evicts expired entries when the set exceeds MAX size.

        Args:
            job_id: The job identifier to check.

        Returns:
            True if the job was already processed (duplicate), False otherwise.
        """
        now = time.monotonic()
        async with self._dedup_lock:
            if job_id in self._processed_jobs:
                logger.warning("Duplicate job detected, skipping: jobId=%s", job_id)
                return True
            # Register this job as in-progress
            self._processed_jobs[job_id] = now
            # Evict expired entries if set is too large
            if len(self._processed_jobs) > JOB_DEDUP_MAX_SIZE:
                cutoff = now - JOB_DEDUP_TTL_SECONDS
                expired = [k for k, t in self._processed_jobs.items() if t < cutoff]
                for k in expired:
                    del self._processed_jobs[k]
        return False

    async def execute_job(self, job: dict[str, Any]) -> dict[str, Any]:
        """Execute a single workflow job with semaphore throttling.

        Args:
            job: Job dict containing at minimum: jobId, actionType, params.
                 May also contain: connectionId, timeout.

        Returns:
            Result dict with: jobId, status, output, error, exitCode, durationMs.
        """
        job_id = job.get("jobId", "unknown")
        action_type = job.get("actionType", "")

        # Deduplication check — skip jobs we've already processed
        if await self._is_duplicate_job(job_id):
            return {
                "jobId": job_id,
                "status": "error",
                "output": None,
                "error": "Duplicate job — already processed",
                "exitCode": -1,
                "durationMs": 0,
            }
        connection_id = job.get("connectionId")
        params = job.get("params", {})

        # P0-C7: Validate params size to prevent memory exhaustion
        try:
            params_size = len(_json.dumps(params, default=str))
            if params_size > MAX_PARAMS_SIZE_BYTES:
                logger.error(
                    "Job params too large: jobId=%s size=%d max=%d",
                    job_id, params_size, MAX_PARAMS_SIZE_BYTES,
                )
                return {
                    "jobId": job_id,
                    "status": "error",
                    "output": None,
                    "error": f"Job params size ({params_size} bytes) exceeds maximum ({MAX_PARAMS_SIZE_BYTES} bytes)",
                    "exitCode": -1,
                    "durationMs": 0,
                }
        except (TypeError, ValueError):
            pass  # If serialization fails, let the executor handle it

        logger.info(
            "Executing job: jobId=%s actionType=%s connectionId=%s",
            job_id, action_type, connection_id,
        )

        # M5: Timeout if semaphore cannot be acquired within MAX_SEMAPHORE_WAIT
        wait_start = time.monotonic_ns()
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=MAX_SEMAPHORE_WAIT)
        except asyncio.TimeoutError:
            logger.warning(
                "Job %s timed out waiting for execution slot after %ds",
                job_id, MAX_SEMAPHORE_WAIT,
            )
            return {
                "jobId": job_id,
                "status": "error",
                "output": None,
                "error": f"Timed out waiting for execution slot after {MAX_SEMAPHORE_WAIT}s",
                "exitCode": -1,
                "durationMs": 0,
            }

        try:
            wait_ms = int((time.monotonic_ns() - wait_start) / 1_000_000)
            if wait_ms > 100:
                logger.info(
                    "Semaphore wait: %dms (jobId=%s)", wait_ms, job_id,
                )

            start = time.monotonic_ns()

            # Handle connection test jobs specially
            if action_type == "connection.test":
                return await self._test_connection(job_id, connection_id, start)

            executor_key = self._resolve_executor_key(action_type)
            action = self._resolve_action(action_type)

            executor = await self._get_executor(executor_key)
            if executor is None:
                duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
                logger.warning("Unknown executor for actionType: %s", action_type)
                return {
                    "jobId": job_id,
                    "status": "error",
                    "output": None,
                    "error": f"Unknown action type: {action_type}",
                    "exitCode": -1,
                    "durationMs": duration_ms,
                }

            try:
                raw_timeout = params.get("timeout", 300)
                try:
                    timeout = max(MIN_JOB_TIMEOUT, min(int(raw_timeout), MAX_JOB_TIMEOUT))
                except (TypeError, ValueError):
                    timeout = 300
                result = await asyncio.wait_for(
                    executor.execute(action, connection_id, params),
                    timeout=timeout,
                )
                if result is None:
                    result = {
                        "status": "error",
                        "output": None,
                        "error": "Executor returned None (implementation error)",
                        "exitCode": -1,
                        "durationMs": 0,
                    }
                result["jobId"] = job_id
                duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
                result["durationMs"] = duration_ms

                logger.info(
                    "Job completed: jobId=%s status=%s durationMs=%d",
                    job_id, result.get("status", "unknown"), duration_ms,
                )
                return result

            except asyncio.TimeoutError:
                duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
                logger.error(
                    "Job timed out: jobId=%s timeout=%ds durationMs=%d",
                    job_id, timeout, duration_ms,
                )
                return {
                    "jobId": job_id,
                    "status": "error",
                    "output": None,
                    "error": f"Job timed out after {timeout}s",
                    "exitCode": -1,
                    "durationMs": duration_ms,
                }

            except Exception as exc:
                duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
                logger.error(
                    "Job failed: jobId=%s error=%s durationMs=%d",
                    job_id, exc, duration_ms,
                )
                return {
                    "jobId": job_id,
                    "status": "error",
                    "output": None,
                    "error": str(exc),
                    "exitCode": -1,
                    "durationMs": duration_ms,
                }
        finally:
            self._semaphore.release()

    async def _test_connection(
        self, job_id: str, connection_id: str | None, start: int
    ) -> dict[str, Any]:
        """Test a connection by verifying vault credentials exist.

        Args:
            job_id: The job identifier.
            connection_id: The connection to test.
            start: Monotonic start time in nanoseconds.

        Returns:
            Result dict indicating success or failure.
        """
        duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

        if not connection_id:
            return {
                "jobId": job_id,
                "status": "error",
                "output": None,
                "error": "No connectionId provided for connection test",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

        credential = self._vault.get_credential(connection_id)
        if credential is None:
            return {
                "jobId": job_id,
                "status": "error",
                "output": {"message": "Credential not found in vault"},
                "error": f"No credential found for connection: {connection_id}",
                "exitCode": 1,
                "durationMs": duration_ms,
            }

        return {
            "jobId": job_id,
            "status": "success",
            "output": {"message": "Connection credential found in vault"},
            "error": None,
            "exitCode": 0,
            "durationMs": duration_ms,
        }

    async def run_jobs(self, jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Execute multiple jobs concurrently and return all results.

        Uses asyncio.gather with return_exceptions=True so a single job
        crash does not lose the entire batch.

        Args:
            jobs: List of job dicts to execute.

        Returns:
            List of result dicts, one per input job.
        """
        if not jobs:
            return []

        logger.info("Executing %d job(s) in parallel", len(jobs))
        tasks = [self.execute_job(job) for job in jobs]
        raw_results = await asyncio.gather(*tasks, return_exceptions=True)

        results: list[dict[str, Any]] = []
        for i, raw in enumerate(raw_results):
            if isinstance(raw, BaseException):
                job_id = jobs[i].get("jobId", "unknown")
                logger.error(
                    "Job %s crashed: %s", job_id, raw, exc_info=True,
                )
                results.append({
                    "jobId": job_id,
                    "status": "error",
                    "output": None,
                    "error": f"Executor crashed: {raw}",
                    "exitCode": -1,
                    "durationMs": 0,
                })
            else:
                results.append(raw)

        return results
