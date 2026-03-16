import asyncio
import json as _json
import logging
import os
import sys
import time
from pathlib import Path
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
DEDUP_SAVE_INTERVAL_SECONDS = 60  # How often to persist dedup state to disk
_DEDUP_FILE_PATH = Path(os.path.expanduser("~")) / ".easyalert" / "processed_jobs.json"


MAX_ERROR_MESSAGE_LENGTH = 500


class Worker:
    """Job executor with semaphore-based concurrency control.

    Dispatches jobs to the appropriate executor based on actionType,
    enforcing a maximum concurrency limit via asyncio.Semaphore.
    """

    def __init__(self, vault: Vault, max_concurrent_jobs: int, *, is_tls: bool = True) -> None:
        """Initialize the worker.

        Args:
            vault: Vault instance for credential access.
            max_concurrent_jobs: Maximum number of jobs that can run concurrently.
            is_tls: Whether the SaaS API connection uses TLS (HTTPS).
        """
        self._vault = vault
        self._semaphore = asyncio.Semaphore(max_concurrent_jobs)
        self._is_tls = is_tls
        self._executor_cache: dict[str, Any] = {}
        self._executor_cache_lock = asyncio.Lock()
        # Job deduplication: {jobId: monotonic_timestamp}
        self._processed_jobs: dict[str, float] = {}
        self._dedup_lock = asyncio.Lock()
        self._last_dedup_save: float = time.monotonic()
        self._load_dedup_state()

    async def close(self) -> None:
        """Close all cached executors that have a close method."""
        # Persist dedup state before shutting down
        self._save_dedup_state()
        for key, executor in self._executor_cache.items():
            if hasattr(executor, "close"):
                try:
                    await executor.close()
                except Exception as exc:
                    logger.warning("Failed to close executor %s: %s", key, exc)
        self._executor_cache.clear()

    def _load_dedup_state(self) -> None:
        """Load previously processed jobs from disk for restart resilience.

        Reads the dedup file and restores entries that are still within the
        TTL window. Uses wall-clock time (stored as epoch seconds) since
        monotonic timestamps do not survive restarts.
        """
        if not _DEDUP_FILE_PATH.exists():
            return
        try:
            with open(_DEDUP_FILE_PATH, "r") as f:
                data = _json.load(f)
            if not isinstance(data, dict):
                logger.warning("Invalid dedup file format, ignoring")
                return
            now_epoch = time.time()
            now_mono = time.monotonic()
            loaded = 0
            for job_id, epoch_ts in data.items():
                if not isinstance(epoch_ts, (int, float)):
                    continue
                age_seconds = now_epoch - epoch_ts
                if age_seconds < JOB_DEDUP_TTL_SECONDS:
                    # Map the wall-clock age back to a monotonic timestamp
                    self._processed_jobs[job_id] = now_mono - age_seconds
                    loaded += 1
            if loaded:
                logger.info(
                    "Loaded %d processed job(s) from dedup state file", loaded,
                )
        except (_json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load dedup state from %s: %s", _DEDUP_FILE_PATH, exc)

    def _save_dedup_state(self) -> None:
        """Persist processed jobs to disk for restart resilience.

        Converts monotonic timestamps to wall-clock epoch seconds for
        portability across restarts. Only saves entries within the TTL window.
        """
        now_mono = time.monotonic()
        now_epoch = time.time()
        cutoff = now_mono - JOB_DEDUP_TTL_SECONDS

        # Build dict with epoch timestamps, trimming expired entries
        state: dict[str, float] = {}
        for job_id, mono_ts in self._processed_jobs.items():
            if mono_ts >= cutoff:
                # Convert monotonic to epoch: epoch = now_epoch - (now_mono - mono_ts)
                state[job_id] = now_epoch - (now_mono - mono_ts)

        try:
            _DEDUP_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
            temp_path = _DEDUP_FILE_PATH.with_suffix(".tmp")
            with open(temp_path, "w") as f:
                _json.dump(state, f)
            os.replace(temp_path, _DEDUP_FILE_PATH)
            logger.debug("Saved dedup state: %d job(s)", len(state))
        except OSError as exc:
            logger.warning("Failed to save dedup state to %s: %s", _DEDUP_FILE_PATH, exc)
        self._last_dedup_save = now_mono

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

        O1: Only checks the dedup set — does NOT register the job here.
        Registration happens after execution via _mark_job_completed().
        This prevents crash/shutdown from permanently marking unfinished
        jobs as done in the persisted dedup state.

        Args:
            job_id: The job identifier to check.

        Returns:
            True if the job was already processed (duplicate), False otherwise.
        """
        async with self._dedup_lock:
            if job_id in self._processed_jobs:
                logger.warning("Duplicate job detected, skipping: jobId=%s", job_id)
                return True
        return False

    async def _mark_job_completed(self, job_id: str) -> None:
        """Register a job as completed in the dedup set.

        Called after execution finishes (success or failure) so that
        incomplete jobs are not permanently marked as processed.
        """
        now = time.monotonic()
        async with self._dedup_lock:
            self._processed_jobs[job_id] = now
            # Evict expired entries; if still over max, evict oldest
            if len(self._processed_jobs) > JOB_DEDUP_MAX_SIZE:
                cutoff = now - JOB_DEDUP_TTL_SECONDS
                expired = [k for k, t in self._processed_jobs.items() if t < cutoff]
                for k in expired:
                    del self._processed_jobs[k]
                # D2: If still over max after TTL eviction, remove oldest entries
                if len(self._processed_jobs) > JOB_DEDUP_MAX_SIZE:
                    sorted_entries = sorted(self._processed_jobs.items(), key=lambda x: x[1])
                    to_remove = len(self._processed_jobs) - JOB_DEDUP_MAX_SIZE + 100
                    for k, _ in sorted_entries[:to_remove]:
                        del self._processed_jobs[k]

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

            # Handle credential storage jobs
            if action_type == "system.storeCredential":
                return await self._store_credential(job_id, params, start)

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
                error_msg = str(exc)[:MAX_ERROR_MESSAGE_LENGTH]
                return {
                    "jobId": job_id,
                    "status": "error",
                    "output": None,
                    "error": error_msg,
                    "exitCode": -1,
                    "durationMs": duration_ms,
                }
        finally:
            self._semaphore.release()
            # O1: Mark job as completed AFTER execution finishes, not before.
            # This ensures crash/shutdown won't leave unfinished jobs in the
            # persisted dedup set, which would block them for the TTL period.
            await self._mark_job_completed(job_id)

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

    async def _store_credential(
        self, job_id: str, params: dict[str, Any], start: int
    ) -> dict[str, Any]:
        """Store credentials in the encrypted vault.

        Called when the SaaS API proxies a credential delivery request.

        Args:
            job_id: The job identifier.
            params: Job params containing connectionId and credentials.
            start: Monotonic start time in nanoseconds.

        Returns:
            Result dict indicating success or failure.
        """
        # H8: Block credential storage over non-TLS connections
        if not self._is_tls:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "jobId": job_id,
                "status": "error",
                "output": None,
                "error": "Credential storage requires HTTPS connection to SaaS API",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

        connection_id = params.get("connectionId")
        credentials = params.get("credentials")

        if not connection_id:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "jobId": job_id,
                "status": "error",
                "output": None,
                "error": "No connectionId provided for credential storage",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

        if not credentials or not isinstance(credentials, dict):
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "jobId": job_id,
                "status": "error",
                "output": None,
                "error": "No credentials provided or invalid format",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

        try:
            self._vault.store_credential(connection_id, credentials)
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.info(
                "Credentials stored for connection %s (jobId=%s)",
                connection_id, job_id,
            )
            return {
                "jobId": job_id,
                "status": "success",
                "output": {"message": "Credentials stored in vault"},
                "error": None,
                "exitCode": 0,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error(
                "Failed to store credentials for connection %s: %s",
                connection_id, exc,
            )
            return {
                "jobId": job_id,
                "status": "error",
                "output": None,
                "error": "Failed to store credentials in vault",
                "exitCode": 1,
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

        # Periodically persist dedup state to disk
        if time.monotonic() - self._last_dedup_save >= DEDUP_SAVE_INTERVAL_SECONDS:
            self._save_dedup_state()

        return results
