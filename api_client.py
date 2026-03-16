import asyncio
import enum
import logging
import os
import platform
import random
import time
from importlib.metadata import version as pkg_version, PackageNotFoundError
from typing import Any

import psutil

import httpx

from config import settings
from result_queue import ResultQueue

try:
    AGENT_VERSION = pkg_version("ec-im-agent")
except PackageNotFoundError:
    AGENT_VERSION = "0.1.0"

logger = logging.getLogger("ec-im-agent.api_client")

REGISTER_ENDPOINT = "/api/v1/internal/agents/register"
JOBS_ENDPOINT = "/api/v1/internal/agents/jobs"
RESULTS_ENDPOINT = "/api/v1/internal/agents/results"
HEARTBEAT_ENDPOINT = "/api/v1/internal/agents/heartbeat"
CONNECTIONS_ENDPOINT = "/api/v1/internal/agents/connections"

REQUEST_TIMEOUT = 30.0
RETRY_ATTEMPTS = 3
RETRY_BACKOFF_BASE = 1.0

# Circuit breaker constants
CIRCUIT_FAILURE_THRESHOLD = 3  # Consecutive failures before opening
CIRCUIT_RESET_TIMEOUT_SECONDS = 30  # How long to wait before trying again


class CircuitState(enum.Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Simple circuit breaker for API calls.

    Three states:
    - CLOSED (normal): Requests pass through.
    - OPEN (tripped): Requests fail immediately, no API calls made.
    - HALF_OPEN (testing): One request passes to test recovery.
    """

    def __init__(
        self,
        failure_threshold: int = CIRCUIT_FAILURE_THRESHOLD,
        reset_timeout: int = CIRCUIT_RESET_TIMEOUT_SECONDS,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: float = 0
        self._lock = asyncio.Lock()
        # Add jitter: randomize the effective reset timeout by ±25%
        self._jittered_timeout = self._new_jittered_timeout()
        # Y2: Only allow one probe request in HALF_OPEN state
        self._half_open_probe_in_flight = False

    def _new_jittered_timeout(self) -> float:
        """Generate a new jittered timeout value (±25% of base reset_timeout)."""
        return self.reset_timeout + random.uniform(
            -self.reset_timeout * 0.25, self.reset_timeout * 0.25
        )

    async def can_execute(self) -> bool:
        """Check if a request can be made.

        In HALF_OPEN state, only one probe request is allowed through.
        All other requests are blocked until the probe succeeds or fails.
        This prevents thundering herd on a recovering API.
        """
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                if time.monotonic() - self.last_failure_time >= self._jittered_timeout:
                    self.state = CircuitState.HALF_OPEN
                    self._half_open_probe_in_flight = True
                    logger.info(
                        "Circuit breaker transitioning to HALF_OPEN after %ds cooldown.",
                        self.reset_timeout,
                    )
                    return True
                return False
            else:  # HALF_OPEN
                # Only allow one probe request through — block the rest
                if self._half_open_probe_in_flight:
                    return False
                self._half_open_probe_in_flight = True
                return True

    async def record_success(self) -> None:
        """Record a successful API call."""
        async with self._lock:
            if self.state != CircuitState.CLOSED:
                logger.info("Circuit breaker CLOSED — API recovered.")
            self.failure_count = 0
            self.state = CircuitState.CLOSED
            self._half_open_probe_in_flight = False

    async def record_failure(self) -> None:
        """Record a failed API call."""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.monotonic()
            if self.failure_count >= self.failure_threshold and self.state != CircuitState.OPEN:
                self.state = CircuitState.OPEN
                self._jittered_timeout = self._new_jittered_timeout()
                logger.warning(
                    "Circuit breaker OPENED after %d consecutive failures. "
                    "Will retry in %ds.",
                    self.failure_count,
                    int(self._jittered_timeout),
                )
            elif self.state == CircuitState.HALF_OPEN:
                # Test request failed — go back to OPEN
                self.state = CircuitState.OPEN
                self._half_open_probe_in_flight = False
                self._jittered_timeout = self._new_jittered_timeout()
                logger.warning(
                    "Circuit breaker test request failed, returning to OPEN state. "
                    "Will retry in %ds.",
                    int(self._jittered_timeout),
                )


class APIClient:
    """HTTPS client for communication with the central EasyAlert SaaS API.

    Uses a long-lived httpx.AsyncClient with connection pooling for efficient
    communication with the central API. Includes retry logic with exponential
    backoff for transient failures.
    """

    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.AGENT_API_URL,
            headers={
                "X-Agent-Api-Key": settings.AGENT_API_KEY,
                "Content-Type": "application/json",
            },
            timeout=REQUEST_TIMEOUT,
            limits=httpx.Limits(
                max_connections=10,
                max_keepalive_connections=5,
            ),
        )
        self._agent_id: str | None = None
        self._needs_reregister: bool = False
        self._jobs_executed: int = 0
        self._jobs_failed: int = 0
        self._start_time: float = time.monotonic()
        self._circuit_breaker = CircuitBreaker()
        # Persistent queue for results that cannot be submitted when the
        # circuit breaker is OPEN — lives beside the vault file.
        from pathlib import Path
        vault_dir = Path(settings.VAULT_PATH).expanduser().parent
        self._result_queue = ResultQueue(vault_dir / "result_queue.json")
        psutil.cpu_percent(interval=None)

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        await self._client.aclose()

    @property
    def agent_id(self) -> str | None:
        """Return the agent ID assigned during registration."""
        return self._agent_id

    @property
    def needs_reregister(self) -> bool:
        """True when the server returned 401/403, indicating the agent should re-register."""
        return self._needs_reregister

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        max_attempts: int | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """HTTP request with exponential backoff retry.

        Handles 429 (rate limited) responses by respecting the Retry-After
        header. Retries on transient network errors (connect, timeout, protocol).

        Args:
            method: HTTP method (GET, POST, PUT, DELETE).
            url: Request URL path.
            max_attempts: Override default retry count.
            **kwargs: Passed through to httpx.AsyncClient.request.

        Returns:
            The successful httpx.Response.

        Raises:
            The last exception if all retry attempts fail.
        """
        attempts = max_attempts or RETRY_ATTEMPTS
        last_exc: BaseException | None = None

        for attempt in range(attempts):
            try:
                response = await self._client.request(method, url, **kwargs)

                if response.status_code == 429:
                    try:
                        retry_after = float(
                            response.headers.get("Retry-After", 2**attempt)
                        )
                    except (ValueError, TypeError):
                        retry_after = float(2**attempt)
                    logger.warning(
                        "Rate limited on %s %s, retrying in %.1fs",
                        method, url, retry_after,
                    )
                    last_exc = httpx.HTTPStatusError(
                        f"Rate limited (429) on {method} {url}",
                        request=response.request,
                        response=response,
                    )
                    await asyncio.sleep(retry_after)
                    continue

                # Retry on 5xx server errors
                if response.status_code >= 500 and attempt < attempts - 1:
                    backoff = RETRY_BACKOFF_BASE * (2**attempt)
                    logger.warning(
                        "Server error %d on %s %s (attempt %d/%d). Retrying in %.1fs",
                        response.status_code, method, url,
                        attempt + 1, attempts, backoff,
                    )
                    await asyncio.sleep(backoff)
                    continue

                # F3: Re-register if server returns 401/403 (key rotated or agent removed)
                if response.status_code in (401, 403) and self._agent_id:
                    logger.warning(
                        "Received %d on %s %s — flagging for re-registration",
                        response.status_code, method, url,
                    )
                    self._needs_reregister = True
                    raise httpx.HTTPStatusError(
                        f"Auth error {response.status_code}",
                        request=response.request,
                        response=response,
                    )

                response.raise_for_status()
                return response

            except (
                httpx.ConnectError,
                httpx.TimeoutException,
                httpx.RemoteProtocolError,
            ) as exc:
                last_exc = exc
                if attempt < attempts - 1:
                    backoff = RETRY_BACKOFF_BASE * (2**attempt)
                    logger.warning(
                        "API request %s %s failed (attempt %d/%d): %s. Retrying in %.1fs",
                        method, url, attempt + 1, attempts, exc, backoff,
                    )
                    await asyncio.sleep(backoff)

        if last_exc is None:
            last_exc = Exception(f"API request failed after {attempts} retries")
        raise last_exc

    async def register(self) -> str:
        """Register this agent with the central API and return the assigned agent ID.

        Uses 5 retry attempts (higher than default) since registration is
        critical for agent startup.

        Returns:
            The assigned agent ID string.

        Raises:
            Exception: If registration fails after all retry attempts.
        """
        payload = {
            "hostname": platform.node(),
            "agentName": settings.AGENT_NAME,
            "version": AGENT_VERSION,
            "platform": platform.system(),
            "architecture": platform.machine(),
        }

        logger.info("Registering agent '%s'", settings.AGENT_NAME)

        try:
            response = await self._request_with_retry(
                "POST", REGISTER_ENDPOINT, max_attempts=5, json=payload
            )
            body = response.json()
            self._agent_id = body["data"]["agentId"]
            self._needs_reregister = False
            logger.info("Registered with agent ID: %s", self._agent_id)
            return self._agent_id
        except Exception as exc:
            logger.critical("Registration failed after all retry attempts: %s", exc)
            raise

    @staticmethod
    def _collect_system_metrics() -> dict[str, Any]:
        """Collect system resource metrics for heartbeat reporting.

        Each metric category is collected independently so that a failure
        in one (e.g. disk on a restricted container) does not prevent the
        others from being reported.

        Returns:
            Dict with available CPU, memory, and disk usage metrics.
        """
        metrics: dict[str, Any] = {}
        try:
            metrics["cpuPercent"] = psutil.cpu_percent(interval=None)
        except Exception:
            pass
        try:
            mem = psutil.virtual_memory()
            metrics["memoryPercent"] = mem.percent
            metrics["memoryUsedMb"] = round(mem.used / 1_048_576)
            metrics["memoryTotalMb"] = round(mem.total / 1_048_576)
        except Exception:
            pass
        try:
            disk = psutil.disk_usage(os.path.abspath(os.sep))
            metrics["diskPercent"] = disk.percent
            metrics["diskUsedGb"] = round(disk.used / 1_073_741_824, 1)
            metrics["diskTotalGb"] = round(disk.total / 1_073_741_824, 1)
        except Exception:
            pass
        try:
            metrics["loadAverage"] = list(os.getloadavg()) if hasattr(os, "getloadavg") else None
        except Exception:
            pass
        return metrics

    async def get_jobs(self) -> list[dict[str, Any]]:
        """Poll the central API for pending workflow jobs.

        Uses the circuit breaker to skip requests when the API is unreachable.

        Returns:
            List of job dicts. Empty list if no jobs are pending or circuit is open.
        """
        if not await self._circuit_breaker.can_execute():
            logger.debug("Circuit breaker OPEN — skipping job poll")
            return []

        try:
            params = {"agentId": self._agent_id}
            response = await self._request_with_retry(
                "GET", JOBS_ENDPOINT, params=params
            )

            data = response.json()
            jobs = data.get("data", [])
            if jobs:
                logger.info("Received %d job(s)", len(jobs))
            else:
                logger.debug("No pending jobs")
            await self._circuit_breaker.record_success()
            return jobs
        except Exception as exc:
            await self._circuit_breaker.record_failure()
            raise exc

    async def submit_results(self, results: list[dict[str, Any]]) -> None:
        """Submit job execution results back to the central API.

        Uses the circuit breaker to skip requests when the API is unreachable.
        On final failure, logs the lost job IDs for diagnostics.

        Args:
            results: List of job result dicts.
        """
        if not results:
            logger.warning("No results to submit")
            return

        if not await self._circuit_breaker.can_execute():
            queued = self._result_queue.enqueue(results)
            queued_ids = [r.get("jobId", "?") for r in results[:queued]]
            logger.warning(
                "Circuit breaker OPEN — queued %d/%d result(s) to disk (jobIds: %s)",
                queued, len(results), queued_ids,
            )
            return

        logger.info("Submitting %d result(s)", len(results))
        try:
            response = await self._request_with_retry(
                "POST", RESULTS_ENDPOINT, json=results
            )
            response.raise_for_status()
            self._jobs_executed += len(results)
            logger.debug("Results submitted successfully")
            await self._circuit_breaker.record_success()
            # Piggyback: flush any queued results while the circuit is healthy
            if self._result_queue.size > 0:
                await self.retry_queued_results()
        except Exception as exc:
            await self._circuit_breaker.record_failure()
            self._jobs_failed += len(results)
            queued = self._result_queue.enqueue(results)
            queued_ids = [r.get("jobId", "?") for r in results[:queued]]
            logger.error(
                "Failed to submit %d result(s), queued %d to disk (jobIds: %s): %s",
                len(results), queued, queued_ids, exc,
            )

    async def retry_queued_results(self) -> None:
        """Drain the persistent result queue and re-submit to the API.

        Only attempts submission when the circuit breaker allows requests.
        On failure the results are re-enqueued so nothing is lost.
        """
        if self._result_queue.size == 0:
            return

        if not await self._circuit_breaker.can_execute():
            logger.debug(
                "Circuit breaker OPEN — skipping queued result retry (%d pending)",
                self._result_queue.size,
            )
            return

        results = self._result_queue.drain()
        if not results:
            return

        logger.info("Retrying %d queued result(s)", len(results))
        try:
            response = await self._request_with_retry(
                "POST", RESULTS_ENDPOINT, json=results,
            )
            response.raise_for_status()
            self._jobs_executed += len(results)
            await self._circuit_breaker.record_success()
            logger.info("Successfully submitted %d queued result(s)", len(results))
        except Exception as exc:
            await self._circuit_breaker.record_failure()
            # Re-enqueue so results are not lost
            requeued = self._result_queue.enqueue(results)
            logger.error(
                "Failed to submit queued results, re-enqueued %d/%d: %s",
                requeued, len(results), exc,
            )

    async def heartbeat(
        self, status: str = "online", data: dict[str, Any] | None = None
    ) -> None:
        """Send a heartbeat signal to the central API with system metrics.

        Uses the circuit breaker to skip requests when the API is unreachable.

        Args:
            status: Agent status string (e.g. 'online', 'offline').
            data: Optional additional heartbeat data to include.
        """
        if not await self._circuit_breaker.can_execute():
            logger.debug("Circuit breaker OPEN — skipping heartbeat")
            return

        # F5: Include system metrics in heartbeat
        system_metrics = self._collect_system_metrics()

        payload = {
            "agentId": self._agent_id,
            "status": status,
            "jobsExecuted": self._jobs_executed,
            "jobsFailed": self._jobs_failed,
            "uptimeSeconds": int(time.monotonic() - self._start_time),
            "version": AGENT_VERSION,
            "systemMetrics": system_metrics,
        }
        if data:
            payload.update(data)

        logger.debug("Sending heartbeat")
        try:
            response = await self._request_with_retry(
                "POST", HEARTBEAT_ENDPOINT, json=payload
            )
            response.raise_for_status()
            await self._circuit_breaker.record_success()
            logger.debug("Heartbeat acknowledged")
        except Exception as exc:
            await self._circuit_breaker.record_failure()
            raise exc

    async def get_connections(self) -> list[dict[str, Any]]:
        """Fetch connection definitions from the central API.

        Uses the circuit breaker to skip requests when the API is unreachable.

        Returns:
            List of connection configuration dicts. Empty list if circuit is open.
        """
        if not await self._circuit_breaker.can_execute():
            logger.debug("Circuit breaker OPEN — skipping connection fetch")
            return []

        try:
            params = {"agentId": self._agent_id}
            response = await self._request_with_retry(
                "GET", CONNECTIONS_ENDPOINT, params=params
            )

            data = response.json()
            connections = data.get("data", [])
            logger.debug("Fetched %d connection(s)", len(connections))
            await self._circuit_breaker.record_success()
            return connections
        except Exception as exc:
            await self._circuit_breaker.record_failure()
            raise exc
