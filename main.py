"""EasyAlert Automation Agent - Entry Point.

Lightweight agent that runs on customer infrastructure, polls the central
SaaS API for workflow jobs, executes them locally, and reports results.

Three concurrent loops:
1. Poll loop: Fetch and execute workflow jobs.
2. Heartbeat loop: Send periodic health signals.
3. Admin server: Local HTTP API for credential management.
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path

from admin_server import AdminServer
from api_client import APIClient
from config import settings, setup_logging
from vault import Vault
from worker import Worker

logger = logging.getLogger("ec-im-agent")

HEARTBEAT_FILE = Path.home() / ".easyalert" / ".agent_heartbeat"
SHUTDOWN_GRACE_PERIOD = 30.0
MAX_REGISTRATION_RETRIES = 5
REGISTRATION_RETRY_BASE_DELAY = 5  # seconds


async def heartbeat_loop(
    client: APIClient,
    shutdown_event: asyncio.Event,
) -> None:
    """Periodically send heartbeat signals to the central API.

    Args:
        client: API client for SaaS communication.
        shutdown_event: Event that signals shutdown.
    """
    while not shutdown_event.is_set():
        try:
            await client.heartbeat()
            # Touch heartbeat file for Docker healthcheck
            try:
                HEARTBEAT_FILE.touch()
            except OSError:
                pass
        except Exception as exc:
            logger.warning("Heartbeat failed: %s", exc)

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=settings.HEARTBEAT_INTERVAL,
            )
        except asyncio.TimeoutError:
            pass


async def poll_loop(
    client: APIClient,
    worker: Worker,
    shutdown_event: asyncio.Event,
    inflight_tasks: set[asyncio.Task],
) -> None:
    """Poll for workflow jobs, execute them, and submit results.

    Tracks in-flight tasks so the shutdown handler can wait for completion.

    Args:
        client: API client for SaaS communication.
        worker: Worker instance for job execution.
        shutdown_event: Event that signals shutdown.
        inflight_tasks: Set tracking currently running tasks.
    """
    while not shutdown_event.is_set():
        # Re-register if the server rejected us (401/403)
        if client.needs_reregister:
            try:
                logger.info("Re-registering agent after auth rejection...")
                await client.register()
                logger.info("Re-registration successful")
            except Exception as exc:
                logger.error("Re-registration failed: %s", exc)
                # Wait before next attempt
                try:
                    await asyncio.wait_for(
                        shutdown_event.wait(),
                        timeout=settings.POLL_INTERVAL,
                    )
                except asyncio.TimeoutError:
                    pass
                continue

        try:
            # Backpressure: skip polling if too many tasks are already in-flight
            if len(inflight_tasks) >= settings.MAX_CONCURRENT_JOBS:
                logger.debug(
                    "Skipping poll — %d in-flight tasks (max %d)",
                    len(inflight_tasks), settings.MAX_CONCURRENT_JOBS,
                )
            else:
                jobs = await client.get_jobs()

                if jobs:
                    task = asyncio.create_task(
                        _execute_and_submit(client, worker, jobs)
                    )
                    inflight_tasks.add(task)
                    task.add_done_callback(inflight_tasks.discard)

        except Exception as exc:
            logger.error("Poll cycle failed: %s", exc)

        try:
            await asyncio.wait_for(
                shutdown_event.wait(),
                timeout=settings.POLL_INTERVAL,
            )
        except asyncio.TimeoutError:
            pass


async def _register_with_retry(
    client: APIClient,
    shutdown_event: asyncio.Event,
) -> bool:
    """Register the agent with exponential backoff retries.

    Args:
        client: API client for SaaS communication.
        shutdown_event: Event that signals shutdown (aborts retry waits).

    Returns:
        True if registration succeeded, False otherwise.
    """
    for attempt in range(1, MAX_REGISTRATION_RETRIES + 1):
        try:
            await client.register()
            return True
        except Exception as exc:
            if attempt == MAX_REGISTRATION_RETRIES:
                logger.error(
                    "Registration failed after %d attempts: %s", attempt, exc,
                )
                return False
            delay = REGISTRATION_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            logger.warning(
                "Registration attempt %d/%d failed: %s. Retrying in %ds...",
                attempt, MAX_REGISTRATION_RETRIES, exc, delay,
            )
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=delay)
                return False  # Shutdown requested during wait
            except asyncio.TimeoutError:
                pass  # Timeout expired, retry
    return False


async def _execute_and_submit(
    client: APIClient,
    worker: Worker,
    jobs: list,
) -> None:
    """Execute workflow jobs and submit results.

    Args:
        client: API client for result submission.
        worker: Worker instance for job execution.
        jobs: List of job dicts to execute.
    """
    try:
        results = await worker.run_jobs(jobs)
        if results:
            await client.submit_results(results)
    except Exception as exc:
        logger.error("Execute-and-submit failed: %s", exc)


async def run() -> None:
    """Main entry point: register, then run poll, heartbeat, and admin loops."""
    setup_logging()

    logger.info("Starting ec-im-agent")
    logger.info("API URL: %s", settings.AGENT_API_URL)
    logger.info("Agent name: %s", settings.AGENT_NAME)
    logger.info("Poll interval: %ds", settings.POLL_INTERVAL)
    logger.info("Heartbeat interval: %ds", settings.HEARTBEAT_INTERVAL)
    logger.info("Max concurrent jobs: %d", settings.MAX_CONCURRENT_JOBS)
    logger.info("Admin port: %d", settings.ADMIN_PORT)

    # Initialize vault
    vault = Vault(settings.VAULT_PATH, settings.AGENT_API_KEY)

    # Initialize worker
    worker = Worker(vault, settings.MAX_CONCURRENT_JOBS)

    # Initialize admin server
    admin = AdminServer(vault, settings.ADMIN_PORT, admin_token=settings.ADMIN_TOKEN)

    # Initialize API client
    client = APIClient()

    shutdown_event = asyncio.Event()
    inflight_tasks: set[asyncio.Task] = set()

    # Handle graceful shutdown - platform-aware
    def _on_shutdown() -> None:
        logger.info("Shutdown signal received, stopping...")
        shutdown_event.set()

    if sys.platform == "win32":
        signal.signal(signal.SIGINT, lambda *_: _on_shutdown())
        signal.signal(signal.SIGTERM, lambda *_: _on_shutdown())
    else:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _on_shutdown)

    try:
        # Register with the central API (with retry + exponential backoff)
        logger.info("Registering agent with central API...")
        if not await _register_with_retry(client, shutdown_event):
            logger.critical("Could not register agent — exiting")
            sys.exit(1)
        logger.info("Agent registered successfully")

        # Start admin server
        await admin.start()

        # Touch heartbeat file on startup
        try:
            HEARTBEAT_FILE.touch()
        except OSError:
            pass

        # Run poll and heartbeat loops concurrently
        await asyncio.gather(
            poll_loop(client, worker, shutdown_event, inflight_tasks),
            heartbeat_loop(client, shutdown_event),
        )

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as exc:
        logger.critical("Fatal error: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        # Wait for in-flight tasks to finish (with grace period)
        if inflight_tasks:
            logger.info(
                "Waiting for %d in-flight job(s) to complete...",
                len(inflight_tasks),
            )
            try:
                await asyncio.wait_for(
                    asyncio.gather(*inflight_tasks, return_exceptions=True),
                    timeout=SHUTDOWN_GRACE_PERIOD,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "%d in-flight job(s) did not complete within %.0fs grace period",
                    len(inflight_tasks), SHUTDOWN_GRACE_PERIOD,
                )

        # Notify central API that agent is going offline
        try:
            logger.info("Sending offline heartbeat...")
            await client.heartbeat(status="offline")
            logger.info("Offline heartbeat sent successfully")
        except Exception as exc:
            logger.warning("Failed to send offline heartbeat: %s", exc)

        await worker.close()
        await admin.stop()
        await client.close()
        logger.info("Agent stopped")


if __name__ == "__main__":
    asyncio.run(run())
