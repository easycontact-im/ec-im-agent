"""Notification executor that sends notifications via the SaaS API."""

import asyncio
import logging
import time
from typing import Any

import httpx

from config import settings
from executors.base import BaseExecutor

logger = logging.getLogger("ec-im-agent.executors.notification")

DEFAULT_NOTIFICATION_TIMEOUT = 15
NOTIFICATION_ENDPOINT = "/api/v1/internal/agents/notify"
MAX_RETRIES = 3
RETRY_BACKOFF_BASE_SECONDS = 1.0
MAX_MESSAGE_SIZE = 65_536  # 64 KB


class NotificationExecutor(BaseExecutor):
    """Send notifications through the central SaaS API.

    Supported actions:
    - sendNotification: POST a notification to the SaaS notification endpoint.

    The agent does not send notifications directly. Instead, it relays the
    notification request to the SaaS API, which handles delivery via the
    configured channel (Slack, Teams, email, etc.).
    """

    def __init__(self, vault: Any) -> None:
        """Initialize the notification executor with an httpx client.

        Args:
            vault: Vault instance (unused for notifications, but required by base).
        """
        super().__init__(vault)
        self._client = httpx.AsyncClient(
            base_url=settings.AGENT_API_URL,
            headers={
                "X-Agent-Api-Key": settings.AGENT_API_KEY,
                "Content-Type": "application/json",
            },
            timeout=DEFAULT_NOTIFICATION_TIMEOUT,
        )

    async def close(self) -> None:
        """Close the underlying httpx client to release connections."""
        await self._client.aclose()

    async def execute(
        self,
        action: str,
        connection_id: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Dispatch to the notification handler.

        Args:
            action: Must be 'sendNotification'.
            connection_id: Unused for notifications.
            params: Must contain 'type' and 'message'. Optional: 'channel'.

        Returns:
            Result dict with notification delivery status.
        """
        if action == "sendNotification":
            return await self._send_notification(params)
        elif action == "testConnection":
            return await self._test_connection(params)
        else:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown notification action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

    async def _send_notification(self, params: dict[str, Any]) -> dict[str, Any]:
        """Send a notification via the SaaS API.

        Args:
            params: Notification parameters:
                - type (required): Notification channel ('slack', 'teams', 'email').
                - message (required): Notification message content.
                - channel (optional): Target channel/recipient.

        Returns:
            Result dict indicating delivery status.
        """
        notification_type = params.get("type", "")
        message = params.get("message", "")
        channel = params.get("channel")

        # M8: Truncate oversized messages to prevent excessive payload sizes
        was_truncated = len(message) > MAX_MESSAGE_SIZE
        if was_truncated:
            message = message[:MAX_MESSAGE_SIZE]
            logger.warning("Notification message truncated to %d characters", MAX_MESSAGE_SIZE)

        if not notification_type:
            return {
                "status": "error",
                "output": None,
                "error": "Notification type is required (slack, teams, email)",
                "exitCode": -1,
                "durationMs": 0,
            }

        if not message:
            return {
                "status": "error",
                "output": None,
                "error": "Notification message is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        payload: dict[str, Any] = {
            "type": notification_type,
            "message": message,
        }
        if channel:
            payload["channel"] = channel

        start = time.monotonic_ns()
        last_error: str = ""

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await self._client.post(
                    NOTIFICATION_ENDPOINT,
                    json=payload,
                )
                response.raise_for_status()

                duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

                return {
                    "status": "success",
                    "output": {
                        "delivered": True,
                        "type": notification_type,
                        "channel": channel,
                        "attempts": attempt,
                        "truncated": was_truncated,
                    },
                    "error": None,
                    "exitCode": 0,
                    "durationMs": duration_ms,
                }

            except httpx.TimeoutException:
                last_error = "Notification request timed out"
                logger.warning(
                    "Notification attempt %d/%d timed out", attempt, MAX_RETRIES,
                )

            except httpx.HTTPStatusError as exc:
                status_code = exc.response.status_code
                last_error = f"HTTP {status_code}: {exc.response.text[:200]}"
                # Don't retry client errors (4xx) except 429 and 401
                if 400 <= status_code < 500:
                    if status_code == 401 and attempt < MAX_RETRIES:
                        # M6: Auth might be transient (key rotation), retry once with longer delay
                        logger.warning(
                            "Auth error (401) on notification attempt %d/%d, retrying after 10s...",
                            attempt, MAX_RETRIES,
                        )
                        await asyncio.sleep(10)
                        continue
                    if status_code != 429:
                        logger.error("Notification failed with non-retryable status %d", status_code)
                        break
                logger.warning(
                    "Notification attempt %d/%d failed with status %d",
                    attempt, MAX_RETRIES, status_code,
                )

            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    "Notification attempt %d/%d failed: %s", attempt, MAX_RETRIES, type(exc).__name__,
                )

            # Exponential backoff before retry (skip on last attempt)
            if attempt < MAX_RETRIES:
                backoff = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
                await asyncio.sleep(backoff)

        duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
        logger.error(
            "Failed to send notification after %d attempts: %s", MAX_RETRIES, last_error,
        )
        return {
            "status": "error",
            "output": None,
            "error": f"Failed after {MAX_RETRIES} attempts: {last_error}",
            "exitCode": -1,
            "durationMs": duration_ms,
        }

    async def _test_connection(self, params: dict[str, Any]) -> dict[str, Any]:
        """Test notification connectivity by verifying the SaaS API is reachable.

        Since the notification executor relays through the SaaS API, connectivity
        is confirmed if the agent is running and the API client is functional.

        Args:
            params: Unused, present for interface consistency.

        Returns:
            Result dict indicating whether the notification channel is operational.
        """
        start = time.monotonic_ns()
        try:
            response = await self._client.get("/api/v1/health/healthz")
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if response.status_code == 200:
                return {
                    "status": "success",
                    "output": {
                        "message": "Notification channel is operational (SaaS API reachable)",
                    },
                    "error": None,
                    "exitCode": 0,
                    "durationMs": duration_ms,
                }
            else:
                return {
                    "status": "error",
                    "output": {"statusCode": response.status_code},
                    "error": f"SaaS API health check returned status {response.status_code}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Notification testConnection failed: %s", type(exc).__name__)
            return {
                "status": "error",
                "output": None,
                "error": f"SaaS API is not reachable: {type(exc).__name__}",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
