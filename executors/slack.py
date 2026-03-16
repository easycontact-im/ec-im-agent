"""Slack executor for sending messages, updating messages, and adding reactions."""

import asyncio
import logging
import time
from typing import Any

import httpx

from executors.base import BaseExecutor

logger = logging.getLogger("ec-im-agent.executors.slack")

SLACK_API_BASE = "https://slack.com/api"
DEFAULT_TIMEOUT = 30
MAX_RATE_LIMIT_RETRIES = 3
MAX_RETRY_AFTER_SECONDS = 60


class SlackExecutor(BaseExecutor):
    """Send messages and interact with Slack channels.

    Supported actions:
    - sendMessage: Post a message to a Slack channel.
    - updateMessage: Update an existing Slack message.
    - addReaction: Add a reaction emoji to a message.
    - testConnection: Test Slack API connectivity.

    Credentials (from vault):
    - botToken: Slack Bot User OAuth Token (xoxb-...)
    """

    def __init__(self, vault: "Vault") -> None:
        """Initialize the Slack executor with a vault reference.

        Args:
            vault: Vault instance for accessing connection credentials.
        """
        super().__init__(vault)
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create a reusable httpx client.

        Returns:
            A long-lived httpx.AsyncClient instance.
        """
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=DEFAULT_TIMEOUT)
        return self._client

    async def close(self) -> None:
        """Close the HTTP client and release resources."""
        if self._client and not self._client.is_closed:
            await self._client.close()
            self._client = None

    async def execute(
        self,
        action: str,
        connection_id: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Dispatch to the appropriate Slack action handler.

        Args:
            action: The Slack action to perform (sendMessage, updateMessage, etc.).
            connection_id: Connection ID for vault credential lookup.
            params: Action-specific parameters.

        Returns:
            Result dict with: status, output, error, exitCode, durationMs.
        """
        if action == "sendMessage":
            return await self._send_message(connection_id, params)
        elif action == "updateMessage":
            return await self._update_message(connection_id, params)
        elif action == "addReaction":
            return await self._add_reaction(connection_id, params)
        elif action == "testConnection":
            return await self._test_connection(connection_id, params)
        else:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown Slack action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

    def _get_bot_token(self, connection_id: str | None) -> str | None:
        """Get Slack Bot Token from vault.

        Args:
            connection_id: Connection ID for vault lookup.

        Returns:
            The bot token string, or None if not found.
        """
        if not connection_id:
            return None
        cred = self.vault.get_credential(connection_id)
        if cred:
            return cred.get("botToken")
        return None

    def _headers(self, token: str) -> dict[str, str]:
        """Build Slack API request headers.

        Args:
            token: Slack Bot User OAuth Token.

        Returns:
            Headers dict for Slack API requests.
        """
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }

    async def _slack_api_call(
        self,
        method: str,
        token: str,
        payload: dict[str, Any],
        timeout: int = DEFAULT_TIMEOUT,
    ) -> dict[str, Any]:
        """Make a Slack Web API call with automatic 429 rate-limit retry.

        Args:
            method: Slack API method name (e.g., 'chat.postMessage').
            token: Slack Bot User OAuth Token.
            payload: JSON payload for the API call.
            timeout: Request timeout in seconds.

        Returns:
            Parsed JSON response from Slack API.

        Raises:
            httpx.HTTPStatusError: If a non-429 HTTP error occurs or retries are exhausted.
        """
        url = f"{SLACK_API_BASE}/{method}"
        client = await self._get_client()

        for attempt in range(1, MAX_RATE_LIMIT_RETRIES + 1):
            response = await client.post(
                url,
                json=payload,
                headers=self._headers(token),
            )

            if response.status_code != 429:
                response.raise_for_status()
                return response.json()

            # 429 rate limited — respect Retry-After header
            retry_after = min(
                int(response.headers.get("Retry-After", "5")),
                MAX_RETRY_AFTER_SECONDS,
            )
            if attempt < MAX_RATE_LIMIT_RETRIES:
                logger.warning(
                    "Slack rate limited on %s (attempt %d/%d), retrying after %ds",
                    method, attempt, MAX_RATE_LIMIT_RETRIES, retry_after,
                )
                await asyncio.sleep(retry_after)
            else:
                logger.error(
                    "Slack rate limited on %s, exhausted %d retries",
                    method, MAX_RATE_LIMIT_RETRIES,
                )
                response.raise_for_status()

        return {}

    async def _send_message(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Post a message to a Slack channel.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Must contain 'channel' and 'message'. Optional: 'threadTs', 'blocks'.

        Returns:
            Result dict with channel and message timestamp on success.
        """
        token = self._get_bot_token(connection_id)
        if not token:
            return {
                "status": "error",
                "output": None,
                "error": f"No Slack bot token found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        channel = params.get("channel", "")
        message = params.get("message", "")
        if not channel or not message:
            return {
                "status": "error",
                "output": None,
                "error": "Both 'channel' and 'message' are required",
                "exitCode": -1,
                "durationMs": 0,
            }

        start = time.monotonic_ns()
        try:
            payload: dict[str, Any] = {
                "channel": channel,
                "text": message,
            }

            # Optional: thread_ts for threading
            thread_ts = params.get("threadTs")
            if thread_ts:
                payload["thread_ts"] = thread_ts

            # Optional: blocks for rich formatting
            blocks = params.get("blocks")
            if blocks:
                payload["blocks"] = blocks

            result = await self._slack_api_call("chat.postMessage", token, payload)
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if not result.get("ok"):
                return {
                    "status": "error",
                    "output": result,
                    "error": f"Slack API error: {result.get('error', 'unknown')}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

            return {
                "status": "success",
                "output": {
                    "channel": result.get("channel"),
                    "ts": result.get("ts"),
                    "message": "Message sent successfully",
                },
                "error": None,
                "exitCode": 0,
                "durationMs": duration_ms,
            }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Slack sendMessage timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Slack API request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Slack sendMessage failed for connection %s: %s", connection_id, type(exc).__name__)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _update_message(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Update an existing Slack message.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Must contain 'channel', 'ts', and 'message'.

        Returns:
            Result dict with channel and message timestamp on success.
        """
        token = self._get_bot_token(connection_id)
        if not token:
            return {
                "status": "error",
                "output": None,
                "error": f"No Slack bot token found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        channel = params.get("channel", "")
        ts = params.get("ts", "")
        message = params.get("message", "")
        if not channel or not ts or not message:
            return {
                "status": "error",
                "output": None,
                "error": "'channel', 'ts', and 'message' are all required",
                "exitCode": -1,
                "durationMs": 0,
            }

        start = time.monotonic_ns()
        try:
            result = await self._slack_api_call("chat.update", token, {
                "channel": channel,
                "ts": ts,
                "text": message,
            })
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if not result.get("ok"):
                return {
                    "status": "error",
                    "output": result,
                    "error": f"Slack API error: {result.get('error', 'unknown')}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

            return {
                "status": "success",
                "output": {
                    "channel": result.get("channel"),
                    "ts": result.get("ts"),
                    "message": "Message updated successfully",
                },
                "error": None,
                "exitCode": 0,
                "durationMs": duration_ms,
            }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Slack updateMessage timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Slack API request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Slack updateMessage failed for connection %s: %s", connection_id, type(exc).__name__)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _add_reaction(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Add a reaction emoji to a message.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Must contain 'channel', 'ts', and 'emoji'.

        Returns:
            Result dict confirming the reaction was added.
        """
        token = self._get_bot_token(connection_id)
        if not token:
            return {
                "status": "error",
                "output": None,
                "error": f"No Slack bot token found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        channel = params.get("channel", "")
        ts = params.get("ts", "")
        emoji = params.get("emoji", "")
        if not channel or not ts or not emoji:
            return {
                "status": "error",
                "output": None,
                "error": "'channel', 'ts', and 'emoji' are all required",
                "exitCode": -1,
                "durationMs": 0,
            }

        # Strip colons from emoji name (users might pass :thumbsup: or thumbsup)
        emoji_name = emoji.strip(":")

        start = time.monotonic_ns()
        try:
            result = await self._slack_api_call("reactions.add", token, {
                "channel": channel,
                "timestamp": ts,
                "name": emoji_name,
            })
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if not result.get("ok"):
                return {
                    "status": "error",
                    "output": result,
                    "error": f"Slack API error: {result.get('error', 'unknown')}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

            return {
                "status": "success",
                "output": {"message": f"Reaction :{emoji_name}: added"},
                "error": None,
                "exitCode": 0,
                "durationMs": duration_ms,
            }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Slack addReaction timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Slack API request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Slack addReaction failed for connection %s: %s", connection_id, type(exc).__name__)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _test_connection(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Test Slack API connectivity by calling auth.test.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Unused, present for interface consistency.

        Returns:
            Result dict with team and user info on success.
        """
        token = self._get_bot_token(connection_id)
        if not token:
            return {
                "status": "error",
                "output": None,
                "error": f"No Slack bot token found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        start = time.monotonic_ns()
        try:
            result = await self._slack_api_call("auth.test", token, {})
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if not result.get("ok"):
                return {
                    "status": "error",
                    "output": result,
                    "error": f"Slack auth test failed: {result.get('error', 'unknown')}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

            return {
                "status": "success",
                "output": {
                    "team": result.get("team"),
                    "user": result.get("user"),
                    "teamId": result.get("team_id"),
                    "message": "Slack connection successful",
                },
                "error": None,
                "exitCode": 0,
                "durationMs": duration_ms,
            }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Slack testConnection timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Slack API request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Slack testConnection failed for connection %s: %s", connection_id, type(exc).__name__)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }
