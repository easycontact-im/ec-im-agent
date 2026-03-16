"""Microsoft Teams executor for sending messages via Incoming Webhooks and Workflows."""

import logging
import time
from typing import Any

import httpx

from executors.base import BaseExecutor

logger = logging.getLogger("ec-im-agent.executors.teams")

DEFAULT_TIMEOUT = 30


class TeamsExecutor(BaseExecutor):
    """Send messages and cards to Microsoft Teams channels.

    Supported actions:
    - sendMessage: Post a message to a Teams channel via webhook.
    - sendAdaptiveCard: Post an Adaptive Card to a Teams channel via webhook.
    - testConnection: Test webhook connectivity.

    Credentials (from vault):
    - webhookUrl: Microsoft Teams Incoming Webhook URL or Power Automate Workflows URL
    """

    def __init__(self, vault: "Vault") -> None:
        super().__init__(vault)
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create a reusable httpx client."""
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
        """Dispatch to the appropriate Teams action handler.

        Args:
            action: The Teams action to perform (sendMessage, sendAdaptiveCard, testConnection).
            connection_id: Connection ID for vault credential lookup.
            params: Action-specific parameters.

        Returns:
            Result dict with: status, output, error, exitCode, durationMs.
        """
        if action == "sendMessage":
            return await self._send_message(connection_id, params)
        elif action == "sendAdaptiveCard":
            return await self._send_adaptive_card(connection_id, params)
        elif action == "testConnection":
            return await self._test_connection(connection_id, params)
        else:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown Teams action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

    def _get_webhook_url(self, connection_id: str | None, params: dict[str, Any] | None = None) -> str | None:
        """Get Teams webhook URL from vault or connection config.

        Checks the vault first (credential data), then falls back to
        connectionConfig in job params (since Teams stores webhookUrl
        in config, not credentials).

        Args:
            connection_id: Connection ID for vault lookup.
            params: Job params that may contain connectionConfig.

        Returns:
            The webhook URL string, or None if not found.
        """
        # Try vault first
        if connection_id:
            cred = self.vault.get_credential(connection_id)
            if cred:
                url = cred.get("webhookUrl")
                if url:
                    return url

        if params:
            # Fall back to connectionConfig in params (test jobs)
            config = params.get("connectionConfig", {})
            if isinstance(config, dict):
                url = config.get("webhookUrl")
                if url:
                    return url

            # Fall back to webhookUrl directly in params (workflow jobs)
            url = params.get("webhookUrl")
            if url:
                return url

        return None

    @staticmethod
    def _validate_webhook_url(webhook_url: str) -> dict[str, Any] | None:
        """Validate that the Teams webhook URL uses HTTPS.

        Args:
            webhook_url: The webhook URL to validate.

        Returns:
            An error result dict if the URL does not use HTTPS, or None if valid.
        """
        if not webhook_url.startswith("https://"):
            return {
                "status": "error",
                "output": None,
                "error": "Teams webhook URL must use HTTPS. Received URL does not start with 'https://'.",
                "exitCode": -1,
                "durationMs": 0,
            }
        return None

    async def _send_message(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Post a message to a Teams channel via webhook.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Must contain 'message'. Optional: 'title', 'themeColor'.

        Returns:
            Result dict confirming the message was posted.
        """
        webhook_url = self._get_webhook_url(connection_id, params)
        if not webhook_url:
            return {
                "status": "error",
                "output": None,
                "error": f"No Teams webhook URL found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        url_error = self._validate_webhook_url(webhook_url)
        if url_error:
            return url_error

        message = params.get("message", "")
        title = params.get("title", "")
        theme_color = params.get("themeColor", "0076D7")

        if not message:
            return {
                "status": "error",
                "output": None,
                "error": "'message' is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        start = time.monotonic_ns()
        try:
            # Detect webhook type: Power Automate Workflows vs legacy connectors
            if "/workflows/" in webhook_url:
                # Power Automate Workflows (new) — expects Adaptive Card payload
                payload = _build_adaptive_card_payload(title, message, theme_color)
            else:
                # Legacy Office 365 connector or Incoming Webhook — MessageCard
                payload: dict[str, Any] = {
                    "@type": "MessageCard",
                    "@context": "http://schema.org/extensions",
                    "themeColor": theme_color,
                    "text": message,
                }
                if title:
                    payload["summary"] = title
                    payload["title"] = title

            client = await self._get_client()
            response = await client.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            # Teams webhooks return 200 with "1" on success (legacy) or 202 (Workflows)
            if response.status_code in (200, 202):
                return {
                    "status": "success",
                    "output": {
                        "message": "Message posted to Teams channel",
                    },
                    "error": None,
                    "exitCode": 0,
                    "durationMs": duration_ms,
                }
            else:
                error_body = response.text[:500]
                return {
                    "status": "error",
                    "output": {"statusCode": response.status_code},
                    "error": f"Teams webhook error ({response.status_code}): {error_body}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Teams sendMessage timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Teams webhook request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Teams sendMessage failed for connection %s: %s", connection_id, exc)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _send_adaptive_card(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Post an Adaptive Card to a Teams channel via webhook.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Must contain 'cardJson' (Adaptive Card JSON string or dict).
                    Optional: 'title'.

        Returns:
            Result dict confirming the card was posted.
        """
        webhook_url = self._get_webhook_url(connection_id, params)
        if not webhook_url:
            return {
                "status": "error",
                "output": None,
                "error": f"No Teams webhook URL found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        url_error = self._validate_webhook_url(webhook_url)
        if url_error:
            return url_error

        card_json = params.get("cardJson")
        if not card_json:
            return {
                "status": "error",
                "output": None,
                "error": "'cardJson' is required (Adaptive Card JSON)",
                "exitCode": -1,
                "durationMs": 0,
            }

        start = time.monotonic_ns()
        try:
            import json

            # Parse cardJson if it's a string
            if isinstance(card_json, str):
                try:
                    card = json.loads(card_json)
                except json.JSONDecodeError as jde:
                    return {
                        "status": "error",
                        "output": None,
                        "error": f"Invalid Adaptive Card JSON: {jde}",
                        "exitCode": -1,
                        "durationMs": 0,
                    }
            else:
                card = card_json

            # Wrap in attachment format for webhooks
            payload = {
                "type": "message",
                "attachments": [
                    {
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "contentUrl": None,
                        "content": card,
                    }
                ],
            }

            client = await self._get_client()
            response = await client.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if response.status_code in (200, 202):
                return {
                    "status": "success",
                    "output": {
                        "message": "Adaptive Card posted to Teams channel",
                    },
                    "error": None,
                    "exitCode": 0,
                    "durationMs": duration_ms,
                }
            else:
                error_body = response.text[:500]
                return {
                    "status": "error",
                    "output": {"statusCode": response.status_code},
                    "error": f"Teams webhook error ({response.status_code}): {error_body}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Teams sendAdaptiveCard timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Teams webhook request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Teams sendAdaptiveCard failed for connection %s: %s", connection_id, exc)
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
        """Test Teams webhook connectivity by sending a test message.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Unused, present for interface consistency.

        Returns:
            Result dict confirming the webhook is reachable.
        """
        webhook_url = self._get_webhook_url(connection_id, params)
        if not webhook_url:
            return {
                "status": "error",
                "output": None,
                "error": f"No Teams webhook URL found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        url_error = self._validate_webhook_url(webhook_url)
        if url_error:
            return url_error

        start = time.monotonic_ns()
        try:
            # Send a minimal test message
            if "/workflows/" in webhook_url:
                payload = _build_adaptive_card_payload(
                    "EasyAlert Connection Test",
                    "This is a test message from EasyAlert to verify the Teams webhook connection.",
                    "00CC6A",
                )
            else:
                payload = {
                    "@type": "MessageCard",
                    "@context": "http://schema.org/extensions",
                    "themeColor": "00CC6A",
                    "summary": "EasyAlert Connection Test",
                    "title": "EasyAlert Connection Test",
                    "text": "This is a test message from EasyAlert to verify the Teams webhook connection.",
                }

            client = await self._get_client()
            response = await client.post(
                webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if response.status_code in (200, 202):
                return {
                    "status": "success",
                    "output": {
                        "message": "Teams webhook connection successful",
                    },
                    "error": None,
                    "exitCode": 0,
                    "durationMs": duration_ms,
                }
            else:
                error_body = response.text[:500]
                return {
                    "status": "error",
                    "output": {"statusCode": response.status_code},
                    "error": f"Teams webhook test failed ({response.status_code}): {error_body}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Teams testConnection timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Teams webhook request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Teams testConnection failed for connection %s: %s", connection_id, exc)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }


def _build_adaptive_card_payload(
    title: str, message: str, theme_color: str
) -> dict[str, Any]:
    """Build a Power Automate Workflows-compatible Adaptive Card payload.

    Args:
        title: Card title text.
        message: Card body text.
        theme_color: Hex color for the accent bar (without #).

    Returns:
        Adaptive Card payload dict for Teams Workflows webhook.
    """
    body_items: list[dict[str, Any]] = []
    if title:
        body_items.append({
            "type": "TextBlock",
            "size": "Medium",
            "weight": "Bolder",
            "text": title,
            "color": "Accent",
        })
    body_items.append({
        "type": "TextBlock",
        "text": message,
        "wrap": True,
    })

    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "contentUrl": None,
                "content": {
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": body_items,
                },
            }
        ],
    }
