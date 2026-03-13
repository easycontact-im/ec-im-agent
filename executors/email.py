"""Email executor for sending emails via SMTP."""

import logging
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import aiosmtplib

from executors.base import BaseExecutor

logger = logging.getLogger("ec-im-agent.executors.email")

DEFAULT_TIMEOUT = 30
DEFAULT_SMTP_PORT = 587


class EmailExecutor(BaseExecutor):
    """Send emails via SMTP.

    Supported actions:
    - sendEmail: Send an email to one or more recipients.
    - testConnection: Test SMTP connectivity and authentication.

    Credentials (from vault):
    - host: SMTP server hostname (e.g., smtp.gmail.com)
    - port: SMTP server port (default: 587)
    - username: SMTP username / email address
    - password: SMTP password or app-specific password
    - useTLS: Whether to use STARTTLS (default: true)
    - fromName: Display name for the sender (optional)
    """

    async def execute(
        self,
        action: str,
        connection_id: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Dispatch to the appropriate email action handler.

        Args:
            action: The email action to perform (sendEmail, testConnection).
            connection_id: Connection ID for vault credential lookup.
            params: Action-specific parameters.

        Returns:
            Result dict with: status, output, error, exitCode, durationMs.
        """
        if action == "sendEmail":
            return await self._send_email(connection_id, params)
        elif action == "testConnection":
            return await self._test_connection(connection_id, params)
        else:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown email action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

    def _get_smtp_config(self, connection_id: str | None) -> dict[str, Any] | None:
        """Get SMTP configuration from vault.

        Args:
            connection_id: Connection ID for vault lookup.

        Returns:
            SMTP config dict, or None if not found.
        """
        if not connection_id:
            return None
        return self.vault.get_credential(connection_id)

    async def _send_email(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Send an email to one or more recipients.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Must contain 'to' and either 'body' or 'htmlBody'.
                    Optional: 'subject', 'cc', 'bcc', 'replyTo'.

        Returns:
            Result dict confirming the email was sent.
        """
        config = self._get_smtp_config(connection_id)
        if not config:
            return {
                "status": "error",
                "output": None,
                "error": f"No SMTP credentials found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        host = config.get("host", "")
        port = int(config.get("port", DEFAULT_SMTP_PORT))
        username = config.get("username", "")
        password = config.get("password", "")
        use_tls = config.get("useTLS", True)
        from_name = config.get("fromName", "")

        to_addrs = params.get("to", "")
        subject = params.get("subject", "")
        body = params.get("body", "")
        html_body = params.get("htmlBody", "")
        cc = params.get("cc", "")
        bcc = params.get("bcc", "")
        reply_to = params.get("replyTo", "")

        if not to_addrs:
            return {
                "status": "error",
                "output": None,
                "error": "'to' is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        if not body and not html_body:
            return {
                "status": "error",
                "output": None,
                "error": "Either 'body' or 'htmlBody' is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        if not host or not username:
            return {
                "status": "error",
                "output": None,
                "error": "SMTP host and username are required in connection credentials",
                "exitCode": -1,
                "durationMs": 0,
            }

        start = time.monotonic_ns()
        try:
            # Build MIME message
            msg = MIMEMultipart("alternative")
            from_addr = f"{from_name} <{username}>" if from_name else username
            msg["From"] = from_addr
            msg["To"] = to_addrs
            msg["Subject"] = subject or "(no subject)"

            if cc:
                msg["Cc"] = cc
            if reply_to:
                msg["Reply-To"] = reply_to

            if body:
                msg.attach(MIMEText(body, "plain", "utf-8"))
            if html_body:
                msg.attach(MIMEText(html_body, "html", "utf-8"))

            # Parse all recipients
            all_recipients: list[str] = []
            for addr_field in [to_addrs, cc, bcc]:
                if addr_field:
                    all_recipients.extend(
                        a.strip() for a in addr_field.split(",") if a.strip()
                    )

            # Send via aiosmtplib
            await aiosmtplib.send(
                msg,
                hostname=host,
                port=port,
                username=username,
                password=password,
                start_tls=use_tls,
                recipients=all_recipients,
                timeout=DEFAULT_TIMEOUT,
            )
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            return {
                "status": "success",
                "output": {
                    "to": to_addrs,
                    "subject": subject,
                    "recipientCount": len(all_recipients),
                    "message": f"Email sent to {len(all_recipients)} recipient(s)",
                },
                "error": None,
                "exitCode": 0,
                "durationMs": duration_ms,
            }

        except aiosmtplib.SMTPAuthenticationError as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("SMTP authentication failed for connection %s: %s", connection_id, exc)
            return {
                "status": "error",
                "output": None,
                "error": f"SMTP authentication failed: {exc}",
                "exitCode": 1,
                "durationMs": duration_ms,
            }
        except aiosmtplib.SMTPConnectError as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("SMTP connection failed for connection %s: %s", connection_id, exc)
            return {
                "status": "error",
                "output": None,
                "error": f"SMTP connection failed: {exc}",
                "exitCode": 1,
                "durationMs": duration_ms,
            }
        except TimeoutError:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("SMTP sendEmail timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"SMTP request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Email sendEmail failed for connection %s: %s", connection_id, exc)
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
        """Test SMTP connectivity by authenticating to the server.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Unused, present for interface consistency.

        Returns:
            Result dict confirming the SMTP connection is valid.
        """
        config = self._get_smtp_config(connection_id)
        if not config:
            return {
                "status": "error",
                "output": None,
                "error": f"No SMTP credentials found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        host = config.get("host", "")
        port = int(config.get("port", DEFAULT_SMTP_PORT))
        username = config.get("username", "")
        password = config.get("password", "")
        use_tls = config.get("useTLS", True)

        start = time.monotonic_ns()
        try:
            smtp = aiosmtplib.SMTP(
                hostname=host,
                port=port,
                timeout=DEFAULT_TIMEOUT,
            )
            await smtp.connect()
            if use_tls:
                await smtp.starttls()
            await smtp.login(username, password)
            await smtp.quit()
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            return {
                "status": "success",
                "output": {
                    "host": host,
                    "port": port,
                    "username": username,
                    "message": "SMTP connection successful",
                },
                "error": None,
                "exitCode": 0,
                "durationMs": duration_ms,
            }

        except aiosmtplib.SMTPAuthenticationError as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("SMTP auth test failed for connection %s: %s", connection_id, exc)
            return {
                "status": "error",
                "output": None,
                "error": f"SMTP authentication failed: {exc}",
                "exitCode": 1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("SMTP testConnection failed for connection %s: %s", connection_id, exc)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }
