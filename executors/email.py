"""Email executor for sending emails via SMTP."""

import json
import logging
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import aiosmtplib

from executors.base import BaseExecutor
from formatters import format_output, FormatContext

logger = logging.getLogger("ec-im-agent.executors.email")

DEFAULT_TIMEOUT = 30
DEFAULT_SMTP_PORT = 587
IMPLICIT_SSL_PORT = 465


class EmailExecutor(BaseExecutor):
    """Send emails via SMTP.

    Supported actions:
    - sendEmail: Send an email to one or more recipients.
    - testConnection: Test SMTP connectivity and authentication.

    Credentials (from vault — merged config + secrets):
    - smtpHost: SMTP server hostname (e.g., smtp.gmail.com)
    - smtpPort: SMTP server port (default: 587)
    - username: SMTP username / email address
    - password: SMTP password or app-specific password
    - security: Encryption mode — "none", "starttls", or "ssl" (new)
    - useTls: Legacy boolean (backward compat — prefer security)
    - fromAddress: Sender email address (optional, falls back to username)
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

    @staticmethod
    def _resolve_tls_params(
        port: int, use_tls: bool, security: str | None = None
    ) -> dict[str, bool]:
        """Return the correct aiosmtplib TLS keyword arguments.

        When the new ``security`` field is present, it is used directly:
        - ``ssl``: implicit SSL (``use_tls=True``)
        - ``starttls``: STARTTLS upgrade (``start_tls=True``)
        - ``none``: no encryption

        Falls back to legacy ``use_tls`` boolean with port-based detection.

        Args:
            port: SMTP server port.
            use_tls: Legacy boolean — whether TLS is enabled.
            security: New explicit mode: ``"none"`` | ``"starttls"`` | ``"ssl"``.

        Returns:
            Dict with ``use_tls`` and ``start_tls`` keys for aiosmtplib.
        """
        if security is not None:
            if security == "ssl":
                return {"use_tls": True, "start_tls": False}
            elif security == "starttls":
                return {"use_tls": False, "start_tls": True}
            else:  # "none"
                return {"use_tls": False, "start_tls": False}

        # Legacy fallback
        if not use_tls:
            return {"use_tls": False, "start_tls": False}
        if port == IMPLICIT_SSL_PORT:
            return {"use_tls": True, "start_tls": False}
        return {"use_tls": False, "start_tls": True}

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

        host = config.get("smtpHost", "") or config.get("host", "")
        port = int(config.get("smtpPort", 0) or config.get("port", DEFAULT_SMTP_PORT))
        username = config.get("username", "")
        password = config.get("password", "")
        security = config.get("security")  # New explicit mode
        use_tls = config.get("useTls", config.get("useTLS", True))  # Legacy fallback
        from_name = config.get("fromName", "")
        from_address = config.get("fromAddress", "")

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

        # Rich formatting: auto-generate branded HTML from output data
        message_format = params.get("messageFormat", "plain")
        if message_format == "rich":
            output_data = params.get("outputData")
            if output_data:
                if isinstance(output_data, str):
                    try:
                        output_data = json.loads(output_data)
                    except (json.JSONDecodeError, ValueError):
                        pass
                ctx = FormatContext(
                    title=params.get("title", subject),
                    source_action=params.get("sourceAction", ""),
                    node_name=params.get("nodeName", ""),
                    workflow_name=params.get("workflowName", ""),
                    incident_url=params.get("incidentUrl", ""),
                    severity=params.get("severity", ""),
                    status=params.get("status", ""),
                    timestamp=params.get("timestamp", ""),
                )
                result = format_output(
                    channel="email",
                    output=output_data,
                    context=ctx,
                    action_type=params.get("sourceAction", ""),
                    output_type_hint=params.get("outputType", ""),
                )
                html_body = result.get("html", html_body)
                if not body:
                    body = result.get("text", "")

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
            sender = from_address or username
            from_addr = f"{from_name} <{sender}>" if from_name else sender
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
            tls_params = self._resolve_tls_params(port, use_tls, security=security)
            await aiosmtplib.send(
                msg,
                hostname=host,
                port=port,
                username=username,
                password=password,
                **tls_params,
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
        """Test SMTP connectivity, optionally sending a real test email.

        If ``params`` contains a ``targetEmail`` key, a real test email is sent
        to that address. Otherwise, only SMTP connectivity is verified (connect,
        STARTTLS, login, quit).

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: May contain ``targetEmail`` for sending a real test email.

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

        host = config.get("smtpHost", "") or config.get("host", "")
        port = int(config.get("smtpPort", 0) or config.get("port", DEFAULT_SMTP_PORT))
        username = config.get("username", "")
        password = config.get("password", "")
        security = config.get("security")  # New explicit mode
        use_tls = config.get("useTls", config.get("useTLS", True))  # Legacy fallback

        target_email = params.get("targetEmail")

        if target_email:
            return await self._send_test_email(config, target_email)

        start = time.monotonic_ns()
        try:
            tls_params = self._resolve_tls_params(port, use_tls, security=security)
            smtp = aiosmtplib.SMTP(
                hostname=host,
                port=port,
                timeout=DEFAULT_TIMEOUT,
                **tls_params,
            )
            await smtp.connect()
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

    async def _send_test_email(
        self, config: dict[str, Any], target_email: str
    ) -> dict[str, Any]:
        """Send a real test email to verify end-to-end delivery.

        Args:
            config: SMTP configuration from vault.
            target_email: Recipient email address.

        Returns:
            Result dict confirming the test email was sent.
        """
        host = config.get("smtpHost", "") or config.get("host", "")
        port = int(config.get("smtpPort", 0) or config.get("port", DEFAULT_SMTP_PORT))
        username = config.get("username", "")
        password = config.get("password", "")
        security = config.get("security")  # New explicit mode
        use_tls = config.get("useTls", config.get("useTLS", True))  # Legacy fallback
        from_name = config.get("fromName", "")
        from_address = config.get("fromAddress", "") or username

        start = time.monotonic_ns()
        try:
            msg = MIMEMultipart("alternative")
            sender = from_address
            from_header = f"{from_name} <{sender}>" if from_name else sender
            msg["From"] = from_header
            msg["To"] = target_email
            msg["Subject"] = "[EasyAlert] Test Email"

            body = (
                "This is a test email from EasyAlert automation.\n\n"
                "If you received this message, your email connection is configured correctly."
            )
            msg.attach(MIMEText(body, "plain", "utf-8"))

            tls_params = self._resolve_tls_params(port, use_tls, security=security)
            await aiosmtplib.send(
                msg,
                hostname=host,
                port=port,
                username=username,
                password=password,
                **tls_params,
                recipients=[target_email],
                timeout=DEFAULT_TIMEOUT,
            )
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            return {
                "status": "success",
                "output": {
                    "to": target_email,
                    "subject": "[EasyAlert] Test Email",
                    "message": f"Test email sent to {target_email}",
                },
                "error": None,
                "exitCode": 0,
                "durationMs": duration_ms,
            }

        except aiosmtplib.SMTPAuthenticationError as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("SMTP auth failed during test email: %s", exc)
            return {
                "status": "error",
                "output": None,
                "error": f"SMTP authentication failed: {exc}",
                "exitCode": 1,
                "durationMs": duration_ms,
            }
        except aiosmtplib.SMTPConnectError as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("SMTP connection failed during test email: %s", exc)
            return {
                "status": "error",
                "output": None,
                "error": f"SMTP connection failed: {exc}",
                "exitCode": 1,
                "durationMs": duration_ms,
            }
        except TimeoutError:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("SMTP test email timed out")
            return {
                "status": "error",
                "output": None,
                "error": f"SMTP request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Test email failed: %s", exc)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }
