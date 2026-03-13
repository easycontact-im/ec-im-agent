"""HTTP executor for making HTTP requests to external services."""

import asyncio
import base64
import ipaddress
import logging
import socket
import time
import urllib.parse
from typing import Any

import httpx

from config import settings
from executors.base import BaseExecutor

logger = logging.getLogger("ec-im-agent.executors.http")

DEFAULT_HTTP_TIMEOUT = 30
MAX_RESPONSE_BODY_SIZE = 1_048_576  # 1 MB
MAX_REQUEST_BODY_SIZE = 10_485_760  # 10 MB


class HTTPExecutor(BaseExecutor):
    """Execute HTTP requests to external services.

    Supported actions:
    - request: Make an HTTP request (GET, POST, PUT, DELETE, PATCH).
    """

    def __init__(self, vault: Any) -> None:
        """Initialize the HTTP executor with a reusable httpx client.

        Args:
            vault: Vault instance for credential access.
        """
        super().__init__(vault)
        self._client = httpx.AsyncClient(
            timeout=DEFAULT_HTTP_TIMEOUT,
            # Disabled to prevent SSRF bypass via redirects to internal IPs
            follow_redirects=False,
            limits=httpx.Limits(
                max_connections=20,
                max_keepalive_connections=10,
            ),
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
        """Dispatch to the HTTP request handler.

        Args:
            action: Must be 'request'.
            connection_id: Optional connection ID for auth credentials.
            params: Must contain 'url', 'method'. Optional: 'headers', 'body', 'timeout'.

        Returns:
            Result dict with HTTP response data.
        """
        if action == "request":
            return await self._make_request(connection_id, params)
        elif action == "testConnection":
            return await self._test_connection(connection_id, params)
        else:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown HTTP action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

    async def _test_connection(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Test HTTP connectivity by making a HEAD or GET request to the configured URL.

        Args:
            connection_id: Optional connection ID for auth credentials.
            params: Must contain 'connectionConfig' with at least a 'host' or 'url'.

        Returns:
            Result dict indicating connection success or failure.
        """
        config = params.get("connectionConfig", {})
        url = config.get("url") or config.get("host", "")

        if not url:
            return {
                "status": "error",
                "output": None,
                "error": "Connection config must have 'url' or 'host' for HTTP test",
                "exitCode": -1,
                "durationMs": 0,
            }

        # Ensure URL has a scheme
        if not url.startswith("http://") and not url.startswith("https://"):
            url = f"https://{url}"

        test_params = {
            "url": url,
            "method": "HEAD",
            "timeout": params.get("timeout", 10),
        }

        result = await self._make_request(connection_id, test_params)

        # HEAD may fail on some servers; retry with GET if needed
        if result["status"] == "error" and "405" in (result.get("error") or ""):
            test_params["method"] = "GET"
            result = await self._make_request(connection_id, test_params)

        # Enrich output with test context
        if result["status"] == "success":
            result["output"]["message"] = "HTTP connection successful"

        return result

    @staticmethod
    async def _check_ssrf(url: str) -> tuple[dict[str, Any] | None, str | None, str | None]:
        """Check if a URL targets a private/internal network.

        Runs DNS resolution in a thread executor to avoid blocking the event loop.
        Returns the first safe resolved IP to pin the actual HTTP request to,
        preventing DNS rebinding attacks (TOCTOU gap between resolve and connect).

        Args:
            url: The target URL to validate.

        Returns:
            Tuple of (error_result, resolved_ip, original_hostname):
            - If blocked: (error_dict, None, None)
            - If safe: (None, resolved_ip_string, original_hostname)
        """
        try:
            parsed = urllib.parse.urlparse(url)
            hostname = parsed.hostname
            if not hostname:
                return {
                    "status": "error",
                    "output": None,
                    "error": "Could not parse hostname from URL",
                    "exitCode": -1,
                    "durationMs": 0,
                }, None, None

            # Resolve hostname to IP addresses in a thread to avoid blocking
            loop = asyncio.get_running_loop()
            addr_infos = await loop.run_in_executor(
                None, socket.getaddrinfo, hostname, None,
            )
            for addr_info in addr_infos:
                ip_str = addr_info[4][0]
                ip = ipaddress.ip_address(ip_str)
                if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                    logger.warning(
                        "SSRF blocked: URL %s resolves to private/internal IP %s",
                        url, ip_str,
                    )
                    return {
                        "status": "error",
                        "output": None,
                        "error": f"URL targets a private/internal network address ({ip_str}). "
                                 f"Set ALLOW_PRIVATE_NETWORK=true in agent config to override.",
                        "exitCode": -1,
                        "durationMs": 0,
                    }, None, None

            # A2: If DNS returned no addresses, block the request
            if not addr_infos:
                return {
                    "status": "error",
                    "output": None,
                    "error": f"DNS resolution for {hostname} returned no addresses",
                    "exitCode": -1,
                    "durationMs": 0,
                }, None, None

            # Return the first resolved IP to pin the request to it
            first_ip = addr_infos[0][4][0]
            return None, first_ip, hostname

        except socket.gaierror as exc:
            return {
                "status": "error",
                "output": None,
                "error": f"Could not resolve hostname: {exc}",
                "exitCode": -1,
                "durationMs": 0,
            }, None, None
        except ValueError as exc:
            return {
                "status": "error",
                "output": None,
                "error": f"Invalid URL or IP address: {exc}",
                "exitCode": -1,
                "durationMs": 0,
            }, None, None

    async def _make_request(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Make an HTTP request and return the response.

        Args:
            connection_id: Optional connection ID for auth header injection.
            params: Request parameters:
                - url (required): Target URL.
                - method (optional, default GET): HTTP method.
                - headers (optional): Request headers dict.
                - body (optional): Request body (string or dict).
                - timeout (optional, default 30): Timeout in seconds.

        Returns:
            Result dict with response status code, headers, and body.
        """
        url = params.get("url", "")
        method = params.get("method", "GET").upper()
        headers = dict(params.get("headers", {}))
        body = params.get("body")
        timeout = params.get("timeout", DEFAULT_HTTP_TIMEOUT)

        if not url:
            return {
                "status": "error",
                "output": None,
                "error": "URL is required for HTTP request",
                "exitCode": -1,
                "durationMs": 0,
            }

        # A1: Validate URL scheme — only allow http:// and https://
        parsed_scheme = urllib.parse.urlparse(url)
        if parsed_scheme.scheme.lower() not in ("http", "https"):
            return {
                "status": "error",
                "output": None,
                "error": f"URL scheme '{parsed_scheme.scheme}' is not allowed. Only http:// and https:// are supported.",
                "exitCode": -1,
                "durationMs": 0,
            }

        # SSRF protection: block requests to private/internal networks
        # H4: Use global config setting instead of per-request param to prevent bypass
        resolved_ip: str | None = None
        original_hostname: str | None = None
        if not settings.ALLOW_PRIVATE_NETWORK:
            ssrf_error, resolved_ip, original_hostname = await self._check_ssrf(url)
            if ssrf_error is not None:
                return ssrf_error

        # Inject auth from vault if connection_id provided
        if connection_id:
            credentials = self.vault.get_credential(connection_id)
            if credentials:
                auth_type = credentials.get("authType", "bearer")
                if auth_type == "bearer" and "token" in credentials:
                    headers["Authorization"] = f"Bearer {credentials['token']}"
                elif auth_type == "basic":
                    username = credentials.get("username", "")
                    password = credentials.get("password", "")
                    encoded = base64.b64encode(
                        f"{username}:{password}".encode()
                    ).decode()
                    headers["Authorization"] = f"Basic {encoded}"
                elif auth_type == "apiKey":
                    header_name = credentials.get("headerName", "X-API-Key")
                    headers[header_name] = credentials.get("apiKey", "")

        # M7: Reject excessively large request bodies before sending
        if body is not None:
            try:
                if isinstance(body, dict):
                    import json as _json
                    body_size = len(_json.dumps(body).encode("utf-8"))
                else:
                    body_size = len(str(body).encode("utf-8"))
                if body_size > MAX_REQUEST_BODY_SIZE:
                    return {
                        "status": "error",
                        "output": None,
                        "error": f"Request body too large: {body_size} bytes (max {MAX_REQUEST_BODY_SIZE})",
                        "exitCode": -1,
                        "durationMs": 0,
                    }
            except Exception:
                pass  # If size check fails, let the request proceed

        start = time.monotonic_ns()
        try:
            # Pin the request to the pre-resolved IP to prevent DNS rebinding.
            # Rewrite the URL to use the resolved IP and set the Host header
            # to the original hostname so the server handles it correctly.
            request_url = url
            if resolved_ip and original_hostname:
                parsed = urllib.parse.urlparse(url)
                # Wrap IPv6 addresses in brackets for URL netloc
                ip_for_url = f"[{resolved_ip}]" if ":" in resolved_ip else resolved_ip
                # Replace hostname with resolved IP in the URL
                if parsed.port:
                    netloc = f"{ip_for_url}:{parsed.port}"
                else:
                    netloc = ip_for_url
                request_url = urllib.parse.urlunparse(
                    (parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
                )
                # Set Host header to original hostname for correct virtual host routing
                # Include port in Host header if non-standard
                if parsed.port and parsed.port not in (80, 443):
                    headers.setdefault("Host", f"{original_hostname}:{parsed.port}")
                else:
                    headers.setdefault("Host", original_hostname)

            # Build request kwargs
            request_kwargs: dict[str, Any] = {
                "method": method,
                "url": request_url,
                "headers": headers,
                "timeout": timeout,
            }

            if body is not None:
                if isinstance(body, dict):
                    request_kwargs["json"] = body
                else:
                    request_kwargs["content"] = str(body)

            async with self._client.stream(**request_kwargs) as response:
                duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

                # H5: Stream response body to avoid OOM on huge responses.
                # Read only up to MAX_RESPONSE_BODY_SIZE bytes.
                chunks: list[bytes] = []
                total_read = 0
                async for chunk in response.aiter_bytes(chunk_size=65536):
                    chunks.append(chunk)
                    total_read += len(chunk)
                    if total_read >= MAX_RESPONSE_BODY_SIZE:
                        break
                raw_body = b"".join(chunks)[:MAX_RESPONSE_BODY_SIZE]
                truncated = total_read > MAX_RESPONSE_BODY_SIZE
                response_body = raw_body.decode(
                    response.encoding or "utf-8", errors="replace"
                )

                # M4: DNS rebinding post-request verification.
                # Re-resolve the hostname and verify the originally checked IP is still valid.
                if resolved_ip and original_hostname:
                    try:
                        loop = asyncio.get_running_loop()
                        addr_info = await loop.run_in_executor(
                            None, socket.getaddrinfo, original_hostname, None
                        )
                        final_ips = {addr[4][0] for addr in addr_info}
                        if resolved_ip not in final_ips:
                            logger.warning(
                                "DNS rebinding detected for %s: checked %s, now resolves to %s",
                                original_hostname, resolved_ip, final_ips,
                            )
                    except Exception:
                        pass  # Best-effort check

                # Convert headers to dict
                response_headers = dict(response.headers)

                # Handle redirect responses (3xx) — we don't follow them to prevent
                # SSRF bypass via redirects to internal IPs
                if 300 <= response.status_code < 400:
                    redirect_location = response_headers.get("location", "unknown")
                    return {
                        "status": "success",
                        "output": {
                            "statusCode": response.status_code,
                            "headers": response_headers,
                            "body": response_body,
                            "truncated": truncated,
                            "redirectLocation": redirect_location,
                            "redirectFollowed": False,
                        },
                        "error": None,
                        "exitCode": 0,
                        "durationMs": duration_ms,
                    }

                return {
                    "status": "success" if 200 <= response.status_code < 400 else "error",
                    "output": {
                        "statusCode": response.status_code,
                        "headers": response_headers,
                        "body": response_body,
                        "truncated": truncated,
                    },
                    "error": None if 200 <= response.status_code < 400 else f"HTTP {response.status_code}",
                    "exitCode": 0 if 200 <= response.status_code < 400 else 1,
                    "durationMs": duration_ms,
                }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "status": "error",
                "output": None,
                "error": f"HTTP request timed out after {timeout}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

        except httpx.ConnectError as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "status": "error",
                "output": None,
                "error": f"Connection failed: {exc}",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("HTTP request failed: %s", exc)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }
