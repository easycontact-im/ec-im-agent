"""HTTP executor for making HTTP requests to external services."""

import asyncio
import base64
import ipaddress
import json
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
        url = config.get("baseUrl") or config.get("url") or config.get("host", "")

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
                # Strip IPv6 zone ID (e.g., "fe80::1%25eth0" → "fe80::1")
                # Zone IDs are local identifiers that could bypass IP-based SSRF checks
                if '%' in ip_str:
                    ip_str = ip_str.split('%')[0]
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
                - headers (optional): Per-request headers dict.
                - queryParams (optional): Query parameters dict (appended to URL).
                - body (optional): Request body (string or dict).
                - bodyType (optional): Body encoding: json|form|raw|none|auto.
                - rawContentType (optional): Content-Type for raw body type.
                - timeout (optional, default 30): Timeout in seconds.
                - assertions (optional): Response assertions dict.

        Returns:
            Result dict with response status code, headers, body, and bodyParsed.
        """
        url = params.get("url", "")
        method = params.get("method", "GET").upper()
        headers = dict(params.get("headers", {}))
        body = params.get("body")
        body_type = params.get("bodyType", "auto")
        raw_content_type = params.get("rawContentType", "text/plain")
        timeout = params.get("timeout") or params.get("timeoutSeconds") or DEFAULT_HTTP_TIMEOUT
        query_params = params.get("queryParams")
        assertions = params.get("assertions", {})

        # Merge connection baseUrl with relative URL path
        base_url = params.get("baseUrl", "")
        if base_url and url and not url.startswith("http://") and not url.startswith("https://"):
            url = base_url.rstrip("/") + "/" + url.lstrip("/")
        elif base_url and not url:
            url = base_url

        if not url:
            return {
                "status": "error",
                "output": None,
                "error": "URL is required for HTTP request",
                "exitCode": -1,
                "durationMs": 0,
            }

        # Append query params to URL if provided
        if query_params and isinstance(query_params, dict):
            parsed_url = urllib.parse.urlparse(url)
            existing_qs = urllib.parse.parse_qs(parsed_url.query, keep_blank_values=True)
            for k, v in query_params.items():
                existing_qs[k] = [str(v)]
            new_query = urllib.parse.urlencode(existing_qs, doseq=True)
            url = urllib.parse.urlunparse((
                parsed_url.scheme, parsed_url.netloc, parsed_url.path,
                parsed_url.params, new_query, parsed_url.fragment,
            ))

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
            except Exception as exc:
                logger.warning("Failed to validate request body size: %s", type(exc).__name__)

        start = time.monotonic_ns()
        try:
            # DNS rebinding mitigation: rewrite URL to resolved IP for plain HTTP.
            # For HTTPS, TLS certificate verification binds the connection to the
            # hostname, so rewriting would break cert validation without adding
            # security benefit.
            request_url = url
            if resolved_ip and original_hostname:
                parsed = urllib.parse.urlparse(url)
                if parsed.scheme == "http":
                    ip_for_url = f"[{resolved_ip}]" if ":" in resolved_ip else resolved_ip
                    if parsed.port:
                        netloc = f"{ip_for_url}:{parsed.port}"
                    else:
                        netloc = ip_for_url
                    request_url = urllib.parse.urlunparse(
                        (parsed.scheme, netloc, parsed.path, parsed.params, parsed.query, parsed.fragment)
                    )
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

            # Body handling based on bodyType
            if body_type == "none":
                pass  # No body
            elif body is not None:
                if body_type == "form":
                    # Form-urlencoded: body can be dict or JSON string
                    if isinstance(body, str):
                        try:
                            body = json.loads(body)
                        except (ValueError, json.JSONDecodeError):
                            pass
                    if isinstance(body, dict):
                        request_kwargs["data"] = body
                    else:
                        request_kwargs["content"] = str(body)
                elif body_type == "json" or (body_type == "auto" and isinstance(body, dict)):
                    # JSON body: parse string to dict if needed
                    if isinstance(body, str):
                        try:
                            request_kwargs["json"] = json.loads(body)
                        except (ValueError, json.JSONDecodeError):
                            request_kwargs["content"] = body
                    else:
                        request_kwargs["json"] = body
                elif body_type == "raw":
                    headers.setdefault("Content-Type", raw_content_type)
                    request_kwargs["content"] = str(body)
                else:
                    # auto + string fallback (backward compat)
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
                        # O4: Strip IPv6 zone IDs for consistent comparison with pre-check
                        final_ips = {addr[4][0].split('%')[0] for addr in addr_info}
                        if resolved_ip not in final_ips:
                            logger.warning(
                                "DNS rebinding detected for %s: checked %s, now resolves to %s",
                                original_hostname, resolved_ip, final_ips,
                            )
                    except Exception:
                        pass  # Best-effort check

                # Convert headers to dict
                response_headers = dict(response.headers)

                # Parse JSON response body if content-type indicates JSON
                body_parsed: dict[str, Any] | list[Any] | None = None
                content_type = response_headers.get("content-type", "")
                if "json" in content_type:
                    try:
                        body_parsed = json.loads(response_body)
                    except (ValueError, json.JSONDecodeError):
                        pass

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
                            "bodyParsed": body_parsed,
                            "truncated": truncated,
                            "redirectLocation": redirect_location,
                            "redirectFollowed": False,
                        },
                        "error": None,
                        "exitCode": 0,
                        "durationMs": duration_ms,
                    }

                is_success = 200 <= response.status_code < 400
                output = {
                    "statusCode": response.status_code,
                    "headers": response_headers,
                    "body": response_body,
                    "bodyParsed": body_parsed,
                    "truncated": truncated,
                }

                # Evaluate assertions (may override success to error)
                if assertions and is_success:
                    assertion_error = self._evaluate_assertions(
                        assertions, response.status_code, response_body, body_parsed,
                    )
                    if assertion_error:
                        return {
                            "status": "error",
                            "output": output,
                            "error": f"Assertion failed: {assertion_error}",
                            "exitCode": 1,
                            "durationMs": duration_ms,
                        }

                return {
                    "status": "success" if is_success else "error",
                    "output": output,
                    "error": None if is_success else f"HTTP {response.status_code}",
                    "exitCode": 0 if is_success else 1,
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

    @staticmethod
    def _evaluate_assertions(
        assertions: dict[str, Any],
        status_code: int,
        body: str,
        body_parsed: dict[str, Any] | list[Any] | None,
    ) -> str | None:
        """Evaluate response assertions and return the first failure message, or None.

        Args:
            assertions: Dict with optional keys: statusCodes, bodyContains,
                bodyNotContains, jsonPath, jsonPathValue.
            status_code: HTTP response status code.
            body: Response body as string.
            body_parsed: Parsed JSON body or None.

        Returns:
            Error message string if an assertion fails, None if all pass.
        """
        # Status code assertion
        status_codes_raw = assertions.get("statusCodes", "")
        if status_codes_raw:
            try:
                if isinstance(status_codes_raw, str):
                    allowed = [int(c.strip()) for c in status_codes_raw.split(",") if c.strip()]
                elif isinstance(status_codes_raw, list):
                    allowed = [int(c) for c in status_codes_raw]
                else:
                    allowed = []
                if allowed and status_code not in allowed:
                    return f"Status code {status_code} not in expected {allowed}"
            except (ValueError, TypeError):
                pass  # Malformed assertion — skip

        # Body contains assertion
        body_contains = assertions.get("bodyContains", "")
        if body_contains and body_contains not in body:
            return f"Body does not contain '{body_contains}'"

        # Body not contains assertion
        body_not_contains = assertions.get("bodyNotContains", "")
        if body_not_contains and body_not_contains in body:
            return f"Body contains forbidden string '{body_not_contains}'"

        # JSON path assertion
        json_path = assertions.get("jsonPath", "")
        json_path_value = assertions.get("jsonPathValue", "")
        if json_path and json_path_value:
            if body_parsed is None:
                return f"JSON path '{json_path}' cannot be evaluated: response is not JSON"
            actual = HTTPExecutor._get_json_path(body_parsed, json_path)
            if actual is None:
                return f"JSON path '{json_path}' not found in response"
            if str(actual) != str(json_path_value):
                return f"JSON path '{json_path}' is '{actual}', expected '{json_path_value}'"

        return None

    @staticmethod
    def _get_json_path(data: Any, path: str) -> Any:
        """Resolve a dot-notation path against a JSON object.

        Supports array index access via numeric segments (e.g. 'data.items.0.id').

        Args:
            data: Parsed JSON data (dict or list).
            path: Dot-notation path string.

        Returns:
            The value at the path, or None if not found.
        """
        current = data
        for segment in path.split("."):
            if current is None:
                return None
            if isinstance(current, dict):
                current = current.get(segment)
            elif isinstance(current, list):
                try:
                    current = current[int(segment)]
                except (ValueError, IndexError):
                    return None
            else:
                return None
        return current
