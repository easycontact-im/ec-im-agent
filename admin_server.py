"""Local admin HTTP server for credential management.

Binds ONLY to 127.0.0.1 (localhost) for security. Provides endpoints
for storing, listing, deleting, and testing connection credentials in
the local vault.
"""

import hmac
import logging
import time
from typing import Any

from aiohttp import web

from vault import Vault

logger = logging.getLogger("ec-im-agent.admin_server")

ADMIN_BIND_HOST = "127.0.0.1"
MAX_REQUEST_BODY_SIZE = 1_048_576  # 1 MB
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX_REQUESTS = 60  # max requests per window


RATE_LIMIT_MAX_CLIENTS = 100  # max unique client IPs tracked
RATE_LIMIT_CLEANUP_INTERVAL = 300  # seconds between idle client cleanup


class _RateLimiter:
    """Sliding-window rate limiter per client IP with bounded memory."""

    def __init__(self, window: int = RATE_LIMIT_WINDOW, max_requests: int = RATE_LIMIT_MAX_REQUESTS) -> None:
        self._window = window
        self._max_requests = max_requests
        self._requests: dict[str, list[float]] = {}
        self._last_cleanup: float = time.monotonic()

    def _cleanup_idle_clients(self, now: float) -> None:
        """Remove client IPs that have no recent requests to prevent unbounded growth."""
        idle_threshold = now - self._window * 2
        stale_keys = [
            ip for ip, timestamps in self._requests.items()
            if not timestamps or timestamps[-1] < idle_threshold
        ]
        for key in stale_keys:
            del self._requests[key]

        # Hard cap: if still over limit, evict oldest clients
        if len(self._requests) > RATE_LIMIT_MAX_CLIENTS:
            sorted_ips = sorted(
                self._requests.keys(),
                key=lambda ip: self._requests[ip][-1] if self._requests[ip] else 0,
            )
            for ip in sorted_ips[:len(self._requests) - RATE_LIMIT_MAX_CLIENTS]:
                del self._requests[ip]

        self._last_cleanup = now

    def is_allowed(self, client_ip: str) -> bool:
        """Check if a request from client_ip is within rate limits."""
        now = time.monotonic()

        # Periodic cleanup to prevent unbounded memory growth
        if now - self._last_cleanup > RATE_LIMIT_CLEANUP_INTERVAL or len(self._requests) > RATE_LIMIT_MAX_CLIENTS:
            self._cleanup_idle_clients(now)

        timestamps = self._requests.get(client_ip, [])
        # Remove expired entries
        timestamps = [t for t in timestamps if now - t < self._window]
        if len(timestamps) >= self._max_requests:
            self._requests[client_ip] = timestamps
            return False
        timestamps.append(now)
        self._requests[client_ip] = timestamps
        return True


@web.middleware
async def _bearer_token_middleware(
    request: web.Request,
    handler: Any,
) -> web.StreamResponse:
    """Middleware that validates Bearer token authentication with rate limiting.

    The expected token is stored on the app instance under the
    '_admin_token' key. Uses constant-time comparison to prevent
    timing attacks.
    """
    # Rate limiting — limiter instance is stored on the app to avoid stale module-level state
    rate_limiter: _RateLimiter = request.app["_rate_limiter"]
    client_ip = request.remote or "127.0.0.1"
    if not rate_limiter.is_allowed(client_ip):
        return web.json_response(
            {"success": False, "error": "Rate limit exceeded. Try again later."},
            status=429,
        )

    expected_token: str | None = request.app.get("_admin_token")
    if expected_token is not None:
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return web.json_response(
                {"success": False, "error": "Authorization header with Bearer token required"},
                status=401,
            )
        provided_token = auth_header[len("Bearer "):]
        # Constant-time comparison to prevent timing attacks
        if not hmac.compare_digest(provided_token, expected_token):
            return web.json_response(
                {"success": False, "error": "Invalid bearer token"},
                status=401,
            )
    return await handler(request)


class AdminServer:
    """Local admin HTTP server for vault credential management.

    Endpoints:
    - PUT  /connections/{id}      - Store a credential in the vault.
    - GET  /connections           - List all credential IDs.
    - DELETE /connections/{id}    - Remove a credential from the vault.
    - POST /connections/{id}/test - Test a connection locally.
    """

    def __init__(self, vault: Vault, port: int, admin_token: str | None = None) -> None:
        """Initialize the admin server.

        Args:
            vault: Vault instance for credential operations.
            port: Port to bind the admin server to.
            admin_token: Bearer token for authentication. Required for security.
        """
        self._vault = vault
        self._port = port
        middlewares: list[Any] = [_bearer_token_middleware]
        self._app = web.Application(
            middlewares=middlewares,
            client_max_size=MAX_REQUEST_BODY_SIZE,
        )
        self._app["_rate_limiter"] = _RateLimiter()
        if admin_token is not None:
            self._app["_admin_token"] = admin_token
        else:
            logger.warning(
                "Admin server started WITHOUT authentication token. "
                "Set ADMIN_TOKEN environment variable to secure the admin API."
            )
        self._runner: web.AppRunner | None = None
        self._setup_routes()

    def _setup_routes(self) -> None:
        """Register all admin endpoint routes."""
        self._app.router.add_put("/connections/{id}", self._put_connection)
        self._app.router.add_get("/connections", self._list_connections)
        self._app.router.add_delete("/connections/{id}", self._delete_connection)
        self._app.router.add_post("/connections/{id}/test", self._test_connection)

    async def start(self) -> None:
        """Start the admin HTTP server on localhost.

        Binds only to 127.0.0.1 for security -- never exposed externally.
        """
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, ADMIN_BIND_HOST, self._port)
        await site.start()
        logger.info(
            "Admin server listening on %s:%d", ADMIN_BIND_HOST, self._port,
        )

    async def stop(self) -> None:
        """Stop the admin HTTP server and clean up resources."""
        if self._runner:
            await self._runner.cleanup()
            logger.info("Admin server stopped")

    async def _put_connection(self, request: web.Request) -> web.Response:
        """Store a credential in the vault.

        PUT /connections/{id}
        Body: JSON credential data (host, port, username, password/key, etc.)

        Returns:
            200 on success, 400 on invalid input.
        """
        connection_id = request.match_info["id"]

        try:
            credential_data = await request.json()
        except Exception:
            return web.json_response(
                {"success": False, "error": "Invalid JSON body"},
                status=400,
            )

        if not isinstance(credential_data, dict):
            return web.json_response(
                {"success": False, "error": "Body must be a JSON object"},
                status=400,
            )

        self._vault.store_credential(connection_id, credential_data)

        logger.info("Stored credential via admin API: %s", connection_id)
        return web.json_response({
            "success": True,
            "data": {"connectionId": connection_id},
            "message": f"Credential stored for connection: {connection_id}",
        })

    async def _list_connections(self, request: web.Request) -> web.Response:
        """List all credential IDs in the vault.

        GET /connections

        Returns:
            200 with list of connection IDs.
        """
        credential_ids = self._vault.list_credentials()

        return web.json_response({
            "success": True,
            "data": credential_ids,
            "count": len(credential_ids),
        })

    async def _delete_connection(self, request: web.Request) -> web.Response:
        """Remove a credential from the vault.

        DELETE /connections/{id}

        Returns:
            200 on success, 404 if not found.
        """
        connection_id = request.match_info["id"]

        deleted = self._vault.delete_credential(connection_id)

        if deleted:
            logger.info("Deleted credential via admin API: %s", connection_id)
            return web.json_response({
                "success": True,
                "data": {"connectionId": connection_id},
                "message": f"Credential deleted for connection: {connection_id}",
            })
        else:
            return web.json_response(
                {
                    "success": False,
                    "error": f"Credential not found for connection: {connection_id}",
                },
                status=404,
            )

    async def _test_connection(self, request: web.Request) -> web.Response:
        """Test a connection locally by verifying the credential exists.

        POST /connections/{id}/test

        Returns:
            200 if credential exists, 404 if not found.
        """
        connection_id = request.match_info["id"]

        credential = self._vault.get_credential(connection_id)

        if credential is None:
            return web.json_response(
                {
                    "success": False,
                    "error": f"Credential not found for connection: {connection_id}",
                },
                status=404,
            )

        # Return credential keys (not values) so the caller knows what's stored
        credential_keys = list(credential.keys())

        return web.json_response({
            "success": True,
            "message": f"Credential found for connection: {connection_id}",
            "data": {
                "connectionId": connection_id,
                "fields": credential_keys,
            },
        })
