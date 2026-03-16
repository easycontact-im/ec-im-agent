"""Database executor for testing database connectivity and executing queries.

Supports PostgreSQL and MySQL via async drivers (asyncpg, aiomysql).
MSSQL and Oracle are stub-only (driver not installed).
"""

import asyncio
import datetime
import decimal
import json as _json
import logging
import ssl
import struct
import time
import uuid
from typing import Any

from executors.base import BaseExecutor

logger = logging.getLogger("ec-im-agent.executors.database")

DEFAULT_CONNECT_TIMEOUT = 15
DEFAULT_QUERY_TIMEOUT = 30
MAX_QUERY_LENGTH = 10_000
MAX_RESULT_ROWS = 1_000
MAX_OUTPUT_SIZE = 1_048_576  # 1 MB

# Default ports per database type
DEFAULT_PORTS: dict[str, int] = {
    "postgresql": 5432,
    "mysql": 3306,
    "mssql": 1433,
    "oracle": 1521,
}

# Lazy imports — agent starts fine without these drivers
try:
    import asyncpg
except ImportError:
    asyncpg = None

try:
    import aiomysql
except ImportError:
    aiomysql = None


def _serialize_value(value: Any) -> Any:
    """Convert database value to JSON-serializable type.

    Args:
        value: Raw value from database driver.

    Returns:
        JSON-safe equivalent.
    """
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, datetime.timedelta):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, bytes):
        return f"<{len(value)} bytes>"
    if isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _serialize_value(v) for k, v in value.items()}
    return str(value)


def _classify_query(query: str) -> str:
    """Classify SQL query as 'select' or 'modify'.

    Args:
        query: Raw SQL string.

    Returns:
        'select' for read queries, 'modify' for write queries.
    """
    stripped = query.strip().lstrip("(").strip()
    first_word = stripped.split()[0].upper() if stripped.split() else ""
    if first_word in ("SELECT", "WITH", "SHOW", "EXPLAIN", "DESCRIBE", "DESC"):
        return "select"
    return "modify"


def _parse_rows_affected(status: str) -> int | None:
    """Parse row count from PostgreSQL command status string.

    Examples: 'UPDATE 3' → 3, 'DELETE 1' → 1, 'INSERT 0 5' → 5

    Args:
        status: asyncpg command status string.

    Returns:
        Number of affected rows, or None if unparseable.
    """
    if not status:
        return None
    parts = status.split()
    if len(parts) >= 2:
        try:
            return int(parts[-1])
        except ValueError:
            pass
    return None


def _build_ssl_context(
    db_type: str, ssl_mode: str
) -> ssl.SSLContext | bool | None:
    """Build SSL parameter appropriate for the database driver.

    Args:
        db_type: 'postgresql' or 'mysql'.
        ssl_mode: 'disable', 'require', 'verify-ca', 'verify-full'.

    Returns:
        SSL parameter for the driver's connect call.
    """
    if ssl_mode == "disable":
        return False

    if ssl_mode == "require":
        if db_type == "postgresql":
            # asyncpg accepts the string "require"
            return "require"  # type: ignore[return-value]
        # aiomysql needs an SSLContext
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    if ssl_mode == "verify-ca":
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_REQUIRED
        return ctx

    # verify-full (default)
    ctx = ssl.create_default_context()
    # check_hostname=True and CERT_REQUIRED are defaults
    return ctx


class DatabaseExecutor(BaseExecutor):
    """Test database connectivity and execute queries.

    Supported actions:
    - testConnection: Verify database is reachable and accepting connections.
    - executeQuery: Execute a SQL query against PostgreSQL or MySQL.

    Credentials (from vault):
    - host: Database server hostname or IP.
    - port: Database server port (default per type).
    - username: Database username.
    - password: Database password.
    - dbname: Database name.
    - databaseType: postgresql | mysql | mssql | oracle.
    - sslMode: disable | require | verify-ca | verify-full.
    """

    async def execute(
        self,
        action: str,
        connection_id: str | None,
        params: dict[str, Any],
    ) -> dict[str, Any]:
        """Dispatch to the appropriate database action handler.

        Args:
            action: The database action (testConnection, executeQuery).
            connection_id: Connection ID for vault credential lookup.
            params: Action-specific parameters.

        Returns:
            Result dict with: status, output, error, exitCode, durationMs.
        """
        if action == "testConnection":
            return await self._test_connection(connection_id, params)
        elif action == "executeQuery":
            return await self._execute_query(connection_id, params)
        else:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown database action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

    def _get_db_config(self, connection_id: str | None) -> dict[str, Any] | None:
        """Get database configuration from vault.

        Args:
            connection_id: Connection ID for vault lookup.

        Returns:
            Database config dict, or None if not found.
        """
        if not connection_id:
            return None
        return self.vault.get_credential(connection_id)

    async def _test_connection(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Test database connectivity via TCP connection and protocol handshake.

        Establishes a TCP connection to the database port and attempts a
        protocol-level startup message for PostgreSQL (most common type).
        For other database types, verifies TCP connectivity.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Optional 'timeout'.

        Returns:
            Result dict confirming the database is reachable.
        """
        if not connection_id:
            return {
                "status": "error",
                "output": None,
                "error": "No connection selected. Assign a database connection to this action node in the workflow designer.",
                "exitCode": -1,
                "durationMs": 0,
            }

        config = self._get_db_config(connection_id)
        if not config:
            return {
                "status": "error",
                "output": None,
                "error": f"No credentials found in agent vault for connection: {connection_id}. Store credentials via the agent admin API.",
                "exitCode": -1,
                "durationMs": 0,
            }

        db_type = config.get("databaseType", "postgresql")
        host = config.get("host", "")
        default_port = DEFAULT_PORTS.get(db_type, 5432)
        port = int(config.get("port", default_port))
        username = config.get("username", "")
        dbname = config.get("dbname", "")
        timeout = params.get("timeout", DEFAULT_CONNECT_TIMEOUT)

        if not host:
            return {
                "status": "error",
                "output": None,
                "error": "Database host is required in connection credentials",
                "exitCode": -1,
                "durationMs": 0,
            }

        start = time.monotonic_ns()
        try:
            if db_type == "postgresql":
                result = await self._test_postgresql(host, port, username, dbname, timeout)
            else:
                result = await self._test_tcp(host, port, db_type, timeout)

            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            return {
                "status": "success",
                "output": {
                    "host": host,
                    "port": port,
                    "databaseType": db_type,
                    "dbname": dbname,
                    "message": result,
                },
                "error": None,
                "exitCode": 0,
                "durationMs": duration_ms,
            }

        except ConnectionRefusedError:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "status": "error",
                "output": None,
                "error": f"Connection refused: {host}:{port} — is the database running?",
                "exitCode": 1,
                "durationMs": duration_ms,
            }
        except asyncio.TimeoutError:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "status": "error",
                "output": None,
                "error": f"Connection timed out after {timeout}s to {host}:{port}",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except OSError as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "status": "error",
                "output": None,
                "error": f"Network error connecting to {host}:{port}: {type(exc).__name__}",
                "exitCode": 1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error(
                "Database testConnection failed for connection %s: %s",
                connection_id, type(exc).__name__,
            )
            return {
                "status": "error",
                "output": None,
                "error": f"Database connection test failed: {type(exc).__name__}",
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _test_postgresql(
        self, host: str, port: int, username: str, dbname: str, timeout: int
    ) -> str:
        """Test PostgreSQL connectivity via protocol-level startup message.

        Sends a PostgreSQL StartupMessage and reads the server response.
        This tests not just TCP connectivity but also that PostgreSQL is
        actually listening and responding to its wire protocol.

        Args:
            host: Database hostname.
            port: Database port.
            username: Database username.
            dbname: Database name.
            timeout: Connection timeout in seconds.

        Returns:
            Success message string.
        """
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        try:
            # Build PostgreSQL StartupMessage (v3.0 protocol)
            # Format: length(int32) + protocol_version(int32) + params + \x00
            user = username or "postgres"
            database = dbname or "postgres"

            params = b""
            params += b"user\x00" + user.encode("utf-8") + b"\x00"
            params += b"database\x00" + database.encode("utf-8") + b"\x00"
            params += b"\x00"  # Terminator

            protocol_version = (3 << 16) | 0  # v3.0
            msg_length = 4 + 4 + len(params)  # length field + protocol + params
            startup = struct.pack("!II", msg_length, protocol_version) + params

            writer.write(startup)
            await writer.drain()

            # Read response — first byte is message type
            response = await asyncio.wait_for(reader.read(1), timeout=5)
            if not response:
                return f"PostgreSQL at {host}:{port} accepted connection (no response data)"

            msg_type = chr(response[0])
            # R = AuthenticationOk/request, E = ErrorResponse
            # Any response means PostgreSQL is alive and speaking the protocol
            if msg_type == "R":
                return f"PostgreSQL at {host}:{port} is accepting connections (auth handshake started)"
            elif msg_type == "E":
                # Server responded with error (e.g., auth failure) — but it IS responding
                return f"PostgreSQL at {host}:{port} is responding (server sent error — check credentials)"
            else:
                return f"PostgreSQL at {host}:{port} is responding (msg_type={msg_type})"

        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _test_tcp(
        self, host: str, port: int, db_type: str, timeout: int
    ) -> str:
        """Test database connectivity via TCP socket connection.

        Establishes a TCP connection to verify the port is open and accepting
        connections. This works for any database type.

        Args:
            host: Database hostname.
            port: Database port.
            db_type: Database type string for display.
            timeout: Connection timeout in seconds.

        Returns:
            Success message string.
        """
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )
        try:
            # For MySQL, the server sends a greeting packet immediately
            if db_type == "mysql":
                greeting = await asyncio.wait_for(reader.read(1024), timeout=5)
                if greeting:
                    return f"MySQL at {host}:{port} is accepting connections (greeting received)"

            return f"{db_type} at {host}:{port} is accepting connections (TCP open)"
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    # ─────────────────────────────────────────────────────────────────
    # Execute Query
    # ─────────────────────────────────────────────────────────────────

    async def _execute_query(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute a SQL query on the database.

        Supports PostgreSQL (asyncpg) and MySQL (aiomysql). Opens a
        connect-per-query connection (no pooling) since each job may
        target a different database.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Must contain 'query'. Optional: 'maxRows', 'timeout'.

        Returns:
            Result dict with query results or error.
        """
        start = time.monotonic_ns()

        # 1. Get credentials from vault
        if not connection_id:
            return {
                "status": "error",
                "output": None,
                "error": "No connection selected. Assign a database connection to this action node in the workflow designer.",
                "exitCode": -1,
                "durationMs": 0,
            }

        config = self._get_db_config(connection_id)
        if not config:
            return {
                "status": "error",
                "output": None,
                "error": f"No credentials found in agent vault for connection: {connection_id}. Store credentials via the agent admin API.",
                "exitCode": -1,
                "durationMs": 0,
            }

        # 2. Validate query param
        query = params.get("query", "").strip()
        if not query:
            return {
                "status": "error",
                "output": None,
                "error": "Query parameter is required and must not be empty",
                "exitCode": -1,
                "durationMs": 0,
            }

        if len(query) > MAX_QUERY_LENGTH:
            return {
                "status": "error",
                "output": None,
                "error": f"Query exceeds maximum length of {MAX_QUERY_LENGTH} characters ({len(query)} given)",
                "exitCode": -1,
                "durationMs": 0,
            }

        # 3. Check database type support
        db_type = config.get("databaseType", "postgresql")
        if db_type in ("mssql", "oracle"):
            return {
                "status": "error",
                "output": None,
                "error": f"{db_type} driver is not installed. Only postgresql and mysql are currently supported.",
                "exitCode": -1,
                "durationMs": 0,
            }

        if db_type not in ("postgresql", "mysql"):
            return {
                "status": "error",
                "output": None,
                "error": f"Unsupported database type: {db_type}. Supported: postgresql, mysql",
                "exitCode": -1,
                "durationMs": 0,
            }

        # 4. Check driver availability
        if db_type == "postgresql" and asyncpg is None:
            return {
                "status": "error",
                "output": None,
                "error": "asyncpg driver is not installed. Install with: uv add asyncpg",
                "exitCode": -1,
                "durationMs": 0,
            }

        if db_type == "mysql" and aiomysql is None:
            return {
                "status": "error",
                "output": None,
                "error": "aiomysql driver is not installed. Install with: uv add aiomysql",
                "exitCode": -1,
                "durationMs": 0,
            }

        # 5. Parse optional params
        max_rows = min(int(params.get("maxRows", MAX_RESULT_ROWS)), MAX_RESULT_ROWS)
        query_timeout = int(params.get("timeout", DEFAULT_QUERY_TIMEOUT))

        # 6. Execute
        try:
            if db_type == "postgresql":
                output = await self._execute_postgresql(config, query, query_timeout, max_rows)
            else:
                output = await self._execute_mysql(config, query, query_timeout, max_rows)

            # 7. Truncate output if needed
            output = self._truncate_result_output(output)

            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "status": "success",
                "output": output,
                "error": None,
                "exitCode": 0,
                "durationMs": duration_ms,
            }

        except asyncio.TimeoutError:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            return {
                "status": "error",
                "output": None,
                "error": f"Query timed out after {query_timeout}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except ConnectionRefusedError:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            host = config.get("host", "?")
            port = config.get("port", DEFAULT_PORTS.get(db_type, "?"))
            return {
                "status": "error",
                "output": None,
                "error": f"Connection refused: {host}:{port} — is the database running?",
                "exitCode": 1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            error_msg = self._format_driver_error(db_type, exc)
            logger.error(
                "Database executeQuery failed for connection %s: %s: %s",
                connection_id, type(exc).__name__, exc,
            )
            return {
                "status": "error",
                "output": None,
                "error": error_msg,
                "exitCode": 1,
                "durationMs": duration_ms,
            }

    async def _execute_postgresql(
        self,
        config: dict[str, Any],
        query: str,
        timeout: int,
        max_rows: int,
    ) -> dict[str, Any]:
        """Execute a query against PostgreSQL using asyncpg.

        Args:
            config: Vault credentials (host, port, username, password, dbname, sslMode).
            query: SQL query string.
            timeout: Query timeout in seconds.
            max_rows: Maximum rows to return for SELECT queries.

        Returns:
            Structured output dict (queryType, columns/rows or rowsAffected).
        """
        host = config.get("host", "localhost")
        default_port = DEFAULT_PORTS["postgresql"]
        port = int(config.get("port", default_port))
        username = config.get("username", "postgres")
        password = config.get("password", "")
        dbname = config.get("dbname", "postgres")
        ssl_mode = config.get("sslMode", "require")

        ssl_param = _build_ssl_context("postgresql", ssl_mode)

        conn = await asyncio.wait_for(
            asyncpg.connect(
                host=host,
                port=port,
                user=username,
                password=password,
                database=dbname,
                ssl=ssl_param,
                timeout=DEFAULT_CONNECT_TIMEOUT,
            ),
            timeout=DEFAULT_CONNECT_TIMEOUT + 5,
        )

        try:
            query_type = _classify_query(query)

            if query_type == "select":
                # Use fetch with a LIMIT to cap rows
                rows = await asyncio.wait_for(
                    conn.fetch(query, timeout=timeout),
                    timeout=timeout + 5,
                )

                truncated = len(rows) > max_rows
                rows = rows[:max_rows]

                # Extract column names from first record
                columns: list[str] = []
                serialized_rows: list[list[Any]] = []

                if rows:
                    columns = list(rows[0].keys())
                    serialized_rows = [
                        [_serialize_value(row[col]) for col in columns]
                        for row in rows
                    ]

                return {
                    "queryType": "select",
                    "columns": columns,
                    "rows": serialized_rows,
                    "rowCount": len(serialized_rows),
                    "truncated": truncated,
                    "databaseType": "postgresql",
                    "dbname": dbname,
                }
            else:
                # Non-SELECT: execute and get status
                status = await asyncio.wait_for(
                    conn.execute(query, timeout=timeout),
                    timeout=timeout + 5,
                )

                rows_affected = _parse_rows_affected(status)

                return {
                    "queryType": "modify",
                    "rowsAffected": rows_affected,
                    "statusMessage": status,
                    "databaseType": "postgresql",
                    "dbname": dbname,
                }
        finally:
            await conn.close()

    async def _execute_mysql(
        self,
        config: dict[str, Any],
        query: str,
        timeout: int,
        max_rows: int,
    ) -> dict[str, Any]:
        """Execute a query against MySQL using aiomysql.

        Args:
            config: Vault credentials (host, port, username, password, dbname, sslMode).
            query: SQL query string.
            timeout: Query timeout in seconds.
            max_rows: Maximum rows to return for SELECT queries.

        Returns:
            Structured output dict (queryType, columns/rows or rowsAffected).
        """
        host = config.get("host", "localhost")
        default_port = DEFAULT_PORTS["mysql"]
        port = int(config.get("port", default_port))
        username = config.get("username", "root")
        password = config.get("password", "")
        dbname = config.get("dbname", "")
        ssl_mode = config.get("sslMode", "require")

        ssl_param = _build_ssl_context("mysql", ssl_mode)

        connect_kwargs: dict[str, Any] = {
            "host": host,
            "port": port,
            "user": username,
            "password": password,
            "db": dbname,
            "connect_timeout": DEFAULT_CONNECT_TIMEOUT,
        }
        if ssl_param is not False and ssl_param is not None:
            connect_kwargs["ssl"] = ssl_param

        conn = await asyncio.wait_for(
            aiomysql.connect(**connect_kwargs),
            timeout=DEFAULT_CONNECT_TIMEOUT + 5,
        )

        try:
            query_type = _classify_query(query)
            async with conn.cursor() as cursor:
                await asyncio.wait_for(
                    cursor.execute(query),
                    timeout=timeout + 5,
                )

                if query_type == "select":
                    rows = await cursor.fetchmany(max_rows + 1)
                    truncated = len(rows) > max_rows
                    rows = rows[:max_rows]

                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    serialized_rows = [
                        [_serialize_value(val) for val in row]
                        for row in rows
                    ]

                    return {
                        "queryType": "select",
                        "columns": columns,
                        "rows": serialized_rows,
                        "rowCount": len(serialized_rows),
                        "truncated": truncated,
                        "databaseType": "mysql",
                        "dbname": dbname,
                    }
                else:
                    await conn.commit()
                    rows_affected = cursor.rowcount

                    return {
                        "queryType": "modify",
                        "rowsAffected": rows_affected if rows_affected >= 0 else None,
                        "statusMessage": f"{query.strip().split()[0].upper()} {rows_affected}" if rows_affected >= 0 else "OK",
                        "databaseType": "mysql",
                        "dbname": dbname,
                    }
        finally:
            conn.close()

    def _truncate_result_output(self, output: dict[str, Any]) -> dict[str, Any]:
        """Truncate output if JSON serialization exceeds MAX_OUTPUT_SIZE.

        Removes rows from the end until the output fits within the limit.

        Args:
            output: Structured output dict.

        Returns:
            Possibly truncated output dict.
        """
        serialized = _json.dumps(output, default=str)
        if len(serialized.encode("utf-8")) <= MAX_OUTPUT_SIZE:
            return output

        # Only truncate SELECT results
        if output.get("queryType") != "select" or "rows" not in output:
            return output

        rows = output["rows"]
        while rows and len(_json.dumps(output, default=str).encode("utf-8")) > MAX_OUTPUT_SIZE:
            rows.pop()

        output["rows"] = rows
        output["rowCount"] = len(rows)
        output["truncated"] = True
        return output

    def _format_driver_error(self, db_type: str, exc: Exception) -> str:
        """Format driver-specific exceptions into user-friendly messages.

        Args:
            db_type: 'postgresql' or 'mysql'.
            exc: The exception raised by the driver.

        Returns:
            Human-readable error message.
        """
        exc_type = type(exc).__name__
        exc_msg = str(exc)

        if db_type == "postgresql" and asyncpg is not None:
            if isinstance(exc, asyncpg.PostgresSyntaxError):
                return f"SQL syntax error: {exc_msg}"
            if isinstance(exc, asyncpg.InvalidPasswordError):
                return f"Authentication failed: {exc_msg}"
            if isinstance(exc, asyncpg.QueryCanceledError):
                return f"Query cancelled (timeout): {exc_msg}"
            if isinstance(exc, asyncpg.PostgresError):
                return f"PostgreSQL error ({exc_type}): {exc_msg}"
            if isinstance(exc, asyncpg.InterfaceError):
                return f"Connection error: {exc_msg}"

        if db_type == "mysql" and aiomysql is not None:
            if isinstance(exc, aiomysql.OperationalError):
                return f"MySQL operational error: {exc_msg}"
            if isinstance(exc, aiomysql.ProgrammingError):
                return f"SQL error: {exc_msg}"
            if isinstance(exc, aiomysql.IntegrityError):
                return f"Integrity error: {exc_msg}"

        return f"Database error ({exc_type}): {exc_msg}"
