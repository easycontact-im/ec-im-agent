"""Tests for agent executors — all I/O mocked."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from executors.base import truncate_output, BaseExecutor
from vault import Vault


# ── Fixtures ─────────────────────────────────────────────────────

@pytest.fixture
def mock_vault(tmp_path):
    """Vault with a test credential pre-stored."""
    vault_path = str(tmp_path / "test_vault.json")
    v = Vault(vault_path=vault_path, api_key="ea_agent_test_key")
    v.store_credential("conn-ssh", {
        "host": "192.168.1.10",
        "port": 22,
        "username": "deploy",
        "password": "secret123",
    })
    v.store_credential("conn-http", {
        "authType": "bearer",
        "token": "tok_abc123",
    })
    v.store_credential("conn-slack", {
        "botToken": "xoxb-test-token",
    })
    v.store_credential("conn-jira", {
        "baseUrl": "https://test.atlassian.net",
        "email": "user@test.com",
        "apiToken": "jira_tok_123",
    })
    v.store_credential("conn-k8s", {
        "kubeconfigPath": "/etc/kube/config",
    })
    return v


# ── truncate_output ──────────────────────────────────────────────

class TestTruncateOutput:
    def test_no_truncation(self):
        result = truncate_output("hello", "world")
        assert result["stdout"] == "hello"
        assert result["stderr"] == "world"
        assert result["truncated"] is False

    def test_stdout_truncation(self):
        long_output = "line\n" * 20000  # ~100KB
        result = truncate_output(long_output, "", max_size=1000)
        assert result["truncated"] is True
        assert len(result["stdout"]) <= 1001
        assert result["originalStdoutSize"] == len(long_output)

    def test_stderr_truncation(self):
        long_err = "e" * 100000
        result = truncate_output("", long_err, max_size=500)
        assert result["truncated"] is True
        assert len(result["stderr"]) <= 501

    def test_empty_output(self):
        result = truncate_output("", "")
        assert result["stdout"] == ""
        assert result["stderr"] == ""
        assert result["truncated"] is False


# ── SSH Executor ─────────────────────────────────────────────────

class TestSSHExecutor:
    @pytest.mark.asyncio
    async def test_execute_command_success(self, mock_vault):
        from executors.ssh import SSHExecutor

        executor = SSHExecutor(mock_vault)

        mock_result = MagicMock()
        mock_result.stdout = "command output\n"
        mock_result.stderr = ""
        mock_result.exit_status = 0

        # _connect does known_hosts validation — mock it directly.
        # The result is used as `async with await self._connect(...) as conn:`
        # so it must work as an async context manager.
        mock_conn = AsyncMock()
        mock_conn.run = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=False)

        with patch.object(executor, "_connect", new_callable=AsyncMock, return_value=mock_conn):
            result = await executor.execute(
                action="executeCommand",
                connection_id="conn-ssh",
                params={"command": "echo hello", "timeout": 10},
            )

        assert result["status"] == "success"
        assert result["exitCode"] == 0
        assert "command output" in result["output"]["stdout"]

    @pytest.mark.asyncio
    async def test_execute_command_no_connection(self, mock_vault):
        from executors.ssh import SSHExecutor

        executor = SSHExecutor(mock_vault)
        result = await executor.execute(
            action="executeCommand",
            connection_id="nonexistent",
            params={"command": "echo hello"},
        )
        assert result["status"] == "error"
        assert result["exitCode"] != 0

    @pytest.mark.asyncio
    async def test_unknown_action(self, mock_vault):
        from executors.ssh import SSHExecutor

        executor = SSHExecutor(mock_vault)
        result = await executor.execute(
            action="unknownAction",
            connection_id="conn-ssh",
            params={},
        )
        assert result["status"] == "error"


# ── Script Executor ──────────────────────────────────────────────

class TestScriptExecutor:
    @pytest.mark.asyncio
    async def test_bash_success(self, mock_vault):
        from executors.script import ScriptExecutor

        executor = ScriptExecutor(mock_vault)

        mock_proc = AsyncMock()
        mock_proc.communicate = AsyncMock(return_value=(b"hello\n", b""))
        mock_proc.returncode = 0

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            result = await executor.execute(
                action="bash",
                connection_id=None,
                params={"script": "echo hello"},
            )

        assert result["status"] == "success"
        assert result["exitCode"] == 0

    @pytest.mark.asyncio
    async def test_script_timeout(self, mock_vault):
        from executors.script import ScriptExecutor
        import asyncio

        executor = ScriptExecutor(mock_vault)

        mock_proc = AsyncMock()
        mock_proc.communicate = AsyncMock(side_effect=asyncio.TimeoutError())
        mock_proc.kill = MagicMock()
        mock_proc.wait = AsyncMock()

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            result = await executor.execute(
                action="bash",
                connection_id=None,
                params={"script": "sleep 999", "timeout": 1},
            )

        assert result["status"] == "error"
        assert "timed out" in result["error"].lower() or "timeout" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_missing_script_param(self, mock_vault):
        from executors.script import ScriptExecutor

        executor = ScriptExecutor(mock_vault)
        result = await executor.execute(
            action="bash",
            connection_id=None,
            params={},
        )
        assert result["status"] == "error"


# ── HTTP Executor ────────────────────────────────────────────────

class TestHTTPExecutor:
    @pytest.mark.asyncio
    async def test_get_request_success(self, mock_vault):
        from contextlib import asynccontextmanager
        from executors.http import HTTPExecutor

        executor = HTTPExecutor(mock_vault)

        # The HTTP executor uses `async with self._client.stream(...) as response:`
        # so we need to mock the stream method as an async context manager.
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "application/json"}
        mock_response.encoding = "utf-8"

        # aiter_bytes must be an async iterator
        async def fake_aiter_bytes(chunk_size=65536):
            yield b'{"ok": true}'
        mock_response.aiter_bytes = fake_aiter_bytes

        @asynccontextmanager
        async def fake_stream(**kwargs):
            yield mock_response

        # Replace the real httpx client's stream with our fake
        executor._client = MagicMock()
        executor._client.stream = fake_stream

        # Mock SSRF check to return no error + resolved IP + hostname
        with patch.object(HTTPExecutor, "_check_ssrf", new_callable=AsyncMock, return_value=(None, "93.184.216.34", "example.com")):
            result = await executor.execute(
                action="request",
                connection_id=None,
                params={"url": "https://example.com/api", "method": "GET"},
            )

        assert result["status"] == "success"
        assert result["exitCode"] == 0

    @pytest.mark.asyncio
    async def test_missing_url(self, mock_vault):
        from executors.http import HTTPExecutor

        executor = HTTPExecutor(mock_vault)
        result = await executor.execute(
            action="request",
            connection_id=None,
            params={"method": "GET"},
        )
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_private_ip_blocked(self, mock_vault):
        from executors.http import HTTPExecutor

        executor = HTTPExecutor(mock_vault)
        # The _resolve_ip method should block private IPs when ALLOW_PRIVATE_NETWORK is false
        result = await executor.execute(
            action="request",
            connection_id=None,
            params={"url": "http://192.168.1.1/admin", "method": "GET"},
        )
        assert result["status"] == "error"


# ── Kubernetes Executor ──────────────────────────────────────────

class TestKubernetesExecutor:
    @pytest.mark.asyncio
    async def test_restart_deployment(self, mock_vault):
        from executors.kubernetes import KubernetesExecutor

        executor = KubernetesExecutor(mock_vault)

        mock_proc = AsyncMock()
        mock_proc.communicate = AsyncMock(return_value=(b"deployment restarted\n", b""))
        mock_proc.returncode = 0

        # Return None for kubeconfig to skip TOCTOU path validation in _build_base_cmd
        with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
             patch.object(executor, "_get_kubeconfig_path", return_value=None):
            result = await executor.execute(
                action="restartDeployment",
                connection_id="conn-k8s",
                params={"deployment": "api-server", "namespace": "default"},
            )

        assert result["status"] == "success"
        assert result["exitCode"] == 0

    @pytest.mark.asyncio
    async def test_invalid_resource_name(self, mock_vault):
        from executors.kubernetes import KubernetesExecutor

        executor = KubernetesExecutor(mock_vault)
        result = await executor.execute(
            action="restartDeployment",
            connection_id="conn-k8s",
            params={"deployment": "../etc/passwd", "namespace": "default"},
        )
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_missing_deployment(self, mock_vault):
        from executors.kubernetes import KubernetesExecutor

        executor = KubernetesExecutor(mock_vault)
        result = await executor.execute(
            action="restartDeployment",
            connection_id="conn-k8s",
            params={"namespace": "default"},
        )
        assert result["status"] == "error"


# ── OS Service Executor ──────────────────────────────────────────

class TestOSServiceExecutor:
    @pytest.mark.asyncio
    async def test_restart_service(self, mock_vault):
        from executors.os_service import OSServiceExecutor

        executor = OSServiceExecutor(mock_vault)

        mock_proc = AsyncMock()
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))
        mock_proc.returncode = 0

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            result = await executor.execute(
                action="restartService",
                connection_id=None,
                params={"serviceName": "nginx"},
            )

        assert result["status"] == "success"
        assert result["exitCode"] == 0

    @pytest.mark.asyncio
    async def test_invalid_service_name(self, mock_vault):
        from executors.os_service import OSServiceExecutor

        executor = OSServiceExecutor(mock_vault)
        result = await executor.execute(
            action="restartService",
            connection_id=None,
            params={"serviceName": "../../etc/passwd"},
        )
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_os_restart_without_approval(self, mock_vault):
        from executors.os_service import OSServiceExecutor

        executor = OSServiceExecutor(mock_vault)
        result = await executor.execute(
            action="restartOS",
            connection_id=None,
            params={},
        )
        assert result["status"] == "error"


# ── Slack Executor ───────────────────────────────────────────────

class TestSlackExecutor:
    @pytest.mark.asyncio
    async def test_send_message(self, mock_vault):
        from executors.slack import SlackExecutor

        executor = SlackExecutor(mock_vault)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json = MagicMock(return_value={
            "ok": True,
            "channel": "C123",
            "ts": "1234567.890",
        })

        with patch("httpx.AsyncClient") as MockClient:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client

            result = await executor.execute(
                action="sendMessage",
                connection_id="conn-slack",
                params={"channel": "#general", "message": "Hello!"},
            )

        assert result["status"] == "success"
        assert result["exitCode"] == 0

    @pytest.mark.asyncio
    async def test_no_connection(self, mock_vault):
        from executors.slack import SlackExecutor

        executor = SlackExecutor(mock_vault)
        result = await executor.execute(
            action="sendMessage",
            connection_id="nonexistent",
            params={"channel": "#general", "message": "Hello!"},
        )
        assert result["status"] == "error"


# ── Jira Executor ────────────────────────────────────────────────

class TestJiraExecutor:
    @pytest.mark.asyncio
    async def test_create_issue(self, mock_vault):
        from executors.jira import JiraExecutor

        executor = JiraExecutor(mock_vault)

        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_response.json = MagicMock(return_value={
            "id": "10001",
            "key": "PROJ-1",
            "self": "https://test.atlassian.net/rest/api/3/issue/10001",
        })

        with patch("httpx.AsyncClient") as MockClient:
            mock_client = AsyncMock()
            mock_client.post = AsyncMock(return_value=mock_response)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client

            result = await executor.execute(
                action="createIssue",
                connection_id="conn-jira",
                params={
                    "projectKey": "PROJ",
                    "summary": "Test issue",
                    "issueType": "Task",
                },
            )

        assert result["status"] == "success"
        assert result["exitCode"] == 0

    @pytest.mark.asyncio
    async def test_missing_project_key(self, mock_vault):
        from executors.jira import JiraExecutor

        executor = JiraExecutor(mock_vault)
        result = await executor.execute(
            action="createIssue",
            connection_id="conn-jira",
            params={"summary": "Test issue"},
        )
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_no_connection(self, mock_vault):
        from executors.jira import JiraExecutor

        executor = JiraExecutor(mock_vault)
        result = await executor.execute(
            action="createIssue",
            connection_id="nonexistent",
            params={"projectKey": "PROJ", "summary": "Test"},
        )
        assert result["status"] == "error"

    @pytest.mark.asyncio
    async def test_invalid_project_key(self, mock_vault):
        from executors.jira import JiraExecutor

        executor = JiraExecutor(mock_vault)
        result = await executor.execute(
            action="createIssue",
            connection_id="conn-jira",
            params={"projectKey": "invalid-key!", "summary": "Test"},
        )
        assert result["status"] == "error"
        assert "Invalid project key" in result["error"]

    @pytest.mark.asyncio
    async def test_invalid_issue_key_update(self, mock_vault):
        from executors.jira import JiraExecutor

        executor = JiraExecutor(mock_vault)
        result = await executor.execute(
            action="updateIssue",
            connection_id="conn-jira",
            params={"issueKey": "not-valid", "summary": "Updated"},
        )
        assert result["status"] == "error"
        assert "Invalid issue key" in result["error"]

    @pytest.mark.asyncio
    async def test_valid_issue_key_format(self, mock_vault):
        from executors.jira import _validate_project_key, _validate_issue_key

        assert _validate_project_key("PROJ") is None
        assert _validate_project_key("AB") is None
        assert _validate_project_key("MYPROJECT1") is None
        assert _validate_project_key("a") is not None  # lowercase
        assert _validate_project_key("") is not None  # empty
        assert _validate_project_key("1PROJ") is not None  # starts with digit

        assert _validate_issue_key("PROJ-1") is None
        assert _validate_issue_key("AB-999") is None
        assert _validate_issue_key("proj-1") is not None  # lowercase
        assert _validate_issue_key("PROJ") is not None  # no number
        assert _validate_issue_key("PROJ-") is not None  # no number after dash


# ── Slack Rate Limiting ─────────────────────────────────────────

class TestSlackRateLimiting:
    @pytest.mark.asyncio
    async def test_retries_on_429(self, mock_vault):
        from executors.slack import SlackExecutor

        executor = SlackExecutor(mock_vault)

        # First call returns 429, second returns 200
        rate_limited = MagicMock()
        rate_limited.status_code = 429
        rate_limited.headers = {"Retry-After": "1"}

        ok_response = MagicMock()
        ok_response.status_code = 200
        ok_response.json = MagicMock(return_value={"ok": True, "channel": "C123", "ts": "1234.5"})

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=[rate_limited, ok_response])
        mock_client.is_closed = False
        executor._client = mock_client

        result = await executor.execute(
            action="sendMessage",
            connection_id="conn-slack",
            params={"channel": "#general", "message": "Hello!"},
        )

        assert result["status"] == "success"
        assert mock_client.post.call_count == 2


# ── Notification Executor ────────────────────────────────────────

class TestNotificationExecutor:
    @pytest.mark.asyncio
    async def test_send_notification(self, mock_vault):
        from executors.notification import NotificationExecutor

        executor = NotificationExecutor(mock_vault)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json = MagicMock(return_value={"success": True})

        # NotificationExecutor uses self._client (httpx.AsyncClient initialized in __init__)
        # Replace it with a mock to avoid real HTTP calls.
        executor._client = AsyncMock()
        executor._client.post = AsyncMock(return_value=mock_response)

        result = await executor.execute(
            action="sendNotification",
            connection_id=None,
            params={
                "type": "slack",
                "message": "Alert: server down",
                "channel": "#ops",
            },
        )

        assert result["status"] == "success"
        assert result["exitCode"] == 0
