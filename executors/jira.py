"""Jira executor for creating/updating issues and managing transitions."""

import base64
import logging
import re
import time
from typing import Any

import httpx

from executors.base import BaseExecutor

logger = logging.getLogger("ec-im-agent.executors.jira")

DEFAULT_TIMEOUT = 30

# Jira project key: 2-10 uppercase letters (e.g., PROJ, MYAPP)
_PROJECT_KEY_RE = re.compile(r"^[A-Z][A-Z0-9]{1,9}$")
# Jira issue key: PROJECT-123 (e.g., PROJ-1, MYAPP-9999)
_ISSUE_KEY_RE = re.compile(r"^[A-Z][A-Z0-9]{1,9}-\d{1,10}$")


def _validate_project_key(key: str) -> dict[str, Any] | None:
    """Validate a Jira project key format. Returns error dict or None."""
    if not _PROJECT_KEY_RE.match(key):
        return {
            "status": "error",
            "output": None,
            "error": f"Invalid project key '{key}'. Must be 2-10 uppercase letters/digits starting with a letter (e.g., PROJ).",
            "exitCode": -1,
            "durationMs": 0,
        }
    return None


def _validate_issue_key(key: str) -> dict[str, Any] | None:
    """Validate a Jira issue key format. Returns error dict or None."""
    if not _ISSUE_KEY_RE.match(key):
        return {
            "status": "error",
            "output": None,
            "error": f"Invalid issue key '{key}'. Must be in PROJECT-NUMBER format (e.g., PROJ-123).",
            "exitCode": -1,
            "durationMs": 0,
        }
    return None


class JiraExecutor(BaseExecutor):
    """Create, update, and manage Jira issues.

    Supported actions:
    - createIssue: Create a new Jira issue.
    - updateIssue: Update an existing Jira issue.
    - addComment: Add a comment to a Jira issue.
    - transitionIssue: Transition a Jira issue to a new status.
    - testConnection: Test Jira API connectivity.

    Credentials (from vault):
    - baseUrl: Jira instance URL (e.g., https://your-domain.atlassian.net)
    - email: Jira user email
    - apiToken: Jira API token
    """

    def __init__(self, vault: "Vault") -> None:
        """Initialize the Jira executor with a vault reference.

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
        """Dispatch to the appropriate Jira action handler.

        Args:
            action: The Jira action to perform (createIssue, updateIssue, etc.).
            connection_id: Connection ID for vault credential lookup.
            params: Action-specific parameters.

        Returns:
            Result dict with: status, output, error, exitCode, durationMs.
        """
        if action == "createIssue":
            return await self._create_issue(connection_id, params)
        elif action == "updateIssue":
            return await self._update_issue(connection_id, params)
        elif action == "addComment":
            return await self._add_comment(connection_id, params)
        elif action == "transitionIssue":
            return await self._transition_issue(connection_id, params)
        elif action == "testConnection":
            return await self._test_connection(connection_id, params)
        else:
            return {
                "status": "error",
                "output": None,
                "error": f"Unknown Jira action: {action}",
                "exitCode": -1,
                "durationMs": 0,
            }

    def _get_credentials(self, connection_id: str | None, params: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """Get Jira credentials from vault, merged with connection config.

        Vault stores apiToken; connection config stores baseUrl and username (email).
        This method merges both sources so the executor has all required fields.

        Args:
            connection_id: Connection ID for vault lookup.
            params: Job params that may contain connectionConfig.

        Returns:
            Credentials dict with baseUrl, email, apiToken, or None if not found.
        """
        merged: dict[str, Any] = {}

        # Get connection config from params (baseUrl, username/email)
        if params:
            config = params.get("connectionConfig", {})
            if isinstance(config, dict):
                merged.update(config)
            # Also check params directly (workflow jobs merge config into params)
            for key in ("baseUrl", "username", "email", "apiToken"):
                if key in (params or {}) and params[key]:
                    merged[key] = params[key]

        # Get vault credentials (apiToken)
        if connection_id:
            cred = self.vault.get_credential(connection_id)
            if cred:
                merged.update(cred)

        # Map 'username' to 'email' if needed (config uses 'username', executor uses 'email')
        if "email" not in merged and "username" in merged:
            merged["email"] = merged["username"]

        if not merged:
            return None
        return merged

    @staticmethod
    def _validate_base_url(base_url: str) -> dict[str, Any] | None:
        """Validate that the Jira base URL uses HTTPS.

        Args:
            base_url: The Jira instance base URL.

        Returns:
            An error result dict if the URL does not use HTTPS, or None if valid.
        """
        if not base_url.startswith("https://"):
            return {
                "status": "error",
                "output": None,
                "error": "Jira base URL must use HTTPS. Received URL does not start with 'https://'.",
                "exitCode": -1,
                "durationMs": 0,
            }
        return None

    def _auth_header(self, email: str, api_token: str) -> str:
        """Build Basic Auth header value for Jira API.

        Args:
            email: Jira user email address.
            api_token: Jira API token.

        Returns:
            Basic Auth header value string.
        """
        credentials = f"{email}:{api_token}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"

    def _headers(self, email: str, api_token: str) -> dict[str, str]:
        """Build Jira API request headers.

        Args:
            email: Jira user email address.
            api_token: Jira API token.

        Returns:
            Headers dict for Jira API requests.
        """
        return {
            "Authorization": self._auth_header(email, api_token),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    async def _create_issue(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Create a new Jira issue.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Must contain 'projectKey' and 'summary'. Optional: 'issueType',
                     'description', 'priority', 'labels', 'assignee'.

        Returns:
            Result dict with issueId, issueKey, and URL on success.
        """
        cred = self._get_credentials(connection_id, params)
        if not cred:
            return {
                "status": "error",
                "output": None,
                "error": f"No Jira credentials found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        base_url = cred.get("baseUrl", "").rstrip("/")
        email = cred.get("email", "")
        api_token = cred.get("apiToken", "")

        url_error = self._validate_base_url(base_url)
        if url_error:
            return url_error

        project_key = params.get("projectKey", "")
        summary = params.get("summary", "")
        issue_type = params.get("issueType", "Task")
        if not project_key or not summary:
            return {
                "status": "error",
                "output": None,
                "error": "'projectKey' and 'summary' are required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_project_key(project_key)
        if validation_error:
            return validation_error

        start = time.monotonic_ns()
        try:
            fields: dict[str, Any] = {
                "project": {"key": project_key},
                "summary": summary,
                "issuetype": {"name": issue_type},
            }

            description = params.get("description")
            if description:
                # Jira Cloud uses ADF (Atlassian Document Format)
                fields["description"] = {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"type": "text", "text": description}],
                        }
                    ],
                }

            priority = params.get("priority")
            if priority:
                fields["priority"] = {"name": priority}

            labels = params.get("labels")
            if labels and isinstance(labels, list):
                fields["labels"] = labels

            assignee = params.get("assignee")
            if assignee:
                fields["assignee"] = {"accountId": assignee}

            client = await self._get_client()
            response = await client.post(
                f"{base_url}/rest/api/3/issue",
                json={"fields": fields},
                headers=self._headers(email, api_token),
            )
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if response.status_code == 201:
                data = response.json()
                return {
                    "status": "success",
                    "output": {
                        "issueId": data.get("id"),
                        "issueKey": data.get("key"),
                        "url": f"{base_url}/browse/{data.get('key')}",
                        "message": f"Issue {data.get('key')} created successfully",
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
                    "error": f"Jira API error ({response.status_code}): {error_body}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Jira createIssue timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Jira API request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Jira createIssue failed for connection %s: %s", connection_id, type(exc).__name__)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _update_issue(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Update fields on an existing Jira issue.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Must contain 'issueKey'. Optional: 'summary', 'description',
                     'priority', 'labels'.

        Returns:
            Result dict confirming the update on success.
        """
        cred = self._get_credentials(connection_id, params)
        if not cred:
            return {
                "status": "error",
                "output": None,
                "error": f"No Jira credentials found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        base_url = cred.get("baseUrl", "").rstrip("/")
        email = cred.get("email", "")
        api_token = cred.get("apiToken", "")

        url_error = self._validate_base_url(base_url)
        if url_error:
            return url_error

        issue_key = params.get("issueKey", "")
        if not issue_key:
            return {
                "status": "error",
                "output": None,
                "error": "'issueKey' is required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_issue_key(issue_key)
        if validation_error:
            return validation_error

        start = time.monotonic_ns()
        try:
            fields: dict[str, Any] = {}
            summary = params.get("summary")
            if summary:
                fields["summary"] = summary

            description = params.get("description")
            if description:
                fields["description"] = {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"type": "text", "text": description}],
                        }
                    ],
                }

            priority = params.get("priority")
            if priority:
                fields["priority"] = {"name": priority}

            labels = params.get("labels")
            if labels and isinstance(labels, list):
                fields["labels"] = labels

            if not fields:
                return {
                    "status": "error",
                    "output": None,
                    "error": "No fields to update. Provide summary, description, priority, or labels.",
                    "exitCode": -1,
                    "durationMs": 0,
                }

            client = await self._get_client()
            response = await client.put(
                f"{base_url}/rest/api/3/issue/{issue_key}",
                json={"fields": fields},
                headers=self._headers(email, api_token),
            )
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if response.status_code == 204:
                return {
                    "status": "success",
                    "output": {
                        "issueKey": issue_key,
                        "message": f"Issue {issue_key} updated successfully",
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
                    "error": f"Jira API error ({response.status_code}): {error_body}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Jira updateIssue timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Jira API request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Jira updateIssue failed for connection %s: %s", connection_id, type(exc).__name__)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _add_comment(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Add a comment to a Jira issue.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Must contain 'issueKey' and 'comment'.

        Returns:
            Result dict with commentId on success.
        """
        cred = self._get_credentials(connection_id, params)
        if not cred:
            return {
                "status": "error",
                "output": None,
                "error": f"No Jira credentials found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        base_url = cred.get("baseUrl", "").rstrip("/")
        email = cred.get("email", "")
        api_token = cred.get("apiToken", "")

        url_error = self._validate_base_url(base_url)
        if url_error:
            return url_error

        issue_key = params.get("issueKey", "")
        comment = params.get("comment", "")
        if not issue_key or not comment:
            return {
                "status": "error",
                "output": None,
                "error": "'issueKey' and 'comment' are required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_issue_key(issue_key)
        if validation_error:
            return validation_error

        start = time.monotonic_ns()
        try:
            body = {
                "body": {
                    "type": "doc",
                    "version": 1,
                    "content": [
                        {
                            "type": "paragraph",
                            "content": [{"type": "text", "text": comment}],
                        }
                    ],
                }
            }

            client = await self._get_client()
            response = await client.post(
                f"{base_url}/rest/api/3/issue/{issue_key}/comment",
                json=body,
                headers=self._headers(email, api_token),
            )
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if response.status_code == 201:
                data = response.json()
                return {
                    "status": "success",
                    "output": {
                        "commentId": data.get("id"),
                        "issueKey": issue_key,
                        "message": f"Comment added to {issue_key}",
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
                    "error": f"Jira API error ({response.status_code}): {error_body}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Jira addComment timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Jira API request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Jira addComment failed for connection %s: %s", connection_id, type(exc).__name__)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }

    async def _transition_issue(
        self, connection_id: str | None, params: dict[str, Any]
    ) -> dict[str, Any]:
        """Transition a Jira issue to a new status.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Must contain 'issueKey' and 'transitionId'.

        Returns:
            Result dict confirming the transition on success.
        """
        cred = self._get_credentials(connection_id, params)
        if not cred:
            return {
                "status": "error",
                "output": None,
                "error": f"No Jira credentials found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        base_url = cred.get("baseUrl", "").rstrip("/")
        email = cred.get("email", "")
        api_token = cred.get("apiToken", "")

        url_error = self._validate_base_url(base_url)
        if url_error:
            return url_error

        issue_key = params.get("issueKey", "")
        transition_id = params.get("transitionId", "")
        if not issue_key or not transition_id:
            return {
                "status": "error",
                "output": None,
                "error": "'issueKey' and 'transitionId' are required",
                "exitCode": -1,
                "durationMs": 0,
            }

        validation_error = _validate_issue_key(issue_key)
        if validation_error:
            return validation_error

        start = time.monotonic_ns()
        try:
            client = await self._get_client()
            response = await client.post(
                f"{base_url}/rest/api/3/issue/{issue_key}/transitions",
                json={"transition": {"id": str(transition_id)}},
                headers=self._headers(email, api_token),
            )
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if response.status_code == 204:
                return {
                    "status": "success",
                    "output": {
                        "issueKey": issue_key,
                        "message": f"Issue {issue_key} transitioned successfully",
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
                    "error": f"Jira API error ({response.status_code}): {error_body}",
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Jira transitionIssue timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Jira API request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Jira transitionIssue failed for connection %s: %s", connection_id, type(exc).__name__)
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
        """Test Jira API connectivity by fetching the authenticated user's profile.

        Args:
            connection_id: Connection ID for vault credential lookup.
            params: Unused, present for interface consistency.

        Returns:
            Result dict with user info on success.
        """
        cred = self._get_credentials(connection_id, params)
        if not cred:
            return {
                "status": "error",
                "output": None,
                "error": f"No Jira credentials found for connection: {connection_id}",
                "exitCode": -1,
                "durationMs": 0,
            }

        base_url = cred.get("baseUrl", "").rstrip("/")
        email = cred.get("email", "")
        api_token = cred.get("apiToken", "")

        url_error = self._validate_base_url(base_url)
        if url_error:
            return url_error

        start = time.monotonic_ns()
        try:
            client = await self._get_client()
            response = await client.get(
                f"{base_url}/rest/api/3/myself",
                headers=self._headers(email, api_token),
            )
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)

            if response.status_code == 200:
                data = response.json()
                return {
                    "status": "success",
                    "output": {
                        "displayName": data.get("displayName"),
                        "emailAddress": data.get("emailAddress"),
                        "accountId": data.get("accountId"),
                        "message": "Jira connection successful",
                    },
                    "error": None,
                    "exitCode": 0,
                    "durationMs": duration_ms,
                }
            else:
                error_body = response.text[:500]
                detail = f"Jira auth failed ({response.status_code})"
                if error_body:
                    detail += f": {error_body}"
                if response.status_code == 401:
                    detail += ". Verify: (1) email matches your Atlassian account, (2) API token is valid (not OAuth), (3) baseUrl is correct"
                return {
                    "status": "error",
                    "output": {"statusCode": response.status_code, "body": error_body},
                    "error": detail,
                    "exitCode": 1,
                    "durationMs": duration_ms,
                }

        except httpx.TimeoutException:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Jira testConnection timed out for connection %s", connection_id)
            return {
                "status": "error",
                "output": None,
                "error": f"Jira API request timed out after {DEFAULT_TIMEOUT}s",
                "exitCode": -1,
                "durationMs": duration_ms,
            }
        except Exception as exc:
            duration_ms = int((time.monotonic_ns() - start) / 1_000_000)
            logger.error("Jira testConnection failed for connection %s: %s", connection_id, type(exc).__name__)
            return {
                "status": "error",
                "output": None,
                "error": str(exc),
                "exitCode": -1,
                "durationMs": duration_ms,
            }
