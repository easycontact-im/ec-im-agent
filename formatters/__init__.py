"""
Rich message formatters for EasyAlert workflow automation notifications.

Detects the output archetype from upstream workflow node results and renders
channel-specific rich messages (Slack Block Kit, Teams Adaptive Cards, branded HTML email).
"""

from __future__ import annotations

import json
import logging
from enum import Enum
from dataclasses import dataclass, field

logger = logging.getLogger("ec-im-agent.formatters")


# ═══════════════════════════════════════════════════════════════
# Output Archetypes
# ═══════════════════════════════════════════════════════════════

class OutputArchetype(str, Enum):
    """Classification of workflow node output for formatting."""

    TERMINAL = "terminal"           # ssh.*, script.*, winrm.*, os.*, k8s simple
    HTTP_RESPONSE = "http_response" # http.request
    POD_LOGS = "pod_logs"           # kubernetes.getLogs (multi-pod)
    TABULAR = "tabular"             # database.executeQuery (SELECT)
    DB_MODIFY = "db_modify"         # database.executeQuery (INSERT/UPDATE/DELETE)
    ISSUE_TRACKER = "issue_tracker" # jira.*
    INCIDENT = "incident"           # trigger context (incident.*)
    CONFIRMATION = "confirmation"   # slack/teams/email send confirmations
    UNKNOWN = "unknown"             # fallback — render as JSON


# ═══════════════════════════════════════════════════════════════
# Format Context
# ═══════════════════════════════════════════════════════════════

@dataclass
class FormatContext:
    """Contextual metadata passed alongside output data for richer formatting."""

    title: str = ""
    description: str = ""
    severity: str = ""                      # critical, high, medium, low
    status: str = ""                        # triggered, acknowledged, resolved
    source_action: str = ""                 # e.g. "ssh.executeCommand"
    node_name: str = ""                     # human-readable node label
    workflow_name: str = ""
    incident_url: str = ""
    timestamp: str = ""
    extra_fields: dict[str, str] = field(default_factory=dict)


# ═══════════════════════════════════════════════════════════════
# Archetype Detection (duck-typing)
# ═══════════════════════════════════════════════════════════════

# Action type → archetype mapping for explicit detection
_ACTION_TYPE_MAP: dict[str, OutputArchetype] = {
    # Terminal output
    "ssh.executeCommand": OutputArchetype.TERMINAL,
    "ssh.executeScript": OutputArchetype.TERMINAL,
    "script.bash": OutputArchetype.TERMINAL,
    "script.powershell": OutputArchetype.TERMINAL,
    "script.python": OutputArchetype.TERMINAL,
    "winrm.executeCommand": OutputArchetype.TERMINAL,
    "winrm.executeScript": OutputArchetype.TERMINAL,
    "os.restartService": OutputArchetype.TERMINAL,
    "os.stopService": OutputArchetype.TERMINAL,
    "os.startService": OutputArchetype.TERMINAL,
    "os.restartOS": OutputArchetype.TERMINAL,
    "kubernetes.restartDeployment": OutputArchetype.TERMINAL,
    "kubernetes.scaleDeployment": OutputArchetype.TERMINAL,
    "kubernetes.deletePod": OutputArchetype.TERMINAL,
    "kubernetes.rollbackDeployment": OutputArchetype.TERMINAL,
    # HTTP
    "http.request": OutputArchetype.HTTP_RESPONSE,
    # Kubernetes logs
    "kubernetes.getLogs": OutputArchetype.POD_LOGS,
    # Database
    "database.executeQuery": OutputArchetype.TABULAR,  # refined by output content
    # Jira
    "jira.createIssue": OutputArchetype.ISSUE_TRACKER,
    "jira.updateIssue": OutputArchetype.ISSUE_TRACKER,
    "jira.addComment": OutputArchetype.ISSUE_TRACKER,
    "jira.transitionIssue": OutputArchetype.ISSUE_TRACKER,
    # Messaging confirmations
    "slack.sendMessage": OutputArchetype.CONFIRMATION,
    "slack.updateMessage": OutputArchetype.CONFIRMATION,
    "slack.addReaction": OutputArchetype.CONFIRMATION,
    "teams.sendMessage": OutputArchetype.CONFIRMATION,
    "teams.sendAdaptiveCard": OutputArchetype.CONFIRMATION,
    "email.sendEmail": OutputArchetype.CONFIRMATION,
    "notification.sendNotification": OutputArchetype.CONFIRMATION,
}


def detect_archetype(
    output: dict | str | None,
    action_type: str = "",
    output_type_hint: str = "",
) -> OutputArchetype:
    """Detect the output archetype from data structure and/or action type.

    Priority:
    1. Explicit output_type_hint (user override)
    2. Duck-typing the output dict structure
    3. action_type mapping
    4. UNKNOWN fallback
    """
    # 1. Explicit hint
    if output_type_hint:
        try:
            return OutputArchetype(output_type_hint)
        except ValueError:
            logger.warning("Unknown output_type_hint: %s", output_type_hint)

    # Normalize output
    data = _normalize_output(output)
    if data is None:
        if action_type in _ACTION_TYPE_MAP:
            return _ACTION_TYPE_MAP[action_type]
        return OutputArchetype.UNKNOWN

    # 2. Duck-type the output structure
    if isinstance(data, dict):
        # Tabular data (SELECT)
        if "columns" in data and "rows" in data:
            return OutputArchetype.TABULAR

        # DB modify (INSERT/UPDATE/DELETE)
        if "rowsAffected" in data or ("queryType" in data and data.get("queryType") == "modify"):
            return OutputArchetype.DB_MODIFY

        # HTTP response
        if "statusCode" in data and "headers" in data:
            return OutputArchetype.HTTP_RESPONSE

        # Multi-pod logs
        if "pods" in data and isinstance(data.get("pods"), list):
            return OutputArchetype.POD_LOGS

        # Issue tracker
        if "issueKey" in data and ("url" in data or "issueId" in data):
            return OutputArchetype.ISSUE_TRACKER

        # Incident context
        if "incidentId" in data or ("severity" in data and "title" in data):
            return OutputArchetype.INCIDENT

        # Terminal output
        if "stdout" in data or "stderr" in data:
            return OutputArchetype.TERMINAL

        # Confirmation
        if "confirmed" in data or "delivered" in data or ("message" in data and "channel" in data):
            return OutputArchetype.CONFIRMATION

    # 3. Action type mapping
    if action_type in _ACTION_TYPE_MAP:
        archetype = _ACTION_TYPE_MAP[action_type]
        # Refine database archetype
        if archetype == OutputArchetype.TABULAR and isinstance(data, dict):
            if data.get("queryType") == "modify":
                return OutputArchetype.DB_MODIFY
        return archetype

    return OutputArchetype.UNKNOWN


def _normalize_output(output: dict | str | None) -> dict | None:
    """Parse output into a dict if it's a JSON string."""
    if output is None:
        return None
    if isinstance(output, dict):
        return output
    if isinstance(output, str):
        try:
            parsed = json.loads(output)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, ValueError):
            pass
    return None


# ═══════════════════════════════════════════════════════════════
# Format Dispatcher
# ═══════════════════════════════════════════════════════════════

def format_output(
    channel: str,
    output: dict | str | None,
    context: FormatContext | None = None,
    action_type: str = "",
    output_type_hint: str = "",
) -> dict:
    """Format workflow output for a specific notification channel.

    Args:
        channel: "slack", "teams", or "email"
        output: Raw output data from upstream workflow node
        context: Optional metadata for richer formatting
        action_type: Source action type for archetype detection
        output_type_hint: Explicit archetype override

    Returns:
        Channel-specific formatted payload:
        - slack: {"blocks": [...], "text": "..."}
        - teams: {"card": {...}} (Adaptive Card)
        - email: {"html": "...", "text": "..."}
    """
    archetype = detect_archetype(output, action_type, output_type_hint)
    data = _normalize_output(output) or {}
    ctx = context or FormatContext()

    logger.info(
        "Formatting %s output for %s (action: %s)",
        archetype.value, channel, action_type or "unknown",
    )

    if channel == "slack":
        from .slack_formatter import format_for_slack
        return format_for_slack(archetype, data, ctx)
    elif channel == "teams":
        from .teams_formatter import format_for_teams
        return format_for_teams(archetype, data, ctx)
    elif channel == "email":
        from .email_formatter import format_for_email
        return format_for_email(archetype, data, ctx)
    else:
        logger.warning("Unknown channel: %s, falling back to plain text", channel)
        return {"text": json.dumps(data, indent=2, default=str)}
