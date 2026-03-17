"""
Slack Block Kit formatter — renders workflow output as rich Slack messages.

Each output archetype gets a purpose-built Block Kit layout:
- Terminal: fenced code block with exit code badge
- HTTP: status code badge + pretty-printed JSON body
- Pod Logs: per-pod collapsible code sections
- Tabular: field grid with column headers
- Issue Tracker: linked issue card
- Incident: full incident notification card
"""

from __future__ import annotations

import json
from . import OutputArchetype, FormatContext

# ═══════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════

_MAX_CODE_BLOCK_LEN = 2900       # Slack block text limit ~3000
_MAX_TABLE_ROWS = 20
_MAX_POD_LOG_LINES = 30
_MAX_FIELD_VALUE_LEN = 200

_SEVERITY_EMOJI = {
    "critical": ":red_circle:",
    "high": ":large_orange_circle:",
    "medium": ":large_yellow_circle:",
    "low": ":large_blue_circle:",
}

_STATUS_EMOJI = {
    "triggered": ":rotating_light:",
    "acknowledged": ":eyes:",
    "resolved": ":white_check_mark:",
    "suppressed": ":no_bell:",
}

_HTTP_STATUS_EMOJI = {
    2: ":white_check_mark:",
    3: ":arrow_right:",
    4: ":warning:",
    5: ":x:",
}


# ═══════════════════════════════════════════════════════════════
# Public Entry Point
# ═══════════════════════════════════════════════════════════════

def format_for_slack(
    archetype: OutputArchetype,
    data: dict,
    ctx: FormatContext,
) -> dict:
    """Return {"blocks": [...], "text": "..."} for Slack chat.postMessage."""

    formatter = _FORMATTERS.get(archetype, _format_unknown)
    blocks = formatter(data, ctx)

    # Fallback text (shown in notifications / non-Block Kit clients)
    fallback = ctx.title or _fallback_text(archetype, data, ctx)

    return {"blocks": blocks, "text": fallback}


# ═══════════════════════════════════════════════════════════════
# Terminal Output (ssh, script, kubectl, os service)
# ═══════════════════════════════════════════════════════════════

def _format_terminal(data: dict, ctx: FormatContext) -> list[dict]:
    stdout = data.get("stdout", "")
    stderr = data.get("stderr", "")
    truncated = data.get("truncated", False)

    blocks: list[dict] = []

    # Header
    title = ctx.title or ctx.node_name or "Command Output"
    action_label = _action_label(ctx.source_action)
    header_text = f"*{title}*"
    if action_label:
        header_text += f"  ·  `{action_label}`"

    blocks.append(_section(header_text))
    blocks.append(_divider())

    # Stdout
    if stdout.strip():
        code = _truncate(stdout.strip(), _MAX_CODE_BLOCK_LEN)
        blocks.append(_section(f"```\n{code}\n```"))
    elif not stderr.strip():
        blocks.append(_section("_No output_"))

    # Stderr (only if non-empty)
    if stderr.strip():
        err_code = _truncate(stderr.strip(), _MAX_CODE_BLOCK_LEN)
        blocks.append(_section(f":warning: *stderr*\n```\n{err_code}\n```"))

    # Footer context
    footer_parts = []
    if ctx.source_action:
        footer_parts.append(f"Action: `{ctx.source_action}`")
    if truncated:
        footer_parts.append(":scissors: Output was truncated")
    if ctx.timestamp:
        footer_parts.append(ctx.timestamp)
    if footer_parts:
        blocks.append(_context(footer_parts))

    return blocks


# ═══════════════════════════════════════════════════════════════
# HTTP Response
# ═══════════════════════════════════════════════════════════════

def _format_http(data: dict, ctx: FormatContext) -> list[dict]:
    status_code = data.get("statusCode", 0)
    body = data.get("body", "")
    body_parsed = data.get("bodyParsed")
    headers = data.get("headers", {})
    truncated = data.get("truncated", False)

    blocks: list[dict] = []

    # Status badge
    category = status_code // 100
    emoji = _HTTP_STATUS_EMOJI.get(category, ":grey_question:")
    status_text = f"{emoji}  *HTTP {status_code}*"

    title = ctx.title or ctx.node_name or "HTTP Response"
    blocks.append(_section(f"*{title}*\n{status_text}"))
    blocks.append(_divider())

    # Response body
    if body_parsed and isinstance(body_parsed, (dict, list)):
        pretty = json.dumps(body_parsed, indent=2, default=str)
        code = _truncate(pretty, _MAX_CODE_BLOCK_LEN)
        blocks.append(_section(f"```\n{code}\n```"))
    elif body.strip():
        code = _truncate(body.strip(), _MAX_CODE_BLOCK_LEN)
        blocks.append(_section(f"```\n{code}\n```"))

    # Key headers as fields
    interesting_headers = ["content-type", "x-request-id", "x-correlation-id", "location"]
    fields = []
    for h in interesting_headers:
        val = headers.get(h) or headers.get(h.title()) or headers.get(h.replace("-", ""))
        if val:
            fields.append(f"*{h}*\n`{_truncate(val, 80)}`")
    if fields:
        blocks.append(_fields(fields[:4]))

    # Footer
    footer_parts = []
    if ctx.source_action:
        footer_parts.append(f"Action: `{ctx.source_action}`")
    if truncated:
        footer_parts.append(":scissors: Body was truncated")
    if footer_parts:
        blocks.append(_context(footer_parts))

    return blocks


# ═══════════════════════════════════════════════════════════════
# Pod Logs (multi-pod kubernetes.getLogs)
# ═══════════════════════════════════════════════════════════════

def _format_pod_logs(data: dict, ctx: FormatContext) -> list[dict]:
    pods = data.get("pods", [])
    matched = data.get("matchedCount", len(pods))
    mode = data.get("mode", "")
    partial = data.get("partialFailure", False)

    # If single-pod format (stdout/stderr), delegate to terminal
    if "stdout" in data and "pods" not in data:
        return _format_terminal(data, ctx)

    blocks: list[dict] = []

    # Header
    title = ctx.title or ctx.node_name or "Pod Logs"
    emoji = ":package:" if not partial else ":warning:"
    blocks.append(_section(f"{emoji}  *{title}*  ·  {matched} pod(s) matched"))
    blocks.append(_divider())

    # Per-pod sections
    for pod in pods[:8]:  # Max 8 pods to stay under Slack limits
        name = pod.get("name", "unknown")
        logs = pod.get("logs", "")
        error = pod.get("error")

        if error:
            blocks.append(_section(f":x:  `{name}`\n_{error}_"))
        elif logs.strip():
            # Last N lines
            lines = logs.strip().splitlines()
            tail = "\n".join(lines[-_MAX_POD_LOG_LINES:])
            tail = _truncate(tail, _MAX_CODE_BLOCK_LEN)
            blocks.append(_section(f":package:  `{name}`\n```\n{tail}\n```"))
        else:
            blocks.append(_section(f":package:  `{name}`\n_No logs available_"))

    # Footer
    footer_parts = []
    if mode:
        footer_parts.append(f"Mode: {mode}")
    if len(pods) > 8:
        footer_parts.append(f"+{len(pods) - 8} more pod(s) not shown")
    if ctx.timestamp:
        footer_parts.append(ctx.timestamp)
    if footer_parts:
        blocks.append(_context(footer_parts))

    return blocks


# ═══════════════════════════════════════════════════════════════
# Tabular Data (database SELECT)
# ═══════════════════════════════════════════════════════════════

def _format_tabular(data: dict, ctx: FormatContext) -> list[dict]:
    columns = data.get("columns", [])
    rows = data.get("rows", [])
    row_count = data.get("rowCount", len(rows))
    truncated_flag = data.get("truncated", False)
    db_type = data.get("databaseType", "")
    dbname = data.get("dbname", "")

    blocks: list[dict] = []

    # Header
    title = ctx.title or ctx.node_name or "Query Results"
    db_badge = f"`{db_type}`" if db_type else ""
    db_name_badge = f"`{dbname}`" if dbname else ""
    header = f":mag:  *{title}*  ·  {row_count} row(s)"
    if db_badge:
        header += f"  ·  {db_badge}"
    if db_name_badge:
        header += f" {db_name_badge}"

    blocks.append(_section(header))
    blocks.append(_divider())

    if not rows:
        blocks.append(_section("_No rows returned_"))
        return blocks

    # Build markdown table (Slack renders monospace well in code blocks)
    display_rows = rows[:_MAX_TABLE_ROWS]
    display_cols = columns[:6]  # Max 6 columns for readability

    # Calculate column widths
    col_widths = []
    for i, col in enumerate(display_cols):
        max_w = len(str(col))
        for row in display_rows[:10]:
            if i < len(row):
                max_w = max(max_w, min(len(str(row[i])), 25))
        col_widths.append(min(max_w, 25))

    # Header row
    header_row = " | ".join(
        str(col).ljust(col_widths[i]) for i, col in enumerate(display_cols)
    )
    separator = "-+-".join("-" * w for w in col_widths)

    # Data rows
    data_rows = []
    for row in display_rows:
        cells = []
        for i, col in enumerate(display_cols):
            val = str(row[i]) if i < len(row) else ""
            cells.append(val[:25].ljust(col_widths[i]))
        data_rows.append(" | ".join(cells))

    table = f"{header_row}\n{separator}\n" + "\n".join(data_rows)
    table = _truncate(table, _MAX_CODE_BLOCK_LEN)
    blocks.append(_section(f"```\n{table}\n```"))

    # Footer
    footer_parts = []
    if len(rows) > _MAX_TABLE_ROWS:
        footer_parts.append(f"+{len(rows) - _MAX_TABLE_ROWS} more row(s)")
    if len(columns) > 6:
        footer_parts.append(f"+{len(columns) - 6} more column(s)")
    if truncated_flag:
        footer_parts.append(":scissors: Results were truncated")
    if footer_parts:
        blocks.append(_context(footer_parts))

    return blocks


# ═══════════════════════════════════════════════════════════════
# DB Modify (INSERT/UPDATE/DELETE)
# ═══════════════════════════════════════════════════════════════

def _format_db_modify(data: dict, ctx: FormatContext) -> list[dict]:
    rows_affected = data.get("rowsAffected", 0)
    status_msg = data.get("statusMessage", "")
    db_type = data.get("databaseType", "")
    dbname = data.get("dbname", "")

    blocks: list[dict] = []

    title = ctx.title or ctx.node_name or "Database Operation"
    blocks.append(_section(f":pencil2:  *{title}*"))
    blocks.append(_divider())

    fields = []
    if status_msg:
        fields.append(f"*Result*\n`{status_msg}`")
    if rows_affected is not None:
        fields.append(f"*Rows Affected*\n{rows_affected}")
    if db_type:
        fields.append(f"*Database*\n`{db_type}`")
    if dbname:
        fields.append(f"*Schema*\n`{dbname}`")

    if fields:
        blocks.append(_fields(fields))

    return blocks


# ═══════════════════════════════════════════════════════════════
# Issue Tracker (Jira)
# ═══════════════════════════════════════════════════════════════

def _format_issue_tracker(data: dict, ctx: FormatContext) -> list[dict]:
    issue_key = data.get("issueKey", "")
    issue_id = data.get("issueId", "")
    url = data.get("url", "")
    comment_id = data.get("commentId", "")
    message = data.get("message", "")

    blocks: list[dict] = []

    title = ctx.title or ctx.node_name or "Jira Update"
    blocks.append(_section(f":jira:  *{title}*"))
    blocks.append(_divider())

    # Issue link
    if issue_key and url:
        blocks.append(_section(f":ticket:  *<{url}|{issue_key}>*\n{message}"))
    elif issue_key:
        blocks.append(_section(f":ticket:  *{issue_key}*\n{message}"))
    else:
        blocks.append(_section(message or "_Operation completed_"))

    # Metadata fields
    fields = []
    if issue_id:
        fields.append(f"*Issue ID*\n`{issue_id}`")
    if comment_id:
        fields.append(f"*Comment ID*\n`{comment_id}`")
    if fields:
        blocks.append(_fields(fields))

    return blocks


# ═══════════════════════════════════════════════════════════════
# Incident Context
# ═══════════════════════════════════════════════════════════════

def _format_incident(data: dict, ctx: FormatContext) -> list[dict]:
    title = data.get("title", ctx.title or "Incident")
    severity = data.get("severity", ctx.severity or "medium")
    status = data.get("status", ctx.status or "triggered")
    description = data.get("description", ctx.description or "")
    service = data.get("service", "")
    host = data.get("host", "")
    source = data.get("source", "")
    team_id = data.get("teamId", "")
    created_at = data.get("createdAt", ctx.timestamp or "")
    tags = data.get("tags", [])
    incident_id = data.get("id", "")

    sev_emoji = _SEVERITY_EMOJI.get(severity, ":white_circle:")
    status_emoji = _STATUS_EMOJI.get(status, ":grey_question:")

    blocks: list[dict] = []

    # Header with severity
    header = f"{sev_emoji}  *{severity.upper()}* Incident  {status_emoji} {status.capitalize()}"
    blocks.append(_section(header))
    blocks.append(_section(f"*{title}*"))
    blocks.append(_divider())

    # Description
    if description:
        desc = _truncate(description, 500)
        blocks.append(_section(desc))

    # Metadata fields
    fields = []
    if service:
        fields.append(f"*Service*\n`{service}`")
    if host:
        fields.append(f"*Host*\n`{host}`")
    if source:
        fields.append(f"*Source*\n`{source}`")
    if team_id:
        fields.append(f"*Team*\n`{team_id}`")
    if fields:
        blocks.append(_fields(fields))

    # Tags
    if tags:
        tag_str = " ".join(f"`{t}`" for t in tags[:8])
        blocks.append(_section(f":label: {tag_str}"))

    # Action button
    if ctx.incident_url:
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": ":link: View Incident", "emoji": True},
                    "url": ctx.incident_url,
                    "style": "primary",
                },
            ],
        })

    # Footer
    footer_parts = []
    if incident_id:
        footer_parts.append(f"ID: {incident_id}")
    if created_at:
        footer_parts.append(created_at)
    if ctx.workflow_name:
        footer_parts.append(f"Workflow: {ctx.workflow_name}")
    if footer_parts:
        blocks.append(_context(footer_parts))

    return blocks


# ═══════════════════════════════════════════════════════════════
# Confirmation (messaging send results)
# ═══════════════════════════════════════════════════════════════

def _format_confirmation(data: dict, ctx: FormatContext) -> list[dict]:
    message = data.get("message", "Operation completed")
    blocks: list[dict] = []

    title = ctx.title or ctx.node_name or "Notification Sent"
    blocks.append(_section(f":white_check_mark:  *{title}*\n{message}"))

    return blocks


# ═══════════════════════════════════════════════════════════════
# Unknown / Fallback
# ═══════════════════════════════════════════════════════════════

def _format_unknown(data: dict, ctx: FormatContext) -> list[dict]:
    blocks: list[dict] = []

    title = ctx.title or ctx.node_name or "Workflow Output"
    blocks.append(_section(f"*{title}*"))
    blocks.append(_divider())

    if data:
        pretty = json.dumps(data, indent=2, default=str)
        code = _truncate(pretty, _MAX_CODE_BLOCK_LEN)
        blocks.append(_section(f"```\n{code}\n```"))
    else:
        blocks.append(_section("_No output data_"))

    return blocks


# ═══════════════════════════════════════════════════════════════
# Formatter Registry
# ═══════════════════════════════════════════════════════════════

_FORMATTERS: dict[OutputArchetype, callable] = {
    OutputArchetype.TERMINAL: _format_terminal,
    OutputArchetype.HTTP_RESPONSE: _format_http,
    OutputArchetype.POD_LOGS: _format_pod_logs,
    OutputArchetype.TABULAR: _format_tabular,
    OutputArchetype.DB_MODIFY: _format_db_modify,
    OutputArchetype.ISSUE_TRACKER: _format_issue_tracker,
    OutputArchetype.INCIDENT: _format_incident,
    OutputArchetype.CONFIRMATION: _format_confirmation,
    OutputArchetype.UNKNOWN: _format_unknown,
}


# ═══════════════════════════════════════════════════════════════
# Block Kit Helpers
# ═══════════════════════════════════════════════════════════════

def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _divider() -> dict:
    return {"type": "divider"}


def _context(elements: list[str]) -> dict:
    return {
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": e} for e in elements],
    }


def _fields(field_texts: list[str]) -> dict:
    return {
        "type": "section",
        "fields": [{"type": "mrkdwn", "text": t} for t in field_texts[:10]],
    }


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 20] + "\n… (truncated)"


def _action_label(action_type: str) -> str:
    labels = {
        "ssh.executeCommand": "SSH",
        "ssh.executeScript": "SSH Script",
        "script.bash": "Bash",
        "script.powershell": "PowerShell",
        "script.python": "Python",
        "winrm.executeCommand": "WinRM",
        "winrm.executeScript": "WinRM Script",
        "os.restartService": "Service Restart",
        "os.stopService": "Service Stop",
        "os.startService": "Service Start",
        "kubernetes.restartDeployment": "K8s Restart",
        "kubernetes.scaleDeployment": "K8s Scale",
        "kubernetes.deletePod": "K8s Delete Pod",
        "kubernetes.rollbackDeployment": "K8s Rollback",
        "kubernetes.getLogs": "K8s Logs",
        "http.request": "HTTP",
        "database.executeQuery": "SQL Query",
    }
    return labels.get(action_type, "")


def _fallback_text(archetype: OutputArchetype, data: dict, ctx: FormatContext) -> str:
    """Plain text fallback for notifications."""
    prefix = ctx.title or ctx.node_name or "EasyAlert"
    if archetype == OutputArchetype.TERMINAL:
        return f"{prefix}: Command completed"
    if archetype == OutputArchetype.HTTP_RESPONSE:
        return f"{prefix}: HTTP {data.get('statusCode', '?')}"
    if archetype == OutputArchetype.TABULAR:
        return f"{prefix}: {data.get('rowCount', '?')} rows returned"
    if archetype == OutputArchetype.INCIDENT:
        return f"{prefix}: {data.get('title', 'Incident')}"
    return f"{prefix}: Workflow output"
