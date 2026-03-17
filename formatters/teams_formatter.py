"""
Microsoft Teams Adaptive Card formatter — renders workflow output as rich cards.

Each output archetype gets a purpose-built Adaptive Card layout:
- Terminal: monospace code block with status container
- HTTP: status badge ColumnSet + pretty JSON
- Pod Logs: per-pod containers with code blocks
- Tabular: ColumnSet-based table
- Issue Tracker: FactSet + Action.OpenUrl
- Incident: full severity-themed card with actions
"""

from __future__ import annotations

import json
from . import OutputArchetype, FormatContext

# ═══════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════

_MAX_CODE_BLOCK_LEN = 4000
_MAX_TABLE_ROWS = 20
_MAX_POD_LOG_LINES = 30

_SEVERITY_COLOR = {
    "critical": "attention",   # Red
    "high": "warning",         # Orange/Yellow
    "medium": "warning",
    "low": "accent",           # Blue
}

_STATUS_ICON = {
    "triggered": "🔴",
    "acknowledged": "🟡",
    "resolved": "🟢",
    "suppressed": "⚪",
}

_HTTP_COLOR = {
    2: "good",      # Green
    3: "accent",    # Blue
    4: "warning",   # Yellow
    5: "attention",  # Red
}


# ═══════════════════════════════════════════════════════════════
# Public Entry Point
# ═══════════════════════════════════════════════════════════════

def format_for_teams(
    archetype: OutputArchetype,
    data: dict,
    ctx: FormatContext,
) -> dict:
    """Return {"card": {...}} Adaptive Card payload for Teams webhook."""

    formatter = _FORMATTERS.get(archetype, _format_unknown)
    body = formatter(data, ctx)

    card = {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.5",
        "body": body,
    }

    # Add actions if present
    actions = _build_actions(data, ctx)
    if actions:
        card["actions"] = actions

    return {"card": card}


# ═══════════════════════════════════════════════════════════════
# Terminal Output
# ═══════════════════════════════════════════════════════════════

def _format_terminal(data: dict, ctx: FormatContext) -> list[dict]:
    stdout = data.get("stdout", "")
    stderr = data.get("stderr", "")
    truncated = data.get("truncated", False)

    elements: list[dict] = []

    # Header
    title = ctx.title or ctx.node_name or "Command Output"
    action_label = _action_label(ctx.source_action)
    elements.append(_header_row(title, action_label))
    elements.append(_separator())

    # Stdout
    if stdout.strip():
        code = _truncate(stdout.strip(), _MAX_CODE_BLOCK_LEN)
        elements.append(_code_block(code))
    elif not stderr.strip():
        elements.append(_text_block("_No output_", is_subtle=True))

    # Stderr
    if stderr.strip():
        elements.append(_text_block("⚠️ **stderr**"))
        err_code = _truncate(stderr.strip(), _MAX_CODE_BLOCK_LEN)
        elements.append(_code_block(err_code))

    # Footer
    footer = _build_footer(ctx, truncated)
    if footer:
        elements.append(_separator())
        elements.append(footer)

    return elements


# ═══════════════════════════════════════════════════════════════
# HTTP Response
# ═══════════════════════════════════════════════════════════════

def _format_http(data: dict, ctx: FormatContext) -> list[dict]:
    status_code = data.get("statusCode", 0)
    body = data.get("body", "")
    body_parsed = data.get("bodyParsed")
    headers = data.get("headers", {})
    truncated = data.get("truncated", False)

    elements: list[dict] = []
    category = status_code // 100
    color = _HTTP_COLOR.get(category, "default")

    # Header with status badge
    title = ctx.title or ctx.node_name or "HTTP Response"
    elements.append({
        "type": "ColumnSet",
        "columns": [
            {
                "type": "Column",
                "width": "stretch",
                "items": [_text_block(f"**{title}**")],
            },
            {
                "type": "Column",
                "width": "auto",
                "items": [
                    {
                        "type": "TextBlock",
                        "text": f"HTTP {status_code}",
                        "weight": "Bolder",
                        "color": color,
                        "horizontalAlignment": "Right",
                    }
                ],
            },
        ],
    })
    elements.append(_separator())

    # Response body
    if body_parsed and isinstance(body_parsed, (dict, list)):
        pretty = json.dumps(body_parsed, indent=2, default=str)
        elements.append(_code_block(_truncate(pretty, _MAX_CODE_BLOCK_LEN)))
    elif body.strip():
        elements.append(_code_block(_truncate(body.strip(), _MAX_CODE_BLOCK_LEN)))

    # Key headers
    facts = []
    for h in ["content-type", "x-request-id", "location"]:
        val = headers.get(h) or headers.get(h.title())
        if val:
            facts.append({"title": h, "value": _truncate(val, 80)})
    if facts:
        elements.append({"type": "FactSet", "facts": facts})

    footer = _build_footer(ctx, truncated)
    if footer:
        elements.append(_separator())
        elements.append(footer)

    return elements


# ═══════════════════════════════════════════════════════════════
# Pod Logs (multi-pod)
# ═══════════════════════════════════════════════════════════════

def _format_pod_logs(data: dict, ctx: FormatContext) -> list[dict]:
    pods = data.get("pods", [])
    matched = data.get("matchedCount", len(pods))
    partial = data.get("partialFailure", False)

    if "stdout" in data and "pods" not in data:
        return _format_terminal(data, ctx)

    elements: list[dict] = []

    title = ctx.title or ctx.node_name or "Pod Logs"
    icon = "⚠️" if partial else "📦"
    elements.append(_text_block(f"{icon} **{title}** · {matched} pod(s) matched"))
    elements.append(_separator())

    for pod in pods[:6]:
        name = pod.get("name", "unknown")
        logs = pod.get("logs", "")
        error = pod.get("error")

        if error:
            elements.append(_text_block(f"❌ `{name}` — {error}"))
        elif logs.strip():
            lines = logs.strip().splitlines()
            tail = "\n".join(lines[-_MAX_POD_LOG_LINES:])
            elements.append(_text_block(f"📦 `{name}`"))
            elements.append(_code_block(_truncate(tail, _MAX_CODE_BLOCK_LEN)))
        else:
            elements.append(_text_block(f"📦 `{name}` — _No logs_"))

    if len(pods) > 6:
        elements.append(_text_block(
            f"_+{len(pods) - 6} more pod(s) not shown_",
            is_subtle=True,
        ))

    return elements


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

    elements: list[dict] = []

    title = ctx.title or ctx.node_name or "Query Results"
    subtitle = f"{row_count} row(s)"
    if db_type:
        subtitle += f" · {db_type}"
    if dbname:
        subtitle += f" · {dbname}"
    elements.append(_text_block(f"🔍 **{title}** — {subtitle}"))
    elements.append(_separator())

    if not rows:
        elements.append(_text_block("_No rows returned_", is_subtle=True))
        return elements

    display_cols = columns[:5]
    display_rows = rows[:_MAX_TABLE_ROWS]

    # Table header
    header_cols = [
        {
            "type": "Column",
            "width": "stretch",
            "items": [
                {
                    "type": "TextBlock",
                    "text": str(col),
                    "weight": "Bolder",
                    "size": "Small",
                }
            ],
        }
        for col in display_cols
    ]
    elements.append({"type": "ColumnSet", "columns": header_cols})

    # Table rows
    for row in display_rows:
        row_cols = [
            {
                "type": "Column",
                "width": "stretch",
                "items": [
                    {
                        "type": "TextBlock",
                        "text": str(row[i]) if i < len(row) else "",
                        "size": "Small",
                        "wrap": True,
                    }
                ],
            }
            for i in range(len(display_cols))
        ]
        elements.append({"type": "ColumnSet", "columns": row_cols, "separator": True})

    if len(rows) > _MAX_TABLE_ROWS or truncated_flag:
        remaining = row_count - _MAX_TABLE_ROWS if row_count > _MAX_TABLE_ROWS else 0
        msg = f"_Showing {min(len(rows), _MAX_TABLE_ROWS)} of {row_count} rows_"
        elements.append(_text_block(msg, is_subtle=True))

    return elements


# ═══════════════════════════════════════════════════════════════
# DB Modify
# ═══════════════════════════════════════════════════════════════

def _format_db_modify(data: dict, ctx: FormatContext) -> list[dict]:
    rows_affected = data.get("rowsAffected", 0)
    status_msg = data.get("statusMessage", "")
    db_type = data.get("databaseType", "")
    dbname = data.get("dbname", "")

    elements: list[dict] = []

    title = ctx.title or ctx.node_name or "Database Operation"
    elements.append(_text_block(f"✏️ **{title}**"))
    elements.append(_separator())

    facts = []
    if status_msg:
        facts.append({"title": "Result", "value": status_msg})
    if rows_affected is not None:
        facts.append({"title": "Rows Affected", "value": str(rows_affected)})
    if db_type:
        facts.append({"title": "Database", "value": db_type})
    if dbname:
        facts.append({"title": "Schema", "value": dbname})

    if facts:
        elements.append({"type": "FactSet", "facts": facts})

    return elements


# ═══════════════════════════════════════════════════════════════
# Issue Tracker (Jira)
# ═══════════════════════════════════════════════════════════════

def _format_issue_tracker(data: dict, ctx: FormatContext) -> list[dict]:
    issue_key = data.get("issueKey", "")
    url = data.get("url", "")
    message = data.get("message", "")

    elements: list[dict] = []

    title = ctx.title or ctx.node_name or "Jira Update"
    elements.append(_text_block(f"🎫 **{title}**"))
    elements.append(_separator())

    if issue_key:
        key_text = f"[{issue_key}]({url})" if url else f"**{issue_key}**"
        elements.append(_text_block(f"🔗 {key_text}"))

    if message:
        elements.append(_text_block(message, is_subtle=True))

    return elements


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

    sev_color = _SEVERITY_COLOR.get(severity, "default")
    status_icon = _STATUS_ICON.get(status, "⚪")

    elements: list[dict] = []

    # Header row: severity + status
    elements.append({
        "type": "ColumnSet",
        "columns": [
            {
                "type": "Column",
                "width": "stretch",
                "items": [
                    {
                        "type": "TextBlock",
                        "text": f"{severity.upper()} INCIDENT",
                        "weight": "Bolder",
                        "size": "Medium",
                        "color": sev_color,
                    },
                ],
            },
            {
                "type": "Column",
                "width": "auto",
                "items": [
                    {
                        "type": "TextBlock",
                        "text": f"{status_icon} {status.capitalize()}",
                        "horizontalAlignment": "Right",
                    }
                ],
            },
        ],
    })

    # Title
    elements.append({
        "type": "TextBlock",
        "text": title,
        "weight": "Bolder",
        "size": "Large",
        "wrap": True,
    })
    elements.append(_separator())

    # Description
    if description:
        elements.append(_text_block(_truncate(description, 500), wrap=True))

    # Metadata
    facts = []
    if service:
        facts.append({"title": "Service", "value": service})
    if host:
        facts.append({"title": "Host", "value": host})
    if source:
        facts.append({"title": "Source", "value": source})
    if team_id:
        facts.append({"title": "Team", "value": team_id})
    if created_at:
        facts.append({"title": "Created", "value": created_at})
    if facts:
        elements.append({"type": "FactSet", "facts": facts})

    # Tags
    if tags:
        elements.append(_text_block("🏷️ " + ", ".join(f"`{t}`" for t in tags[:8])))

    return elements


# ═══════════════════════════════════════════════════════════════
# Confirmation
# ═══════════════════════════════════════════════════════════════

def _format_confirmation(data: dict, ctx: FormatContext) -> list[dict]:
    message = data.get("message", "Operation completed")
    title = ctx.title or ctx.node_name or "Notification Sent"
    return [_text_block(f"✅ **{title}**\n\n{message}")]


# ═══════════════════════════════════════════════════════════════
# Unknown / Fallback
# ═══════════════════════════════════════════════════════════════

def _format_unknown(data: dict, ctx: FormatContext) -> list[dict]:
    elements: list[dict] = []
    title = ctx.title or ctx.node_name or "Workflow Output"
    elements.append(_text_block(f"**{title}**"))
    elements.append(_separator())

    if data:
        pretty = json.dumps(data, indent=2, default=str)
        elements.append(_code_block(_truncate(pretty, _MAX_CODE_BLOCK_LEN)))
    else:
        elements.append(_text_block("_No output data_", is_subtle=True))

    return elements


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
# Adaptive Card Helpers
# ═══════════════════════════════════════════════════════════════

def _text_block(
    text: str,
    is_subtle: bool = False,
    wrap: bool = True,
    size: str = "Default",
) -> dict:
    block: dict = {
        "type": "TextBlock",
        "text": text,
        "wrap": wrap,
        "size": size,
    }
    if is_subtle:
        block["isSubtle"] = True
    return block


def _code_block(code: str) -> dict:
    return {
        "type": "TextBlock",
        "text": code,
        "fontType": "Monospace",
        "size": "Small",
        "wrap": True,
    }


def _separator() -> dict:
    return {"type": "TextBlock", "text": " ", "spacing": "None", "separator": True}


def _header_row(title: str, badge: str = "") -> dict:
    cols = [
        {
            "type": "Column",
            "width": "stretch",
            "items": [
                {
                    "type": "TextBlock",
                    "text": f"**{title}**",
                    "weight": "Bolder",
                    "size": "Medium",
                }
            ],
        },
    ]
    if badge:
        cols.append({
            "type": "Column",
            "width": "auto",
            "items": [
                {
                    "type": "TextBlock",
                    "text": f"`{badge}`",
                    "horizontalAlignment": "Right",
                    "isSubtle": True,
                }
            ],
        })
    return {"type": "ColumnSet", "columns": cols}


def _build_footer(ctx: FormatContext, truncated: bool = False) -> dict | None:
    parts = []
    if ctx.source_action:
        parts.append(f"Action: {ctx.source_action}")
    if truncated:
        parts.append("✂️ Output truncated")
    if ctx.timestamp:
        parts.append(ctx.timestamp)
    if not parts:
        return None
    return _text_block(" · ".join(parts), is_subtle=True, size="Small")


def _build_actions(data: dict, ctx: FormatContext) -> list[dict]:
    actions = []
    url = data.get("url") or ctx.incident_url
    if url:
        actions.append({
            "type": "Action.OpenUrl",
            "title": "View Details",
            "url": url,
        })
    return actions


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
        "kubernetes.restartDeployment": "K8s Restart",
        "kubernetes.scaleDeployment": "K8s Scale",
        "kubernetes.getLogs": "K8s Logs",
        "http.request": "HTTP",
        "database.executeQuery": "SQL Query",
    }
    return labels.get(action_type, "")
