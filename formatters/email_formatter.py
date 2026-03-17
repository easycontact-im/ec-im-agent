"""
Email HTML formatter — renders workflow output as branded HTML emails.

Follows the EasyAlert email design system (from email_service.py):
- Indigo gradient logo header
- White card body with rounded corners and shadow
- Table-based layout for email client compatibility
- Severity-colored badges
- CTA buttons with gradient
- Max-width 600px container
"""

from __future__ import annotations

import json
import html as html_lib
from . import OutputArchetype, FormatContext

# ═══════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════

_MAX_CODE_LINES = 60
_MAX_TABLE_ROWS = 30
_MAX_POD_LOG_LINES = 40

_SEVERITY_COLORS = {
    "critical": {"bg": "#fef2f2", "border": "#fca5a5", "text": "#dc2626", "badge": "#ef4444"},
    "high": {"bg": "#fff7ed", "border": "#fdba74", "text": "#ea580c", "badge": "#f97316"},
    "medium": {"bg": "#fffbeb", "border": "#fcd34d", "text": "#d97706", "badge": "#f59e0b"},
    "low": {"bg": "#eff6ff", "border": "#93c5fd", "text": "#2563eb", "badge": "#3b82f6"},
}

_STATUS_COLORS = {
    "triggered": "#ef4444",
    "acknowledged": "#f59e0b",
    "resolved": "#10b981",
    "suppressed": "#6b7280",
}

_HTTP_STATUS_COLORS = {
    2: "#10b981",
    3: "#3b82f6",
    4: "#f59e0b",
    5: "#ef4444",
}


# ═══════════════════════════════════════════════════════════════
# Public Entry Point
# ═══════════════════════════════════════════════════════════════

def format_for_email(
    archetype: OutputArchetype,
    data: dict,
    ctx: FormatContext,
) -> dict:
    """Return {"html": "...", "text": "..."} for email sending."""

    formatter = _FORMATTERS.get(archetype, _format_unknown)
    body_html = formatter(data, ctx)

    title = ctx.title or ctx.node_name or "EasyAlert Notification"
    full_html = _wrap_email(title, body_html, ctx)
    plain_text = _generate_plain_text(archetype, data, ctx)

    return {"html": full_html, "text": plain_text}


# ═══════════════════════════════════════════════════════════════
# Terminal Output
# ═══════════════════════════════════════════════════════════════

def _format_terminal(data: dict, ctx: FormatContext) -> str:
    stdout = data.get("stdout", "")
    stderr = data.get("stderr", "")
    truncated = data.get("truncated", False)

    title = ctx.title or ctx.node_name or "Command Output"
    action = _action_badge(ctx.source_action)

    parts = [f'<h2 style="{_H2_STYLE}">{_esc(title)}{action}</h2>']

    if stdout.strip():
        lines = stdout.strip().splitlines()[:_MAX_CODE_LINES]
        code = _esc("\n".join(lines))
        parts.append(_code_block_html(code))
    elif not stderr.strip():
        parts.append(f'<p style="color: #71717a; font-style: italic;">No output</p>')

    if stderr.strip():
        lines = stderr.strip().splitlines()[:_MAX_CODE_LINES]
        code = _esc("\n".join(lines))
        parts.append(f'<p style="color: #ef4444; font-weight: 600; margin-top: 16px;">⚠ stderr</p>')
        parts.append(_code_block_html(code, border_color="#fca5a5"))

    if truncated:
        parts.append(_info_badge("Output was truncated"))

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════
# HTTP Response
# ═══════════════════════════════════════════════════════════════

def _format_http(data: dict, ctx: FormatContext) -> str:
    status_code = data.get("statusCode", 0)
    body = data.get("body", "")
    body_parsed = data.get("bodyParsed")
    headers = data.get("headers", {})
    truncated = data.get("truncated", False)

    category = status_code // 100
    color = _HTTP_STATUS_COLORS.get(category, "#6b7280")

    title = ctx.title or ctx.node_name or "HTTP Response"
    parts = [f'<h2 style="{_H2_STYLE}">{_esc(title)}</h2>']

    # Status badge
    parts.append(
        f'<div style="margin-bottom: 16px;">'
        f'<span style="display: inline-block; padding: 6px 16px; border-radius: 20px; '
        f'background-color: {color}; color: white; font-weight: 700; font-size: 16px; '
        f'font-family: \'SFMono-Regular\', Consolas, monospace;">'
        f'HTTP {status_code}</span></div>'
    )

    # Body
    if body_parsed and isinstance(body_parsed, (dict, list)):
        pretty = json.dumps(body_parsed, indent=2, default=str)
        lines = pretty.splitlines()[:_MAX_CODE_LINES]
        parts.append(_code_block_html(_esc("\n".join(lines))))
    elif body.strip():
        lines = body.strip().splitlines()[:_MAX_CODE_LINES]
        parts.append(_code_block_html(_esc("\n".join(lines))))

    # Headers table
    interesting = ["content-type", "x-request-id", "x-correlation-id", "location"]
    header_rows = []
    for h in interesting:
        val = headers.get(h) or headers.get(h.title())
        if val:
            header_rows.append((h, val))

    if header_rows:
        parts.append(_metadata_table(header_rows))

    if truncated:
        parts.append(_info_badge("Response body was truncated"))

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════
# Pod Logs
# ═══════════════════════════════════════════════════════════════

def _format_pod_logs(data: dict, ctx: FormatContext) -> str:
    pods = data.get("pods", [])
    matched = data.get("matchedCount", len(pods))
    partial = data.get("partialFailure", False)

    if "stdout" in data and "pods" not in data:
        return _format_terminal(data, ctx)

    title = ctx.title or ctx.node_name or "Pod Logs"
    icon = "⚠️" if partial else "📦"

    parts = [f'<h2 style="{_H2_STYLE}">{icon} {_esc(title)} — {matched} pod(s)</h2>']

    for pod in pods[:8]:
        name = pod.get("name", "unknown")
        logs = pod.get("logs", "")
        error = pod.get("error")

        parts.append(
            f'<div style="margin-top: 16px; padding: 4px 0;">'
            f'<span style="display: inline-block; padding: 3px 10px; border-radius: 4px; '
            f'background-color: #f1f5f9; font-family: monospace; font-size: 13px; '
            f'font-weight: 600; color: #334155;">{_esc(name)}</span></div>'
        )

        if error:
            parts.append(
                f'<p style="color: #ef4444; font-size: 13px; margin: 4px 0;">❌ {_esc(error)}</p>'
            )
        elif logs.strip():
            lines = logs.strip().splitlines()[-_MAX_POD_LOG_LINES:]
            parts.append(_code_block_html(_esc("\n".join(lines))))
        else:
            parts.append(f'<p style="color: #71717a; font-style: italic; font-size: 13px;">No logs</p>')

    if len(pods) > 8:
        parts.append(_info_badge(f"+{len(pods) - 8} more pod(s) not shown"))

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════
# Tabular Data
# ═══════════════════════════════════════════════════════════════

def _format_tabular(data: dict, ctx: FormatContext) -> str:
    columns = data.get("columns", [])
    rows = data.get("rows", [])
    row_count = data.get("rowCount", len(rows))
    truncated_flag = data.get("truncated", False)
    db_type = data.get("databaseType", "")
    dbname = data.get("dbname", "")

    title = ctx.title or ctx.node_name or "Query Results"
    subtitle = f"{row_count} row(s)"
    if db_type:
        subtitle += f" · {db_type}"
    if dbname:
        subtitle += f" · {dbname}"

    parts = [f'<h2 style="{_H2_STYLE}">🔍 {_esc(title)}</h2>']
    parts.append(f'<p style="color: #71717a; font-size: 13px; margin: 0 0 12px;">{_esc(subtitle)}</p>')

    if not rows:
        parts.append(f'<p style="color: #71717a; font-style: italic;">No rows returned</p>')
        return "\n".join(parts)

    display_cols = columns[:8]
    display_rows = rows[:_MAX_TABLE_ROWS]

    # HTML table
    table = [
        '<table style="width: 100%; border-collapse: collapse; font-size: 13px; '
        'font-family: \'SFMono-Regular\', Consolas, monospace; margin-top: 8px;">',
        "<thead><tr>",
    ]
    for col in display_cols:
        table.append(
            f'<th style="padding: 8px 12px; text-align: left; font-weight: 600; '
            f'color: #374151; background-color: #f8fafc; border-bottom: 2px solid #e2e8f0; '
            f'font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em;">'
            f'{_esc(str(col))}</th>'
        )
    table.append("</tr></thead><tbody>")

    for i, row in enumerate(display_rows):
        bg = "#ffffff" if i % 2 == 0 else "#f8fafc"
        table.append(f'<tr style="background-color: {bg};">')
        for j in range(len(display_cols)):
            val = str(row[j]) if j < len(row) else ""
            table.append(
                f'<td style="padding: 6px 12px; border-bottom: 1px solid #f1f5f9; '
                f'color: #374151; max-width: 200px; overflow: hidden; text-overflow: ellipsis; '
                f'white-space: nowrap;">{_esc(val[:100])}</td>'
            )
        table.append("</tr>")

    table.append("</tbody></table>")
    parts.append("\n".join(table))

    footer_notes = []
    if len(rows) > _MAX_TABLE_ROWS:
        footer_notes.append(f"+{len(rows) - _MAX_TABLE_ROWS} more row(s)")
    if len(columns) > 8:
        footer_notes.append(f"+{len(columns) - 8} more column(s)")
    if truncated_flag:
        footer_notes.append("Results truncated")
    if footer_notes:
        parts.append(_info_badge(" · ".join(footer_notes)))

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════
# DB Modify
# ═══════════════════════════════════════════════════════════════

def _format_db_modify(data: dict, ctx: FormatContext) -> str:
    rows_affected = data.get("rowsAffected", 0)
    status_msg = data.get("statusMessage", "")
    db_type = data.get("databaseType", "")
    dbname = data.get("dbname", "")

    title = ctx.title or ctx.node_name or "Database Operation"
    parts = [f'<h2 style="{_H2_STYLE}">✏️ {_esc(title)}</h2>']

    rows = []
    if status_msg:
        rows.append(("Result", status_msg))
    if rows_affected is not None:
        rows.append(("Rows Affected", str(rows_affected)))
    if db_type:
        rows.append(("Database", db_type))
    if dbname:
        rows.append(("Schema", dbname))

    if rows:
        parts.append(_metadata_table(rows))

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════
# Issue Tracker
# ═══════════════════════════════════════════════════════════════

def _format_issue_tracker(data: dict, ctx: FormatContext) -> str:
    issue_key = data.get("issueKey", "")
    url = data.get("url", "")
    message = data.get("message", "")

    title = ctx.title or ctx.node_name or "Jira Update"
    parts = [f'<h2 style="{_H2_STYLE}">🎫 {_esc(title)}</h2>']

    if issue_key:
        if url:
            parts.append(
                f'<div style="margin: 12px 0;">'
                f'<a href="{_esc(url)}" style="display: inline-block; padding: 8px 20px; '
                f'border-radius: 8px; background-color: #2563eb; color: white; '
                f'text-decoration: none; font-weight: 600; font-size: 15px;">'
                f'🔗 {_esc(issue_key)}</a></div>'
            )
        else:
            parts.append(
                f'<p style="font-weight: 700; font-size: 18px; color: #1e293b;">{_esc(issue_key)}</p>'
            )

    if message:
        parts.append(f'<p style="color: #52525b; margin-top: 8px;">{_esc(message)}</p>')

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════
# Incident Context
# ═══════════════════════════════════════════════════════════════

def _format_incident(data: dict, ctx: FormatContext) -> str:
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

    sev = _SEVERITY_COLORS.get(severity, _SEVERITY_COLORS["medium"])
    status_color = _STATUS_COLORS.get(status, "#6b7280")

    parts = []

    # Severity + Status badges
    parts.append(
        f'<div style="margin-bottom: 16px;">'
        f'<span style="display: inline-block; padding: 4px 12px; border-radius: 12px; '
        f'background-color: {sev["badge"]}; color: white; font-weight: 700; font-size: 11px; '
        f'text-transform: uppercase; letter-spacing: 0.05em;">{_esc(severity)}</span>'
        f'&nbsp;&nbsp;'
        f'<span style="display: inline-block; padding: 4px 12px; border-radius: 12px; '
        f'background-color: {status_color}; color: white; font-weight: 700; font-size: 11px; '
        f'text-transform: uppercase; letter-spacing: 0.05em;">{_esc(status)}</span>'
        f'</div>'
    )

    # Title
    parts.append(
        f'<h2 style="margin: 0 0 12px; font-size: 22px; color: #18181b; font-weight: 700;">'
        f'{_esc(title)}</h2>'
    )

    # Description
    if description:
        desc = description[:500]
        parts.append(f'<p style="color: #52525b; font-size: 14px; line-height: 1.6; margin: 0 0 16px;">{_esc(desc)}</p>')

    # Metadata table
    meta_rows = []
    if service:
        meta_rows.append(("Service", service))
    if host:
        meta_rows.append(("Host", host))
    if source:
        meta_rows.append(("Source", source))
    if team_id:
        meta_rows.append(("Team", team_id))
    if created_at:
        meta_rows.append(("Created", created_at))
    if meta_rows:
        parts.append(_metadata_table(meta_rows))

    # Tags
    if tags:
        tag_html = " ".join(
            f'<span style="display: inline-block; padding: 2px 8px; border-radius: 4px; '
            f'background-color: #f1f5f9; color: #475569; font-size: 11px; margin: 2px;">'
            f'{_esc(t)}</span>'
            for t in tags[:8]
        )
        parts.append(f'<div style="margin-top: 12px;">🏷️ {tag_html}</div>')

    # CTA button
    if ctx.incident_url:
        parts.append(_cta_button("View Incident", ctx.incident_url))

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════
# Confirmation
# ═══════════════════════════════════════════════════════════════

def _format_confirmation(data: dict, ctx: FormatContext) -> str:
    message = data.get("message", "Operation completed")
    title = ctx.title or ctx.node_name or "Notification Sent"
    return (
        f'<h2 style="{_H2_STYLE}">✅ {_esc(title)}</h2>'
        f'<p style="color: #52525b;">{_esc(message)}</p>'
    )


# ═══════════════════════════════════════════════════════════════
# Unknown / Fallback
# ═══════════════════════════════════════════════════════════════

def _format_unknown(data: dict, ctx: FormatContext) -> str:
    title = ctx.title or ctx.node_name or "Workflow Output"
    parts = [f'<h2 style="{_H2_STYLE}">{_esc(title)}</h2>']

    if data:
        pretty = json.dumps(data, indent=2, default=str)
        lines = pretty.splitlines()[:_MAX_CODE_LINES]
        parts.append(_code_block_html(_esc("\n".join(lines))))
    else:
        parts.append(f'<p style="color: #71717a; font-style: italic;">No output data</p>')

    return "\n".join(parts)


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
# Email Wrapper (branded layout)
# ═══════════════════════════════════════════════════════════════

def _wrap_email(title: str, body_html: str, ctx: FormatContext) -> str:
    workflow_info = ""
    if ctx.workflow_name:
        workflow_info = f' · {_esc(ctx.workflow_name)}'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{_esc(title)}</title>
</head>
<body style="margin: 0; padding: 0; background-color: #f4f4f5; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color: #f4f4f5;">
    <tr>
      <td align="center" style="padding: 32px 16px;">
        <table role="presentation" width="600" cellpadding="0" cellspacing="0" style="max-width: 600px; width: 100%;">

          <!-- Logo Header -->
          <tr>
            <td align="center" style="padding: 24px 0 20px;">
              <table role="presentation" cellpadding="0" cellspacing="0">
                <tr>
                  <td style="background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%); border-radius: 12px; padding: 10px 14px;">
                    <span style="color: white; font-size: 18px; font-weight: 700;">⚡</span>
                  </td>
                  <td style="padding-left: 12px;">
                    <span style="font-size: 20px; font-weight: 700; color: #18181b; letter-spacing: -0.5px;">EasyAlert</span>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Main Card -->
          <tr>
            <td>
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color: #ffffff; border-radius: 16px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -2px rgba(0, 0, 0, 0.1); overflow: hidden;">
                <tr>
                  <td style="padding: 32px;">
                    {body_html}
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td align="center" style="padding: 20px 0 8px;">
              <p style="margin: 0; font-size: 12px; color: #a1a1aa;">
                Sent by EasyAlert Automation{workflow_info}
              </p>
              <p style="margin: 4px 0 0; font-size: 11px; color: #d4d4d8;">
                This is an automated notification from your workflow.
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════
# HTML Helpers
# ═══════════════════════════════════════════════════════════════

_H2_STYLE = (
    "margin: 0 0 16px; font-size: 18px; color: #18181b; font-weight: 700;"
)


def _esc(text: str) -> str:
    return html_lib.escape(str(text))


def _code_block_html(code: str, border_color: str = "#e2e8f0") -> str:
    return (
        f'<div style="background-color: #1e293b; border-radius: 8px; padding: 16px; '
        f'margin: 8px 0; overflow-x: auto; border-left: 3px solid {border_color};">'
        f'<pre style="margin: 0; font-family: \'SFMono-Regular\', Consolas, \'Liberation Mono\', '
        f'Menlo, monospace; font-size: 12px; line-height: 1.5; color: #e2e8f0; '
        f'white-space: pre-wrap; word-wrap: break-word;">{code}</pre></div>'
    )


def _metadata_table(rows: list[tuple[str, str]]) -> str:
    table_rows = ""
    for label, value in rows:
        table_rows += (
            f'<tr>'
            f'<td style="padding: 6px 12px 6px 0; font-size: 13px; color: #71717a; '
            f'font-weight: 500; white-space: nowrap; vertical-align: top;">{_esc(label)}</td>'
            f'<td style="padding: 6px 0; font-size: 13px; color: #18181b; '
            f'font-weight: 500;">{_esc(value)}</td>'
            f'</tr>'
        )
    return (
        f'<table role="presentation" cellpadding="0" cellspacing="0" '
        f'style="margin: 12px 0; width: 100%; border-top: 1px solid #f1f5f9;">'
        f'{table_rows}</table>'
    )


def _info_badge(text: str) -> str:
    return (
        f'<div style="margin-top: 12px; padding: 8px 12px; border-radius: 6px; '
        f'background-color: #f8fafc; border: 1px solid #e2e8f0; '
        f'font-size: 12px; color: #64748b;">ℹ️ {_esc(text)}</div>'
    )


def _cta_button(text: str, url: str) -> str:
    return (
        f'<div style="margin-top: 24px; text-align: center;">'
        f'<a href="{_esc(url)}" style="display: inline-block; padding: 12px 32px; '
        f'border-radius: 8px; background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%); '
        f'color: white; text-decoration: none; font-weight: 600; font-size: 14px; '
        f'box-shadow: 0 4px 14px 0 rgba(99, 102, 241, 0.4);">'
        f'{_esc(text)}</a></div>'
    )


def _action_badge(action_type: str) -> str:
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
        "database.executeQuery": "SQL",
    }
    label = labels.get(action_type)
    if not label:
        return ""
    return (
        f'&nbsp;&nbsp;<span style="display: inline-block; padding: 2px 8px; border-radius: 4px; '
        f'background-color: #f1f5f9; color: #475569; font-size: 11px; font-weight: 600; '
        f'vertical-align: middle;">{label}</span>'
    )


# ═══════════════════════════════════════════════════════════════
# Plain Text Generator
# ═══════════════════════════════════════════════════════════════

def _generate_plain_text(
    archetype: OutputArchetype,
    data: dict,
    ctx: FormatContext,
) -> str:
    """Generate a plain text version of the email for multipart MIME."""
    title = ctx.title or ctx.node_name or "EasyAlert Notification"
    lines = [title, "=" * len(title), ""]

    if archetype == OutputArchetype.TERMINAL:
        stdout = data.get("stdout", "")
        stderr = data.get("stderr", "")
        if stdout.strip():
            lines.append("Output:")
            lines.append(stdout.strip()[:2000])
        if stderr.strip():
            lines.append("\nStderr:")
            lines.append(stderr.strip()[:1000])

    elif archetype == OutputArchetype.HTTP_RESPONSE:
        lines.append(f"Status: HTTP {data.get('statusCode', '?')}")
        body = data.get("body", "")
        if body.strip():
            lines.append(f"\nBody:\n{body.strip()[:2000]}")

    elif archetype == OutputArchetype.TABULAR:
        columns = data.get("columns", [])
        rows = data.get("rows", [])
        lines.append(f"{data.get('rowCount', len(rows))} row(s)")
        if columns:
            lines.append(" | ".join(str(c) for c in columns[:8]))
            lines.append("-" * 40)
            for row in rows[:20]:
                lines.append(" | ".join(str(v) for v in row[:8]))

    elif archetype == OutputArchetype.INCIDENT:
        sev = data.get("severity", "")
        status = data.get("status", "")
        lines.append(f"[{sev.upper()}] [{status.upper()}]")
        lines.append(data.get("title", ""))
        if data.get("description"):
            lines.append(f"\n{data['description'][:500]}")

    elif archetype == OutputArchetype.ISSUE_TRACKER:
        if data.get("issueKey"):
            lines.append(data["issueKey"])
        if data.get("url"):
            lines.append(data["url"])
        if data.get("message"):
            lines.append(data["message"])

    else:
        lines.append(json.dumps(data, indent=2, default=str)[:2000])

    if ctx.workflow_name:
        lines.append(f"\n---\nWorkflow: {ctx.workflow_name}")

    return "\n".join(lines)
