"""
Output formatters for Slack, Teams, and Email channels.

Formats automation workflow execution output into rich, channel-specific messages.
Used by slack, teams, and email executors to render structured output.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("ec-im-agent.formatters")


@dataclass
class FormatContext:
    """Context for formatting output."""
    title: str = ""
    source_action: str = ""
    node_name: str = ""
    workflow_name: str = ""
    incident_url: str = ""
    severity: str = ""
    status: str = ""
    timestamp: str = ""


def format_output(
    channel: str,
    output: Any,
    context: FormatContext,
    action_type: str = "",
    output_type_hint: str = "",
) -> dict[str, Any]:
    """
    Format execution output for a specific channel.

    Args:
        channel: "slack", "teams", or "email"
        output: Raw output data (str, dict, list)
        context: Formatting context with metadata
        action_type: The action that produced this output (e.g., "ssh.executeCommand")
        output_type_hint: Hint about the output type (e.g., "json", "text", "table")

    Returns:
        Channel-specific formatted payload:
        - slack: {"blocks": [...], "text": "..."}
        - teams: {"card": {...}}
        - email: {"html": "..."}
    """
    if channel == "slack":
        return _format_slack(output, context, action_type, output_type_hint)
    elif channel == "teams":
        return _format_teams(output, context, action_type, output_type_hint)
    elif channel == "email":
        return _format_email(output, context, action_type, output_type_hint)
    else:
        logger.warning(f"Unknown channel '{channel}', returning raw output")
        return {"text": str(output)}


def _stringify(output: Any) -> str:
    """Convert output to a display string."""
    if output is None:
        return ""
    if isinstance(output, str):
        return output
    try:
        return json.dumps(output, indent=2, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return str(output)


def _truncate(text: str, max_len: int = 3000) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len - 1] + "\u2026"


# ════════════════════════════════════════════════════════════════
# Slack Block Kit
# ════════════════════════════════════════════════════════════════

def _format_slack(output: Any, ctx: FormatContext, action_type: str, hint: str) -> dict[str, Any]:
    text = _stringify(output)
    title = ctx.title or ctx.node_name or action_type or "Workflow Output"

    blocks: list[dict] = []

    # Header
    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*{title}*"}
    })

    # Metadata line
    meta_parts = []
    if ctx.workflow_name:
        meta_parts.append(f"Workflow: {ctx.workflow_name}")
    if ctx.source_action:
        meta_parts.append(f"Action: `{ctx.source_action}`")
    if ctx.severity:
        meta_parts.append(f"Severity: {ctx.severity}")
    if meta_parts:
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": " \u2022 ".join(meta_parts)}]
        })

    # Output body
    if text:
        truncated = _truncate(text, 2900)
        # Use code block for structured/command output
        if hint in ("json", "table", "command") or action_type.startswith("ssh.") or action_type.startswith("script."):
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"```\n{truncated}\n```"}
            })
        else:
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": truncated}
            })

    # Footer
    if ctx.timestamp or ctx.incident_url:
        footer_parts = []
        if ctx.timestamp:
            footer_parts.append(ctx.timestamp)
        if ctx.incident_url:
            footer_parts.append(f"<{ctx.incident_url}|View Incident>")
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": " \u2022 ".join(footer_parts)}]
        })

    fallback = _truncate(f"{title}: {text}", 200) if text else title
    return {"blocks": blocks, "text": fallback}


# ════════════════════════════════════════════════════════════════
# Teams Adaptive Card
# ════════════════════════════════════════════════════════════════

def _format_teams(output: Any, ctx: FormatContext, action_type: str, hint: str) -> dict[str, Any]:
    text = _stringify(output)
    title = ctx.title or ctx.node_name or action_type or "Workflow Output"

    body: list[dict] = []

    # Title
    body.append({
        "type": "TextBlock",
        "text": title,
        "size": "medium",
        "weight": "bolder",
        "wrap": True,
    })

    # Facts
    facts = []
    if ctx.workflow_name:
        facts.append({"title": "Workflow", "value": ctx.workflow_name})
    if ctx.source_action:
        facts.append({"title": "Action", "value": ctx.source_action})
    if ctx.severity:
        facts.append({"title": "Severity", "value": ctx.severity})
    if ctx.status:
        facts.append({"title": "Status", "value": ctx.status})
    if facts:
        body.append({"type": "FactSet", "facts": facts, "spacing": "small"})

    # Output
    if text:
        truncated = _truncate(text, 2900)
        body.append({
            "type": "TextBlock",
            "text": truncated,
            "wrap": True,
            "fontType": "monospace" if hint in ("json", "table", "command") or action_type.startswith("ssh.") else "default",
            "spacing": "medium",
        })

    # Timestamp
    if ctx.timestamp:
        body.append({
            "type": "TextBlock",
            "text": ctx.timestamp,
            "size": "small",
            "isSubtle": True,
            "spacing": "medium",
        })

    card = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.5",
        "body": body,
    }

    return {"card": card}


# ════════════════════════════════════════════════════════════════
# Email HTML
# ════════════════════════════════════════════════════════════════

def _format_email(output: Any, ctx: FormatContext, action_type: str, hint: str) -> dict[str, Any]:
    text = _stringify(output)
    title = ctx.title or ctx.node_name or action_type or "Workflow Output"

    meta_rows = ""
    if ctx.workflow_name:
        meta_rows += f"<tr><td style='padding:4px 8px;font-weight:600;color:#64748b'>Workflow</td><td style='padding:4px 8px'>{ctx.workflow_name}</td></tr>"
    if ctx.source_action:
        meta_rows += f"<tr><td style='padding:4px 8px;font-weight:600;color:#64748b'>Action</td><td style='padding:4px 8px'><code>{ctx.source_action}</code></td></tr>"
    if ctx.severity:
        meta_rows += f"<tr><td style='padding:4px 8px;font-weight:600;color:#64748b'>Severity</td><td style='padding:4px 8px'>{ctx.severity}</td></tr>"

    meta_html = f"<table style='border-collapse:collapse;margin:12px 0'>{meta_rows}</table>" if meta_rows else ""

    # Output block
    output_html = ""
    if text:
        escaped = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        if hint in ("json", "table", "command") or action_type.startswith("ssh."):
            output_html = f"<pre style='background:#f1f5f9;padding:12px;border-radius:6px;font-size:13px;overflow-x:auto;white-space:pre-wrap'>{escaped}</pre>"
        else:
            output_html = f"<div style='padding:8px 0;white-space:pre-wrap'>{escaped}</div>"

    # Footer
    footer = ""
    if ctx.timestamp or ctx.incident_url:
        parts = []
        if ctx.timestamp:
            parts.append(ctx.timestamp)
        if ctx.incident_url:
            parts.append(f"<a href='{ctx.incident_url}'>View Incident</a>")
        footer = f"<div style='padding:8px 0;font-size:12px;color:#94a3b8'>{' &bull; '.join(parts)}</div>"

    html = f"""<div style='font-family:-apple-system,BlinkMacSystemFont,sans-serif;max-width:600px'>
<h2 style='margin:0 0 8px;font-size:18px'>{title}</h2>
{meta_html}
{output_html}
{footer}
</div>"""

    return {"html": html}
