"""Executor registry for the EasyAlert automation agent.

Maps executor category keys to their implementation classes. The worker
resolves the category from the actionType (e.g., 'ssh.executeCommand' -> 'ssh')
and looks up the corresponding executor here.
"""

from executors.base import BaseExecutor
from executors.ssh import SSHExecutor
from executors.script import ScriptExecutor
from executors.http import HTTPExecutor
from executors.kubernetes import KubernetesExecutor
from executors.os_service import OSServiceExecutor
from executors.notification import NotificationExecutor
from executors.slack import SlackExecutor
from executors.jira import JiraExecutor
from executors.email import EmailExecutor
from executors.teams import TeamsExecutor
from executors.database import DatabaseExecutor
from executors.winrm import WinRMExecutor

EXECUTOR_REGISTRY: dict[str, type[BaseExecutor]] = {
    "ssh": SSHExecutor,
    "script": ScriptExecutor,
    "http": HTTPExecutor,
    "kubernetes": KubernetesExecutor,
    "os": OSServiceExecutor,
    "notification": NotificationExecutor,
    "slack": SlackExecutor,
    "jira": JiraExecutor,
    "email": EmailExecutor,
    "teams": TeamsExecutor,
    "database": DatabaseExecutor,
    "winrm": WinRMExecutor,
}

__all__ = [
    "BaseExecutor",
    "SSHExecutor",
    "ScriptExecutor",
    "HTTPExecutor",
    "KubernetesExecutor",
    "OSServiceExecutor",
    "NotificationExecutor",
    "SlackExecutor",
    "JiraExecutor",
    "EmailExecutor",
    "TeamsExecutor",
    "DatabaseExecutor",
    "WinRMExecutor",
    "EXECUTOR_REGISTRY",
]
