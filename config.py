import logging
import os
import platform
import secrets
import sys
from pathlib import Path

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings


class AgentSettings(BaseSettings):
    """Configuration for the automation agent.

    All values can be set via environment variables prefixed with AGENT_.
    Example: AGENT_API_URL, AGENT_API_KEY, AGENT_NAME, etc.
    """

    AGENT_API_URL: str
    AGENT_API_KEY: str
    AGENT_NAME: str = platform.node()
    POLL_INTERVAL: int = 5
    HEARTBEAT_INTERVAL: int = 30
    MAX_CONCURRENT_JOBS: int = 5
    ADMIN_PORT: int = 9191
    ADMIN_TOKEN: str | None = None
    VAULT_PATH: str = "~/.easyalert/vault.json"
    LOG_LEVEL: str = "INFO"
    ALLOW_PRIVATE_NETWORK: bool = False
    ALLOW_OS_RESTART: bool = False

    model_config = {
        "env_prefix": "",
        "case_sensitive": True,
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }

    @field_validator("AGENT_API_URL")
    @classmethod
    def validate_api_url(cls, v: str) -> str:
        """Validate that the API URL uses a valid scheme."""
        if not v.startswith(("https://", "http://")):
            raise ValueError("AGENT_API_URL must start with https:// or http://")
        if v.startswith("http://"):
            logging.getLogger("ec-im-agent.config").warning(
                "AGENT_API_URL uses HTTP - API key will be sent in plaintext. "
                "Use HTTPS in production."
            )
        return v

    @field_validator("POLL_INTERVAL", "HEARTBEAT_INTERVAL")
    @classmethod
    def validate_positive_int(cls, v: int) -> int:
        """Ensure interval values are at least 1 second."""
        if v < 1:
            raise ValueError("Must be >= 1")
        return v

    @field_validator("MAX_CONCURRENT_JOBS")
    @classmethod
    def validate_max_concurrent(cls, v: int) -> int:
        """Ensure concurrency limit is within a reasonable range."""
        if v < 1 or v > 50:
            raise ValueError("Must be between 1 and 50")
        return v

    @field_validator("ADMIN_PORT")
    @classmethod
    def validate_admin_port(cls, v: int) -> int:
        """Ensure admin port is a valid port number."""
        if v < 1 or v > 65535:
            raise ValueError("Must be between 1 and 65535")
        return v

    @field_validator("ADMIN_TOKEN", mode="before")
    @classmethod
    def auto_generate_token(cls, v: str | None) -> str:
        """Auto-generate a secure admin token if not provided, persisting to disk."""
        _logger = logging.getLogger("ec-im-agent.config")
        if v is None or v.strip() == "":
            token_file = Path.home() / ".easyalert" / "admin_token"
            value: str | None = None
            if token_file.exists():
                try:
                    value = token_file.read_text().strip()
                    if value:
                        _logger.info("ADMIN_TOKEN loaded from %s", token_file)
                except OSError:
                    value = None
            if not value:
                value = secrets.token_urlsafe(32)
                try:
                    token_file.parent.mkdir(parents=True, exist_ok=True)
                    token_file.write_text(value)
                    if sys.platform != "win32":
                        os.chmod(token_file, 0o600)
                    _logger.info("ADMIN_TOKEN auto-generated and persisted to %s", token_file)
                except OSError as exc:
                    _logger.warning("ADMIN_TOKEN auto-generated (could not persist: %s)", exc)
            return value
        return v

    @model_validator(mode="after")
    def warn_dangerous_settings(self) -> "AgentSettings":
        """Log warnings when security-sensitive settings are enabled."""
        _logger = logging.getLogger("ec-im-agent.config")
        if self.ALLOW_PRIVATE_NETWORK:
            _logger.warning(
                "ALLOW_PRIVATE_NETWORK is enabled. "
                "HTTP executor can access private/internal networks."
            )
        if self.ALLOW_OS_RESTART:
            _logger.warning(
                "ALLOW_OS_RESTART is enabled. "
                "OS service executor can restart system services."
            )
        return self


settings = AgentSettings()


def setup_logging() -> logging.Logger:
    """Configure and return the root logger for the agent."""
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    logger = logging.getLogger("ec-im-agent")
    logger.setLevel(log_level)
    return logger
