<p align="center">
  <img src="https://easyalert.io/favicon.svg" width="56" height="56" alt="EasyAlert" />
</p>

<h1 align="center">EasyAlert Agent</h1>

<p align="center">
  Lightweight automation agent that runs on your infrastructure, executes workflow jobs dispatched by <a href="https://easyalert.io">EasyAlert</a>, and reports results back to the platform.
</p>

<p align="center">
  <a href="#quick-install">Install</a> &middot;
  <a href="#docker">Docker</a> &middot;
  <a href="#configuration">Configuration</a> &middot;
  <a href="#executors">Executors</a> &middot;
  <a href="#credentials">Credentials</a> &middot;
  <a href="SECURITY.md">Security</a>
</p>

---

## How It Works

```
EasyAlert SaaS ──── polls ────▶ Agent (your server)
                                  │
                  ┌───────────────┼───────────────┐
                  ▼               ▼               ▼
              SSH/Script     HTTP/K8s        Slack/Jira
                  │               │               │
                  └───────────────┼───────────────┘
                                  │
                  ◀── results ────┘
```

The agent connects **outbound** to the EasyAlert API — no inbound ports need to be opened. Credentials are stored in a locally encrypted vault and **never leave your machine**.

---

## Quick Install

> Requires Ubuntu 22.04+ or Debian 12+.

```bash
curl -fsSL https://raw.githubusercontent.com/easycontact-im/ec-im-agent/main/install.sh | \
  sudo bash -s -- \
    --api-url https://api.easyalert.io \
    --api-key ea_agent_xxxxx
```

The installer creates a `systemd` service that starts automatically on boot.

### Install Options

| Flag | Description | Default |
|------|-------------|---------|
| `--api-url URL` | EasyAlert API URL | *(required)* |
| `--api-key KEY` | Agent API key | *(required)* |
| `--agent-name NAME` | Display name in the dashboard | hostname |
| `--version TAG` | Pin to a specific release (e.g. `v1.0.0`) | `latest` |
| `--install-dir DIR` | Installation path | `/opt/easyalert/agent` |
| `--uninstall` | Remove the agent, service, and all data | |

### Upgrade

Re-run the install command — it's idempotent. Config and vault are preserved.

### Uninstall

```bash
curl -fsSL https://raw.githubusercontent.com/easycontact-im/ec-im-agent/main/install.sh | \
  sudo bash -s -- --uninstall
```

---

## Docker

```bash
docker run -d \
  --name easyalert-agent \
  --restart unless-stopped \
  -e AGENT_API_URL=https://api.easyalert.io \
  -e AGENT_API_KEY=ea_agent_xxxxx \
  -e AGENT_NAME=prod-agent \
  easycontactai/agent:latest
```

The image is based on Alpine Linux, runs as a non-root user, and includes a built-in health check.

---

## Configuration

All settings are read from environment variables. When installed via the script, they live in `/etc/easyalert/agent.env`.

| Variable | Description | Default |
|----------|-------------|---------|
| `AGENT_API_URL` | EasyAlert API base URL | *(required)* |
| `AGENT_API_KEY` | Agent authentication key | *(required)* |
| `AGENT_NAME` | Display name in the dashboard | hostname |
| `POLL_INTERVAL` | Job polling interval (seconds) | `5` |
| `HEARTBEAT_INTERVAL` | Heartbeat interval (seconds) | `30` |
| `MAX_CONCURRENT_JOBS` | Maximum parallel job executions | `5` |
| `ADMIN_PORT` | Local admin API port | `9191` |
| `ADMIN_TOKEN` | Bearer token for the admin API | auto-generated |
| `VAULT_PATH` | Path to the encrypted vault file | `/var/lib/easyalert/vault.json` |
| `LOG_LEVEL` | `DEBUG` / `INFO` / `WARNING` / `ERROR` | `INFO` |
| `ALLOW_PRIVATE_NETWORK` | Allow HTTP executor to reach private IPs | `false` |
| `ALLOW_OS_RESTART` | Allow OS service executor to restart services | `false` |

---

## Executors

Each workflow step is handled by a specialized executor:

| Executor | Actions | Description |
|----------|---------|-------------|
| **SSH** | `executeCommand`, `uploadFile`, `testConnection` | Remote command execution via AsyncSSH |
| **Script** | `runBash`, `runPython`, `runPowershell` | Local script execution with timeout |
| **HTTP** | `request`, `testConnection` | HTTP/HTTPS requests with SSRF protection |
| **Kubernetes** | `restartDeployment`, `scalePods`, `rollback`, `deletePod`, `getLogs` | kubectl operations with kubeconfig isolation |
| **OS Service** | `start`, `stop`, `restart`, `status` | systemd (Linux) / sc.exe (Windows) management |
| **Slack** | `sendMessage`, `updateMessage`, `addReaction`, `testConnection` | Slack Bot API integration |
| **Jira** | `createIssue`, `updateIssue`, `addComment`, `transitionIssue`, `testConnection` | Jira REST API integration |
| **Email** | `send`, `testConnection` | SMTP email delivery |
| **Teams** | `sendMessage`, `sendCard`, `testConnection` | Microsoft Teams webhook integration |
| **Notification** | `send` | Relay notifications through the SaaS API |

---

## Credentials

Credentials (SSH keys, API tokens, etc.) are stored in a **locally encrypted vault** on the agent's machine. They are never sent to the EasyAlert SaaS platform.

- **Encryption**: AES-256-GCM
- **Key derivation**: PBKDF2-HMAC-SHA256 with 600,000 iterations
- **File permissions**: `0600` (owner read/write only)

### Managing Credentials

Use the local admin API (binds to `127.0.0.1` only):

```bash
# Store a credential
curl -X PUT http://127.0.0.1:9191/connections/my-server \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"host": "10.0.0.5", "username": "deploy", "privateKey": "..."}'

# List all credential IDs
curl http://127.0.0.1:9191/connections \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Test a credential
curl -X POST http://127.0.0.1:9191/connections/my-server/test \
  -H "Authorization: Bearer $ADMIN_TOKEN"

# Delete a credential
curl -X DELETE http://127.0.0.1:9191/connections/my-server \
  -H "Authorization: Bearer $ADMIN_TOKEN"
```

> The `ADMIN_TOKEN` is printed during installation. You can also find it in `/etc/easyalert/agent.env`.

---

## Service Management

```bash
systemctl status easyalert-agent       # Check status
systemctl restart easyalert-agent      # Restart the agent
journalctl -u easyalert-agent -f       # Follow live logs
journalctl -u easyalert-agent -n 100   # Last 100 log lines
```

---

## Architecture

```
ec-im-agent/
├── main.py              # Entry point — poll, heartbeat, admin loops
├── config.py            # Pydantic Settings (env vars)
├── api_client.py        # SaaS API client (httpx, circuit breaker)
├── vault.py             # AES-256-GCM encrypted credential store
├── worker.py            # Concurrent job executor (semaphore-based)
├── admin_server.py      # Local admin HTTP server (aiohttp)
├── executors/
│   ├── base.py          # Abstract base executor
│   ├── ssh.py           # SSH (asyncssh)
│   ├── script.py        # Local scripts (subprocess)
│   ├── http.py          # HTTP requests (httpx)
│   ├── kubernetes.py    # Kubernetes (kubectl)
│   ├── os_service.py    # OS services (systemctl/sc.exe)
│   ├── slack.py         # Slack Bot API
│   ├── jira.py          # Jira REST API
│   ├── email.py         # SMTP email
│   ├── teams.py         # Microsoft Teams webhooks
│   └── notification.py  # SaaS notification relay
├── install.sh           # One-line installer (Ubuntu/Debian)
├── Dockerfile           # Multi-stage Alpine build
└── pyproject.toml       # Dependencies (managed with uv)
```

---

## Development

Requires **Python 3.12+** and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/easycontact-im/ec-im-agent.git
cd ec-im-agent

uv sync                           # Install dependencies
uv run python main.py             # Run locally

# With environment variables
AGENT_API_URL=http://localhost:8080 \
AGENT_API_KEY=ea_agent_dev_xxx \
uv run python main.py
```

### Running Tests

```bash
uv sync --group dev               # Install dev dependencies
uv run pytest                     # Run all tests
uv run pytest --cov               # With coverage
```

---

## Security

See [SECURITY.md](SECURITY.md) for the full trust model, encryption details, systemd hardening, and vulnerability reporting.

**Key highlights:**
- Credentials encrypted at rest with AES-256-GCM — never transmitted to the cloud
- Admin API bound to `127.0.0.1` — not network-accessible
- SSRF protection blocks requests to private networks by default
- Constant-time token comparison prevents timing attacks
- systemd sandboxing: `NoNewPrivileges`, `ProtectSystem=strict`, `PrivateTmp`

---

## License

[MIT](LICENSE) &copy; 2026 EasyContact
