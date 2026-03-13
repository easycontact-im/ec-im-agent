# EasyAlert Automation Agent

Lightweight automation agent for [EasyAlert](https://easyalert.io). Runs on your infrastructure, executes workflow jobs dispatched by the EasyAlert SaaS platform, and reports results.

## Features

- **SSH**: Execute commands and scripts on remote hosts
- **HTTP**: Make HTTP/HTTPS requests to external services
- **Kubernetes**: Restart, scale, rollback deployments; delete pods; fetch logs
- **Scripts**: Run local bash, PowerShell, or Python scripts
- **OS Services**: Manage systemd/Windows services
- **Notifications**: Relay notifications through the SaaS API
- **Encrypted Vault**: AES-256-GCM encrypted local credential storage
- **Admin API**: Local HTTP API for credential management (localhost only)

## Quick Install

```bash
curl -fsSL https://raw.githubusercontent.com/easycontact-im/ec-im-agent/main/install.sh | sudo bash -s -- \
  --api-url https://api.easyalert.io \
  --api-key ea_agent_xxxxx
```

### Install Options

| Option | Description | Default |
|--------|-------------|---------|
| `--api-url URL` | EasyAlert API URL | (required) |
| `--api-key KEY` | Agent API key | (required) |
| `--agent-name NAME` | Display name | hostname |
| `--version TAG` | Specific version | latest |
| `--install-dir DIR` | Install directory | `/opt/easyalert/agent` |
| `--uninstall` | Remove the agent | |

## Configuration

Configuration is stored in `/etc/easyalert/agent.env`:

| Variable | Description | Default |
|----------|-------------|---------|
| `AGENT_API_URL` | EasyAlert API URL | (required) |
| `AGENT_API_KEY` | Agent API key | (required) |
| `AGENT_NAME` | Agent display name | hostname |
| `ADMIN_TOKEN` | Local admin API token | (auto-generated) |
| `POLL_INTERVAL` | Job poll interval (seconds) | `5` |
| `HEARTBEAT_INTERVAL` | Heartbeat interval (seconds) | `30` |
| `MAX_CONCURRENT_JOBS` | Max parallel jobs | `5` |
| `ADMIN_PORT` | Local admin API port | `9191` |
| `VAULT_PATH` | Vault file path | `/var/lib/easyalert/vault.json` |
| `LOG_LEVEL` | Log level | `INFO` |
| `ALLOW_PRIVATE_NETWORK` | Allow HTTP to private IPs | `false` |

## Managing Credentials

Store credentials via the local admin API:

```bash
# Store SSH credentials
curl -X PUT http://127.0.0.1:9191/connections/my-ssh \
  -H "Authorization: Bearer <ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"host": "10.0.0.1", "username": "deploy", "password": "xxx"}'

# List stored credentials
curl http://127.0.0.1:9191/connections \
  -H "Authorization: Bearer <ADMIN_TOKEN>"

# Delete a credential
curl -X DELETE http://127.0.0.1:9191/connections/my-ssh \
  -H "Authorization: Bearer <ADMIN_TOKEN>"
```

## Useful Commands

```bash
systemctl status easyalert-agent      # Check status
systemctl restart easyalert-agent     # Restart
journalctl -u easyalert-agent -f      # Follow logs
journalctl -u easyalert-agent -n 50   # Recent logs
```

## Development

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync                    # Install dependencies
uv run python main.py      # Run locally
```

## Security

See [SECURITY.md](SECURITY.md) for the trust model and security architecture.

## License

[MIT](LICENSE)
