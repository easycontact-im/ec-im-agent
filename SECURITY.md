# Security

## Trust Model

The EasyAlert automation agent operates on a **split-trust model**:

- **SaaS API (ec-im-api)**: Sends workflow job definitions (what to do, which connection to use). Does NOT have access to credentials.
- **Agent (ec-im-agent)**: Runs on customer infrastructure. Holds encrypted credentials in a local vault. Executes jobs using those credentials.

**Credentials never leave the customer's machine.** The SaaS API references credentials by connection ID only.

## Credential Storage

- All credentials are stored in a local vault file encrypted with **AES-256-GCM**
- Encryption key is derived from the agent API key using **PBKDF2-HMAC-SHA256** (600,000 iterations)
- Vault file permissions are enforced to `0600` (owner read/write only)
- The vault is never transmitted to the SaaS API

## Authentication

- Agent authenticates to the SaaS API using a pre-shared API key (`X-Agent-Api-Key` header)
- API keys are prefixed with `ea_agent_` and are bcrypt-hashed server-side
- The local admin API requires a Bearer token (`ADMIN_TOKEN`) for all requests
- Token comparison uses constant-time `hmac.compare_digest` to prevent timing attacks

## Network Security

- The local admin API binds to `127.0.0.1` only — never exposed to the network
- SSRF protection blocks HTTP requests to private/internal network addresses by default
- Private network access can be explicitly enabled via the `ALLOW_PRIVATE_NETWORK` config setting
- SSH connections verify host keys by default; connections are rejected if `known_hosts` is not found

## Input Validation

- SSH script interpreters are validated against a whitelist of known safe paths
- Kubernetes resource names are validated against a strict regex pattern
- Numeric parameters (replicas, timeouts, etc.) are validated as positive integers
- Output from remote commands is truncated to 1 MB to prevent memory exhaustion
- Admin API request body size is limited to 1 MB

## Rate Limiting

- The local admin API applies a sliding-window rate limit (60 requests per 60 seconds per client IP)

## Systemd Hardening

The install script configures the following systemd security directives:

- `NoNewPrivileges=true`
- `ProtectSystem=strict`
- `ProtectHome=true`
- `PrivateTmp=true`
- `ProtectKernelTunables=true`
- `ProtectControlGroups=true`
- `RestrictSUIDSGID=true`

## Reporting Vulnerabilities

If you discover a security vulnerability, please report it responsibly by emailing **security@easycontact.ai**. Do not open a public issue.
