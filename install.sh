#!/usr/bin/env bash
# ============================================================
# EasyAlert Agent - Installation Script
#
# Supported: Ubuntu 22.04+, Debian 12+
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/easycontact-im/ec-im-agent/main/install.sh | sudo bash -s -- \
#     --api-url https://api.easyalert.io \
#     --api-key ea_agent_xxxxx
#
# Options:
#   --api-url URL       EasyAlert API URL (required)
#   --api-key KEY       Agent API key (required)
#   --agent-name NAME   Agent display name (default: hostname)
#   --version TAG       Install specific version (default: latest)
#   --install-dir DIR   Installation directory (default: /opt/easyalert/agent)
#   --uninstall         Remove agent and all data
# ============================================================

set -euo pipefail

# ── Constants ────────────────────────────────────────────────
GITHUB_REPO="easycontact-im/ec-im-agent"
GITHUB_API="https://api.github.com/repos/${GITHUB_REPO}"
GITHUB_RAW="https://github.com/${GITHUB_REPO}"

SERVICE_USER="easyalert"
SERVICE_NAME="easyalert-agent"
CONFIG_DIR="/etc/easyalert"
VAULT_DIR="/var/lib/easyalert"

# ── Defaults (env vars supported for frontend-generated scripts) ──
API_URL="${AGENT_API_URL:-}"
API_KEY="${AGENT_API_KEY:-}"
AGENT_NAME="${AGENT_NAME:-}"
VERSION="latest"
INSTALL_DIR="/opt/easyalert/agent"
DO_UNINSTALL=false

# ── Colors ───────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Parse arguments ──────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --api-url)     API_URL="$2";     shift 2 ;;
        --api-key)     API_KEY="$2";     shift 2 ;;
        --agent-name)  AGENT_NAME="$2";  shift 2 ;;
        --version)     VERSION="$2";     shift 2 ;;
        --install-dir) INSTALL_DIR="$2"; shift 2 ;;
        --uninstall)   DO_UNINSTALL=true; shift ;;
        --help|-h)
            echo "Usage: install.sh --api-url URL --api-key KEY [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --api-url URL       EasyAlert API URL (required)"
            echo "  --api-key KEY       Agent API key (required)"
            echo "  --agent-name NAME   Agent display name (default: hostname)"
            echo "  --version TAG       Install specific version (default: latest)"
            echo "  --install-dir DIR   Install directory (default: /opt/easyalert/agent)"
            echo "  --uninstall         Remove the agent completely"
            exit 0
            ;;
        *) error "Unknown option: $1. Use --help for usage." ;;
    esac
done

# ── Uninstall ────────────────────────────────────────────────
if $DO_UNINSTALL; then
    echo ""
    echo "============================================"
    echo "  EasyAlert Agent - Uninstaller"
    echo "============================================"
    echo ""

    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        info "Stopping service..."
        systemctl stop "$SERVICE_NAME"
    fi

    if systemctl is-enabled --quiet "$SERVICE_NAME" 2>/dev/null; then
        systemctl disable "$SERVICE_NAME"
    fi

    [ -f "/etc/systemd/system/${SERVICE_NAME}.service" ] && rm -f "/etc/systemd/system/${SERVICE_NAME}.service" && systemctl daemon-reload
    [ -d "$INSTALL_DIR" ] && rm -rf "$INSTALL_DIR"
    [ -d "$CONFIG_DIR" ] && rm -rf "$CONFIG_DIR"
    [ -d "$VAULT_DIR" ] && rm -rf "$VAULT_DIR"

    if id "$SERVICE_USER" &>/dev/null; then
        userdel "$SERVICE_USER" 2>/dev/null || true
    fi

    success "Agent uninstalled successfully."
    exit 0
fi

# ── Validation ───────────────────────────────────────────────
[[ -z "$API_URL" ]] && error "--api-url is required"
[[ -z "$API_KEY" ]] && error "--api-key is required"
[[ -z "$AGENT_NAME" ]] && AGENT_NAME="$(hostname)"

# ── Check root ───────────────────────────────────────────────
[[ $EUID -ne 0 ]] && error "This script must be run as root (or with sudo)"

echo ""
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}  EasyAlert Agent Installer${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""
echo "  API URL:     $API_URL"
echo "  API Key:     [configured]"
echo "  Agent Name:  $AGENT_NAME"
echo "  Install Dir: $INSTALL_DIR"
echo "  Version:     $VERSION"
echo ""

# ── Check if already installed (upgrade path) ────────────────
IS_UPGRADE=false
if [ -d "$INSTALL_DIR" ] && [ -f "$CONFIG_DIR/agent.env" ]; then
    IS_UPGRADE=true
    warn "Existing installation detected. Upgrading..."
    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        info "Stopping existing service..."
        systemctl stop "$SERVICE_NAME"
    fi
fi

# ── Step 1: System dependencies ──────────────────────────────
info "[1/7] Checking system dependencies..."

# Check Python 3.12+
if command -v python3 &>/dev/null; then
    PY_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    PY_MAJOR=$(echo "$PY_VERSION" | cut -d. -f1)
    PY_MINOR=$(echo "$PY_VERSION" | cut -d. -f2)

    if [[ "$PY_MAJOR" -lt 3 ]] || [[ "$PY_MAJOR" -eq 3 && "$PY_MINOR" -lt 12 ]]; then
        error "Python 3.12+ is required (found $PY_VERSION). Install python3.12 or newer."
    fi
    success "Python $PY_VERSION found"
else
    info "Installing Python 3..."
    apt-get update -qq
    apt-get install -y -qq python3 python3-venv > /dev/null
    success "Python 3 installed"
fi

# Ensure curl and other basics
for cmd in curl tar; do
    if ! command -v "$cmd" &>/dev/null; then
        info "Installing $cmd..."
        apt-get update -qq 2>/dev/null
        apt-get install -y -qq "$cmd" > /dev/null
    fi
done

success "System dependencies OK"

# ── Step 2: Install uv ──────────────────────────────────────
info "[2/7] Checking uv package manager..."

if ! command -v uv &>/dev/null; then
    info "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh 2>/dev/null
    # uv installs to ~/.local/bin (for root that's /root/.local/bin)
    export PATH="/root/.local/bin:$PATH"

    if ! command -v uv &>/dev/null; then
        error "Failed to install uv. Please install manually: https://docs.astral.sh/uv/"
    fi
fi

UV_VERSION=$(uv --version 2>/dev/null | head -1)
success "uv ready ($UV_VERSION)"

# ── Step 3: Create service user ──────────────────────────────
info "[3/7] Setting up service user..."

if ! id "$SERVICE_USER" &>/dev/null; then
    useradd --system --home-dir "$VAULT_DIR" --create-home --shell /usr/sbin/nologin "$SERVICE_USER"
    success "Created user '$SERVICE_USER'"
else
    success "User '$SERVICE_USER' already exists"
fi

# ── Step 4: Resolve version and download ─────────────────────
info "[4/7] Downloading agent..."

if [[ "$VERSION" == "latest" ]]; then
    info "Resolving latest version..."
    VERSION=$(curl -fsSL "${GITHUB_API}/releases/latest" 2>/dev/null | grep '"tag_name"' | head -1 | cut -d'"' -f4)

    if [[ -z "$VERSION" ]]; then
        error "Could not resolve latest version. Check network access to github.com"
    fi
    info "Latest version: $VERSION"
fi

# Ensure version starts with 'v'
[[ "$VERSION" != v* ]] && VERSION="v${VERSION}"

DOWNLOAD_URL="${GITHUB_RAW}/releases/download/${VERSION}/ec-im-agent-${VERSION}.tar.gz"
CHECKSUM_URL="${GITHUB_RAW}/releases/download/${VERSION}/checksums.txt"

# Create install directory
mkdir -p "$INSTALL_DIR"

# Download tarball
info "Downloading $DOWNLOAD_URL..."
HTTP_CODE=$(curl -fsSL -w "%{http_code}" -o /tmp/ec-im-agent.tar.gz "$DOWNLOAD_URL" 2>/dev/null || echo "000")

if [[ "$HTTP_CODE" != "200" ]]; then
    rm -f /tmp/ec-im-agent.tar.gz
    error "Download failed (HTTP $HTTP_CODE). Check version '$VERSION' exists at: https://github.com/${GITHUB_REPO}/releases"
fi

# Verify checksum if available
if curl -fsSL -o /tmp/ec-im-agent-checksums.txt "$CHECKSUM_URL" 2>/dev/null; then
    EXPECTED_SUM=$(grep "ec-im-agent-${VERSION}.tar.gz" /tmp/ec-im-agent-checksums.txt | awk '{print $1}')
    if [[ -n "$EXPECTED_SUM" ]]; then
        ACTUAL_SUM=$(sha256sum /tmp/ec-im-agent.tar.gz | awk '{print $1}')
        if [[ "$EXPECTED_SUM" != "$ACTUAL_SUM" ]]; then
            rm -f /tmp/ec-im-agent.tar.gz /tmp/ec-im-agent-checksums.txt
            error "Checksum mismatch! Download may be corrupted or tampered with."
        fi
        success "Checksum verified"
    fi
    rm -f /tmp/ec-im-agent-checksums.txt
fi

# Extract
tar xzf /tmp/ec-im-agent.tar.gz -C "$INSTALL_DIR" --strip-components=1
rm -f /tmp/ec-im-agent.tar.gz

success "Agent $VERSION downloaded and extracted"

# ── Step 5: Install Python dependencies ──────────────────────
info "[5/7] Installing Python dependencies..."

cd "$INSTALL_DIR"
if ! uv sync --no-dev --no-install-project 2>&1 | tail -5; then
    error "uv sync failed. Check dependencies in pyproject.toml."
fi

# Set ownership
chown -R "$SERVICE_USER":"$SERVICE_USER" "$INSTALL_DIR"

success "Dependencies installed"

# ── Step 6: Configure ────────────────────────────────────────
info "[6/7] Configuring agent..."

mkdir -p "$CONFIG_DIR"
mkdir -p "$VAULT_DIR"
chown -R "$SERVICE_USER":"$SERVICE_USER" "$VAULT_DIR"

# Write environment file (only if new install or explicitly upgrading config)
if ! $IS_UPGRADE; then
    # Generate a secure random ADMIN_TOKEN for the local admin API
    ADMIN_TOKEN=$(python3 -c "import secrets; print(secrets.token_hex(32))")

    {
        printf '# EasyAlert Agent Configuration\n'
        printf '# Generated: %s\n\n' "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        printf 'AGENT_API_URL=%s\n' "$API_URL"
        printf 'AGENT_API_KEY=%s\n' "$API_KEY"
        printf 'AGENT_NAME=%s\n' "$AGENT_NAME"
        printf 'ADMIN_TOKEN=%s\n' "$ADMIN_TOKEN"
        printf 'POLL_INTERVAL=5\n'
        printf 'HEARTBEAT_INTERVAL=30\n'
        printf 'MAX_CONCURRENT_JOBS=5\n'
        printf 'ADMIN_PORT=9191\n'
        printf 'VAULT_PATH=%s/vault.json\n' "$VAULT_DIR"
        printf 'LOG_LEVEL=INFO\n'
    } > "$CONFIG_DIR/agent.env"

    chmod 600 "$CONFIG_DIR/agent.env"
    success "Configuration written to $CONFIG_DIR/agent.env"
else
    success "Keeping existing configuration at $CONFIG_DIR/agent.env"
fi

# ── Step 7: Systemd service ─────────────────────────────────
info "[7/7] Setting up systemd service..."

cat > "/etc/systemd/system/${SERVICE_NAME}.service" << EOF
[Unit]
Description=EasyAlert Automation Agent
Documentation=https://github.com/${GITHUB_REPO}
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory=$INSTALL_DIR
EnvironmentFile=$CONFIG_DIR/agent.env
ExecStart=$INSTALL_DIR/.venv/bin/python main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$SERVICE_NAME

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$VAULT_DIR /tmp
PrivateTmp=true
ProtectKernelTunables=true
ProtectControlGroups=true
RestrictSUIDSGID=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "$SERVICE_NAME" > /dev/null 2>&1
systemctl start "$SERVICE_NAME"

# Wait a moment and check status
sleep 2
if systemctl is-active --quiet "$SERVICE_NAME"; then
    success "Service started successfully"
else
    warn "Service may not have started correctly. Check: journalctl -u $SERVICE_NAME -n 20"
fi

# ── Summary ──────────────────────────────────────────────────
echo ""
echo -e "${GREEN}============================================${NC}"
echo -e "${GREEN}  EasyAlert Agent installed successfully!${NC}"
echo -e "${GREEN}============================================${NC}"
echo ""
echo "  Version:    $VERSION"
echo "  Service:    $SERVICE_NAME"
echo "  Config:     $CONFIG_DIR/agent.env"
echo "  Vault:      $VAULT_DIR/vault.json"
echo "  Logs:       journalctl -u $SERVICE_NAME -f"
if ! $IS_UPGRADE; then
echo ""
echo -e "  ${YELLOW}Admin Token:${NC}  $ADMIN_TOKEN"
echo -e "  ${YELLOW}Save this token — it is required for the local admin API.${NC}"
fi
echo ""
echo "  Useful commands:"
echo "    systemctl status $SERVICE_NAME     # Check status"
echo "    systemctl restart $SERVICE_NAME    # Restart agent"
echo "    journalctl -u $SERVICE_NAME -n 50  # View recent logs"
echo ""
echo "  Store credentials (via local admin API):"
echo "    curl -X PUT http://127.0.0.1:9191/connections/my-ssh \\"
echo "      -H 'Authorization: Bearer <ADMIN_TOKEN>' \\"
echo "      -H 'Content-Type: application/json' \\"
echo "      -d '{\"host\": \"10.0.0.1\", \"username\": \"deploy\", \"password\": \"xxx\"}'"
echo ""
echo -e "  ${YELLOW}Security note:${NC} The --api-key value may be visible in the"
echo "  process list during installation. The key is stored securely in"
echo "  $CONFIG_DIR/agent.env (permissions 600) after install."
echo ""
