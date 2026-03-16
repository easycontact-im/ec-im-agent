# ============================================================
# Stage 1: Builder - install dependencies
# ============================================================
FROM python:3.12-alpine AS builder

# Build tools for C/C++ extensions (cryptography, asyncssh, etc.)
RUN apk add --no-cache build-base libffi-dev openssl-dev curl

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY pyproject.toml uv.lock* ./

RUN uv sync --no-dev --no-install-project --extra database

COPY . .

# Download kubectl with SHA256 checksum verification
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then ARCH="amd64"; elif [ "$ARCH" = "aarch64" ]; then ARCH="arm64"; fi && \
    KUBECTL_VERSION=$(curl -L -s https://dl.k8s.io/release/stable.txt) && \
    curl -LO "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl" && \
    curl -LO "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl.sha256" && \
    echo "$(cat kubectl.sha256)  kubectl" | sha256sum -c - && \
    rm -f kubectl.sha256 && \
    chmod +x kubectl

# ============================================================
# Stage 2: Runtime - minimal production image
# ============================================================
FROM python:3.12-alpine

# Runtime libraries: openssh-client for SSH, kubectl for k8s
RUN apk add --no-cache openssh-client libstdc++ libffi

# Non-root user for security
RUN addgroup -S agent && adduser -S agent -G agent

WORKDIR /app

# Copy the virtual environment and source from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/*.py /app/
COPY --from=builder /app/executors /app/executors/
COPY --from=builder /app/kubectl /usr/local/bin/kubectl

# Create vault and heartbeat directories with proper ownership
RUN mkdir -p /home/agent/.easyalert && \
    chown -R agent:agent /app /home/agent/.easyalert

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV VAULT_PATH=/home/agent/.easyalert/vault.json

# Healthcheck: verify heartbeat file was updated within the last 60 seconds
# Uses the venv python to ensure the correct interpreter is used
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD test -f /home/agent/.easyalert/.agent_heartbeat && /app/.venv/bin/python -c "import os,time;exit(0 if time.time()-os.path.getmtime('/home/agent/.easyalert/.agent_heartbeat')<60 else 1)" || exit 1

USER agent

ENTRYPOINT ["python", "main.py"]
