# =============================================================================
# CRMA Toolkit - Interactive TUI Docker Image
# Multi-stage build for minimal production image
# =============================================================================

# ---- Build Stage ----
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock* ./

RUN uv sync --extra interactive --extra dev --frozen || uv sync --extra interactive --extra dev

COPY . .

RUN uv pip install -e .

# ---- Runtime Stage ----
FROM python:3.12-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    procps \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://developer.salesforce.com/media/salesforce-cli/salesforce-cli-linux-x64.tar.xz | tar -xJ -C /usr/local/bin --strip-components=1

RUN useradd -m -s /bin/bash tcrm && \
    mkdir -p /home/tcrm/.config /home/tcrm/.local/share && \
    chown -R tcrm:tcrm /home/tcrm

COPY --from=builder /app /app
COPY --from=builder /root/.local /home/tcrm/.local

ENV PATH="/home/tcrm/.local/bin:${PATH}"
ENV HOME="/home/tcrm"
ENV USER="tcrm"

USER tcrm
WORKDIR /home/tcrm

ENTRYPOINT ["tcrm"]
CMD ["--help"]
