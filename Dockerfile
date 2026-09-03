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
COPY . .

RUN uv pip install --system -e .[interactive,dev]

# ---- Runtime Stage ----
FROM python:3.12-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    procps \
    npm \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g @salesforce/cli

RUN useradd -m -s /bin/bash tcrm && \
    mkdir -p /home/tcrm/.config /home/tcrm/.local/share && \
    chown -R tcrm:tcrm /home/tcrm

COPY --from=builder /app /app
RUN chown -R tcrm:tcrm /app
WORKDIR /app
RUN pip install -e .

ENV HOME="/home/tcrm"
ENV USER="tcrm"

USER tcrm
WORKDIR /home/tcrm

ENTRYPOINT ["tcrm"]
CMD ["--help"]
