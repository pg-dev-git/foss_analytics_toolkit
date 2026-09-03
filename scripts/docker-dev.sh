#!/usr/bin/env bash
# Development helper: start dev container and attach

set -euo pipefail

docker compose build dev
docker compose up -d dev
docker compose exec -it dev bash
