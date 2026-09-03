#!/usr/bin/env bash
# Run TUI in production container

set -euo pipefail

docker compose run --rm prod "$@"
