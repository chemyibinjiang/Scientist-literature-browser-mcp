#!/bin/sh
set -eu

name=${1:-scientist-literature}
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="$root/docker-compose.yml"

codex mcp add "$name" -- \
  docker compose -f "$compose" run --rm --no-deps -T mcp
printf '%s\n' "Registered MCP server '$name'. Restart Codex, then inspect it with /mcp."
printf '%s\n' "For long paper reads, apply codex.config.example.toml."
