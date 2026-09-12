#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="$root/docker-compose.yml"

docker compose -f "$compose" config --quiet
docker compose -f "$compose" build mcp browser
docker compose -f "$compose" run --rm --no-deps mcp \
  python3 -m scientist_literature_mcp.publisher_policy \
  --config /config/publishers.json
docker compose -f "$compose" up -d flaresolverr browser monitor
docker compose -f "$compose" ps
