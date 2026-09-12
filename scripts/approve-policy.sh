#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  printf '%s\n' "usage: $0 'Reviewer name'" >&2
  exit 2
fi
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose="$root/docker-compose.yml"

docker compose -f "$compose" build mcp
docker compose -f "$compose" run --rm --no-deps policy-admin \
  python3 -m scientist_literature_mcp.publisher_policy \
  --config /config/publishers.json --approve --reviewed-by "$1"
