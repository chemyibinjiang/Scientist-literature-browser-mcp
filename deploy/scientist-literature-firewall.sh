#!/usr/bin/env bash
set -euo pipefail

chain="SCIENTIST-LITERATURE"
allowed_source="${SCIENTIST_LITERATURE_ALLOWED_SOURCE:?allowed source is required}"
host_port="${SCIENTIST_LITERATURE_HOST_PORT:-19030}"

iptables -w -N "$chain" 2>/dev/null || true
iptables -w -F "$chain"
iptables -w -A "$chain" -m conntrack --ctstate ESTABLISHED,RELATED -j RETURN
iptables -w -A "$chain" -s "$allowed_source" -p tcp \
  -m conntrack --ctorigdstport "$host_port" -j RETURN
iptables -w -A "$chain" -p tcp \
  -m conntrack --ctorigdstport "$host_port" -j DROP
iptables -w -A "$chain" -j RETURN

iptables -w -C DOCKER-USER -j "$chain" 2>/dev/null || \
  iptables -w -I DOCKER-USER 1 -j "$chain"
