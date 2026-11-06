#!/usr/bin/env bash
# Prints the name of the replica holding an unexpired lease.
set -euo pipefail
cd "$(dirname "$0")/.."
leader=$(docker compose exec -T postgres psql -X -q -tA -U scheduler -d scheduler -c \
  "SELECT holder FROM leader_lease WHERE expires_at > now()")
if [ -z "$leader" ]; then
  echo "no leader right now" >&2
  exit 1
fi
echo "$leader"
