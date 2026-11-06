#!/usr/bin/env bash
# Waits until a replica leads and the expected workers have registered, then prints status.
set -euo pipefail
cd "$(dirname "$0")/.."
expected_workers=$1
for _ in $(seq 1 150); do
  alive=$(docker compose exec -T postgres psql -X -q -tA -U scheduler -d scheduler -c \
    "SELECT count(*) FROM nodes WHERE status = 'ALIVE'" 2>/dev/null || echo 0)
  if scripts/leader.sh >/dev/null 2>&1 && [ "$alive" = "$expected_workers" ]; then
    scripts/cluster-status.sh
    exit 0
  fi
  sleep 0.2
done
echo "the cluster was not ready after 30 seconds" >&2
exit 1
