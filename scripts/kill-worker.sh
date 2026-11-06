#!/usr/bin/env bash
# Kills the worker container running the most jobs, to watch its jobs move.
set -euo pipefail
cd "$(dirname "$0")/.."
busiest=$(docker compose exec -T postgres psql -X -q -tA -F ' ' -U scheduler -d scheduler -c "
  SELECT n.node_id, split_part(n.address, ':', 1), count(a.assignment_id)
  FROM nodes n
  LEFT JOIN assignments a ON a.node_id = n.node_id AND a.state IN ('ASSIGNED', 'RUNNING')
  WHERE n.status = 'ALIVE'
  GROUP BY n.node_id
  ORDER BY count(a.assignment_id) DESC, n.node_id
  LIMIT 1")
read -r node_id ip jobs <<EOF
$busiest
EOF
for container in $(docker compose ps -q worker); do
  container_ip=$(docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container")
  if [ "$container_ip" = "$ip" ]; then
    docker kill "$container" >/dev/null
    echo "killed worker ${node_id:0:8}, which was running $jobs jobs"
    exit 0
  fi
done
echo "no worker container has IP $ip" >&2
exit 1
