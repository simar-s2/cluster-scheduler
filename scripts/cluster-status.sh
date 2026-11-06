#!/usr/bin/env bash
# Prints who leads, how full each live worker is, and where the jobs are.
# Everything comes from PostgreSQL, the same place the leader reads it from.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose exec -T postgres psql -X -q -tA -U scheduler -d scheduler <<'SQL'
SELECT 'leader    ' || COALESCE(
    (SELECT holder || ', epoch ' || epoch FROM leader_lease WHERE expires_at > now()),
    'none right now (last epoch ' || (SELECT epoch FROM leader_lease) || ')');
SELECT '';
SELECT format('%-8s  %-25s  %s', 'worker', 'CPU allocated', 'jobs');
SELECT format('%-8s  %s%s %3s%%  %4s',
        left(node_id, 8),
        repeat('█', round(20.0 * used / capacity)::int),
        repeat('░', 20 - round(20.0 * used / capacity)::int),
        round(100.0 * used / capacity)::int,
        jobs)
FROM (
    SELECT n.node_id,
           n.cpu_capacity_millis AS capacity,
           COALESCE(SUM(j.cpu_millis), 0) AS used,
           COUNT(a.assignment_id) AS jobs
    FROM nodes n
    LEFT JOIN assignments a ON a.node_id = n.node_id AND a.state IN ('ASSIGNED', 'RUNNING')
    LEFT JOIN jobs j ON j.job_id = a.job_id
    WHERE n.status = 'ALIVE'
    GROUP BY n.node_id
) AS workers
ORDER BY node_id;
SELECT '';
SELECT 'workers   ' || count(*) FILTER (WHERE status = 'ALIVE') || ' alive, '
    || count(*) FILTER (WHERE status = 'DEAD') || ' dead' FROM nodes;
SELECT 'jobs      ' || count(*) FILTER (WHERE state = 'SCHEDULED') || ' placed, '
    || count(*) FILTER (WHERE state = 'PENDING') || ' waiting, '
    || count(*) FILTER (WHERE state = 'FINISHED') || ' finished' FROM jobs;
SELECT 'on two workers at once: ' || count(*) FROM (
    SELECT job_id FROM assignments
    WHERE state IN ('ASSIGNED', 'RUNNING')
    GROUP BY job_id HAVING count(*) > 1
) AS doubled;
SQL
