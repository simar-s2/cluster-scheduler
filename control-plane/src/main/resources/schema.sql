-- The whole cluster state. Every control plane replica applies this file at
-- startup; every statement is safe to run more than once.

-- One row per worker registration. A restarted worker registers again and
-- gets a new row.
CREATE TABLE IF NOT EXISTS nodes (
    node_id                  TEXT        PRIMARY KEY,
    address                  TEXT        NOT NULL,
    cpu_capacity_millis      BIGINT      NOT NULL CHECK (cpu_capacity_millis > 0),
    memory_capacity_mb       BIGINT      NOT NULL CHECK (memory_capacity_mb > 0),
    -- What the worker said in its latest heartbeat.
    reported_free_cpu_millis BIGINT      NOT NULL,
    reported_free_memory_mb  BIGINT      NOT NULL,
    registered_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Database time, never a replica's clock, so clock skew between replicas
    -- cannot make a node look dead.
    last_heartbeat_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Desired state: every job a client has submitted. A PENDING job is waiting
-- in the queue, a SCHEDULED job has an active assignment, and a FINISHED job
-- is done.
CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT        PRIMARY KEY,
    cpu_millis      BIGINT      NOT NULL CHECK (cpu_millis > 0),
    memory_mb       BIGINT      NOT NULL CHECK (memory_mb > 0),
    duration_millis BIGINT      NOT NULL CHECK (duration_millis >= 0),
    state           TEXT        NOT NULL DEFAULT 'PENDING'
                                CHECK (state IN ('PENDING', 'SCHEDULED', 'FINISHED')),
    submitted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

-- The pending queue, oldest first. Partial, so finished jobs do not bloat it.
CREATE INDEX IF NOT EXISTS pending_jobs_in_queue_order
    ON jobs (submitted_at, job_id) WHERE state = 'PENDING';
