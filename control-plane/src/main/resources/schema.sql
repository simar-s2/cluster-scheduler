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
    -- ALIVE until the leader sees no heartbeat for longer than the timeout.
    -- A DEAD node never comes back; its worker registers as a new node.
    status                   TEXT        NOT NULL DEFAULT 'ALIVE'
                                         CHECK (status IN ('ALIVE', 'DEAD')),
    registered_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Database time, never a replica's clock, so clock skew between replicas
    -- cannot make a node look dead.
    last_heartbeat_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    marked_dead_at           TIMESTAMPTZ
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

-- Every placement of a job on a node, kept forever as history. ASSIGNED means
-- the leader saved it, RUNNING means the node confirmed it in a heartbeat, and
-- LOST means the node died first, so the job went back to the queue.
CREATE TABLE IF NOT EXISTS assignments (
    assignment_id TEXT        PRIMARY KEY,
    job_id        TEXT        NOT NULL REFERENCES jobs (job_id),
    node_id       TEXT        NOT NULL REFERENCES nodes (node_id),
    state         TEXT        NOT NULL DEFAULT 'ASSIGNED'
                              CHECK (state IN ('ASSIGNED', 'RUNNING', 'FINISHED', 'LOST')),
    assigned_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    running_at    TIMESTAMPTZ,
    ended_at      TIMESTAMPTZ
);

-- The database itself refuses a second active assignment for the same job,
-- whatever bug or race tries to create one.
CREATE UNIQUE INDEX IF NOT EXISTS one_active_assignment_per_job
    ON assignments (job_id) WHERE state IN ('ASSIGNED', 'RUNNING');

-- Used to add up what is placed on each node.
CREATE INDEX IF NOT EXISTS active_assignments_by_node
    ON assignments (node_id) WHERE state IN ('ASSIGNED', 'RUNNING');

-- Assignments no worker has confirmed yet, which the reconcile loop resends.
CREATE INDEX IF NOT EXISTS unconfirmed_assignments
    ON assignments (assigned_at) WHERE state = 'ASSIGNED';
