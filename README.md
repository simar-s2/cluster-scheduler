# cluster-scheduler

**A distributed cluster scheduler in Java: a leader-elected control plane packs jobs onto worker nodes, keeps the cluster in its desired state, and uses fencing tokens so a task is never placed on two nodes, even during failover.**

## What I built

Clients submit jobs, each with a CPU and memory request. Three control plane replicas share all their state through PostgreSQL. One of them leads at a time, chosen by a lease in the database, and only the leader makes decisions: it packs pending jobs onto worker nodes with best-fit bin packing and pushes each placement to its worker over gRPC. A Kubernetes-style reconcile loop compares desired state (the jobs table) with actual state (assignments plus heartbeats). When a node goes quiet it is declared dead, and its jobs go back in the queue to be placed elsewhere.

The hard part is failover. A leader that freezes (a long GC pause, a stopped container) can wake up after a standby has taken over, still believing it leads. Every leader term has an epoch number that serves as a fencing token. PostgreSQL rejects writes from an old epoch, and workers reject assignments from an old epoch, so a replaced leader cannot change anything.

- **Correctness tested under failure.** A chaos experiment takes the leader down 200 times, alternating SIGKILL and freezing the container, while jobs keep arriving. It then counts double placements in two independent places: the assignment history in PostgreSQL, and the execution logs the workers wrote about what they actually ran.
- **Failure recovery measured.** An experiment kills worker containers and times how long until every task that was on them runs on another node.
- **Packing measured.** A benchmark submits 10,000 mixed-size jobs to 100 nodes and samples CPU utilization (allocated CPU / total CPU capacity) every second.
- **Every number is generated.** The experiments write `results/*.json`, and the [results table](#results) below is rebuilt from those files by a program. None of it is typed by hand.

**Tech:** Java 21, Gradle, gRPC and Protocol Buffers, PostgreSQL 18 (plain JDBC with HikariCP), Docker Compose, JUnit 5 and Testcontainers, Micrometer with a Prometheus endpoint.

## Architecture

```mermaid
flowchart LR
    client(["Job client"])

    subgraph cp["Control plane: 3 replicas"]
        direction TB
        leader["Leader<br/>scheduling loop<br/>reconcile loop"]
        standby1["Standby"]
        standby2["Standby"]
    end

    subgraph pg["PostgreSQL"]
        direction TB
        lease[("leader_lease<br/>holder, epoch, expiry")]
        state[("jobs · nodes · assignments")]
    end

    subgraph workers["Worker nodes"]
        direction TB
        w1["Worker"]
        w2["Worker"]
        w3["Worker ..."]
    end

    client -- "SubmitJob (any replica)" --> cp
    workers -- "RegisterNode, Heartbeat (any replica)" --> cp
    leader -- "AssignTask + epoch" --> workers
    cp -- "lease renew / take over" --> lease
    cp -- "fenced writes" --> state
```

- **Any replica accepts facts.** Job submissions, registrations and heartbeats are written to PostgreSQL by whichever replica receives them. That keeps them flowing while there is no leader, so a failover never makes healthy nodes look dead.
- **Only the leader makes decisions,** on a single thread: placing jobs and declaring nodes dead. Each decision commits in a transaction that first locks the lease row and checks the epoch.
- **The leader pushes assignments** to workers over gRPC, stamped with its epoch. A worker refuses an epoch older than one it has seen, an assignment meant for another node ID, and an assignment ID it has already handled.

A job's life, from submission to completion:

```mermaid
sequenceDiagram
    participant C as Client
    participant R as Any replica
    participant DB as PostgreSQL
    participant L as Leader
    participant W as Worker

    C->>R: SubmitJob(job_id, cpu, memory)
    R->>DB: INSERT job (PENDING), idempotent on job_id
    loop every 100 ms
        L->>DB: read pending jobs and free space per node
        L->>L: best fit: tightest node that has room
        L->>DB: BEGIN; check epoch FOR SHARE; job PENDING to SCHEDULED; insert assignment; COMMIT
        L->>W: AssignTask(assignment_id, node_id, epoch)
        W->>W: check epoch, node ID, dedupe; start task
    end
    loop every second
        W->>R: Heartbeat(free space, running IDs, finished IDs)
        R->>DB: assignment RUNNING / FINISHED, job FINISHED
    end
```

[DESIGN.md](DESIGN.md) explains every part milestone by milestone: what was built, why, the tradeoffs, and the questions an interviewer might ask.

## How it stays correct

"A task is never placed on two nodes" does not rest on one mechanism. Each layer below would catch most mistakes on its own:

1. **One decision thread per replica.** A leader never races itself.
2. **Claiming a job is a compare-and-set.** `UPDATE jobs SET state = 'SCHEDULED' WHERE job_id = ? AND state = 'PENDING'` succeeds once per job.
3. **A partial unique index** in PostgreSQL allows at most one active assignment per job, whatever the code does.
4. **Fenced transactions.** Each leader transaction starts with `SELECT epoch FROM leader_lease FOR SHARE`. A stale epoch aborts it. The shared lock makes a takeover wait until in-flight writes commit, so leader terms never overlap in the database.
5. **Worker epoch check.** An RPC is not a database write, so workers check the token themselves.
6. **Dedupe by assignment ID.** Retries and resends are harmless.
7. **Node ID check.** A new worker that inherits a dead worker's IP address does not run the dead worker's tasks.

A frozen leader that holds the lease lock cannot block a takeover forever, because PostgreSQL ends any session that sits idle inside a transaction for 2 seconds.

## Results

Measured on one MacBook (10 cores), with everything in Docker Desktop. The table is generated from [results/](results/) by `./gradlew :experiments:readmeResults`.

<!-- results:start -->
| Measurement | Result | From |
|---|---|---|
| Leader failovers (SIGKILL / pause) | 200 (100 / 100) | [failover-chaos.json](results/failover-chaos.json) |
| Time until a new leader holds the lease, median / p99 / max | 1,769 ms / 2,367 ms / 2,449 ms | [failover-chaos.json](results/failover-chaos.json) |
| Jobs submitted during the failovers / finished | 15,098 / 15,098 | [failover-chaos.json](results/failover-chaos.json) |
| **Double placements:** jobs with overlapping active assignments (database) | **0** | [failover-chaos.json](results/failover-chaos.json) |
| **Double placements:** jobs run on two nodes at once (worker logs) | **0** | [failover-chaos.json](results/failover-chaos.json) |
| Jobs ever assigned to more than one node / assignments started twice | 0 / 0 | [failover-chaos.json](results/failover-chaos.json) |
| Stale-leader writes rejected by the epoch check / frozen sessions ended by PostgreSQL | 1 / 0 | [failover-chaos.json](results/failover-chaos.json) |
| Assignments workers rejected for a stale epoch | 0 | [failover-chaos.json](results/failover-chaos.json) |
| Worker killed: time until all its tasks run elsewhere, median / max (5 trials) | 5,351 ms / 5,664 ms | [node-failure.json](results/node-failure.json) |
| Worker killed: time until it is declared dead, median | 4,964 ms | [node-failure.json](results/node-failure.json) |
| **CPU utilization** (allocated / capacity) while jobs were waiting, mean / min | **97.5%** / 90.4% | [packing.json](results/packing.json) |
| Memory utilization while jobs were waiting, mean | 92.3% | [packing.json](results/packing.json) |
| 10,000 jobs on 100 nodes: time until all finished / jobs per second | 388 s / 25.8 | [packing.json](results/packing.json) |
| Mean scheduling pass on the leader (up to 1,000 jobs x 100 nodes) | 5.4 ms | [packing.json](results/packing.json) |
| Memory per worker container (one node, small heap), mean / max | 41 MiB / 46 MiB | [packing.json](results/packing.json) |
<!-- results:end -->

How each number is measured:

- **Failover chaos** (`FailoverChaosExperiment`): 3 replicas and 10 workers (4 cores and 8 GB each), with 20 jobs per second of 0.1 to 1 core and 1 to 5 seconds. Faults alternate between `docker compose kill -s SIGKILL` and `docker compose pause` on whichever replica holds the lease. The next fault waits until a replica holds an unexpired lease with a higher epoch. The killed or paused replica is then started or unpaused again. "Time until a new leader" runs from when Docker reports the fault in place until that new lease is visible (polled every 50 ms). After the last fault, the experiment waits for every accepted job to finish, then checks:
  - the assignment history for any job with two assignments active at overlapping times;
  - the workers' execution logs for any job that ran on two nodes at overlapping times.
  
  The worker-log checker has unit tests showing it does detect such overlaps.
- **Node failure** (`NodeFailureExperiment`): 10 workers half filled with 40 long-running jobs. Each trial SIGKILLs the container of the busiest node and times, from when Docker reports it killed, (a) until the node is marked dead and (b) until every one of its tasks is confirmed `RUNNING` on another node. Defaults: a heartbeat every 1 s, dead after 5 s of silence, reconcile every 1 s.
- **Packing** (`PackingBenchmark`): 100 nodes of 4 cores and 8 GB. 10,000 jobs are submitted at once, with CPU of 0.25 to 2 cores, memory of 256 MB to 4 GB and durations of 5 to 20 s, drawn from a fixed seed. Utilization is allocated CPU / total CPU capacity of live nodes, where allocated means the sum of the CPU requests of all `ASSIGNED` or `RUNNING` assignments. It is sampled every second, and only samples taken while at least one job was still waiting count, because then any idle CPU is space the scheduler could not use. An allocation lasts from the moment the leader saves it until a heartbeat reports the task finished, so it reads higher than the CPU the workers themselves report in use. That second figure lags by up to one heartbeat and is also recorded in `packing.json`.

**100 nodes, 100 containers.** Every node in the packing benchmark is its own worker container with its own JVM. To fit 100 JVMs into Docker Desktop's VM (under 8 GB on this laptop), workers run with a small heap and few threads; see `JAVA_OPTS` in `docker-compose.yml`. The table shows the measured memory per container, and even so the VM used swap during the run. On a smaller machine, `NODES_PER_PROCESS=N` runs N independent nodes in one container, each with its own gRPC server, registration, heartbeats, task runner and fencing state. The results above do not use it.

## Quick start

Requires Docker. Gradle downloads JDK 21 itself if needed.

```sh
./gradlew test                                        # unit and integration tests (Testcontainers starts PostgreSQL)
docker compose up -d --build --wait --scale worker=5  # 3 replicas, PostgreSQL, 5 workers
```

Submit a job with [grpcurl](https://github.com/fullstorydev/grpcurl), to any replica (ports 9091 to 9093):

```sh
grpcurl -plaintext -import-path proto/src/main/proto -proto clusterscheduler/v1/control_plane.proto \
  -d '{"job_id": "hello", "requested": {"cpu_millis": 500, "memory_mb": 256}, "duration_millis": 10000}' \
  localhost:9091 clusterscheduler.v1.ControlPlane/SubmitJob
```

See who leads and what runs where:

```sh
docker compose exec postgres psql -U scheduler -c "SELECT holder, epoch, expires_at FROM leader_lease"
docker compose exec postgres psql -U scheduler -c "SELECT job_id, node_id, epoch, state FROM assignments"
curl -s localhost:8081/metrics | grep ^scheduler_   # replica 1; 8082 and 8083 for the others
```

Kill the leader and watch a standby take over with a higher epoch:

```sh
docker compose kill control-plane-1   # or whichever replica the lease names
docker compose down -v                # stop everything and delete the data
```

Run the experiments. Each one starts its own fresh cluster, and writes `results/*.json` when it finishes:

```sh
./gradlew :experiments:failoverChaos    # 200 leader failovers
./gradlew :experiments:nodeFailure      # kill workers, time the recovery
./gradlew :experiments:packing          # 10,000 jobs on 100 nodes
./gradlew :experiments:readmeResults    # rebuild the table above from results/
```

## Project layout

```
proto/          the gRPC contract: ControlPlane and Worker services
control-plane/  replica: API, lease election, fencing, scheduler, reconcile loop, metrics
  database/       connection pool, transactions, schema setup
  state/          SQL for nodes, jobs, assignments and heartbeats
  leadership/     lease, local leadership view, fenced transactions
  scheduling/     best-fit bin packing and the scheduling loop
  reconcile/      dead-node handling and resending unconfirmed assignments
  delivery/       pushing assignments to workers
  metrics/        Micrometer counters and the /metrics endpoint
worker/         node: registration, heartbeats, simulated tasks, epoch fence, dedupe
experiments/    chaos, node failure and packing experiments, and the README table generator
results/        the JSON files those experiments wrote
```

## Known limits

- **PostgreSQL is a single point of failure.** Nothing can happen without it, which is also why leader election lives there.
- **A worker cut off from the control plane keeps running its tasks** until it reconnects and is told it was declared dead. Self-fencing (a worker stops its own tasks after missing heartbeats for too long) would close that window.
- **There are no schema migrations;** every run starts from an empty database.
- **Tasks are simulated:** they hold their CPU and memory for their duration and do no work.

## License

MIT
