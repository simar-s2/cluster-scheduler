# Design

This file grows by one section per milestone. Each section covers what was built, why it was designed that way, the tradeoffs, and questions an interviewer might ask.

## Terms

- **Job:** what a client submits: a CPU and memory request plus how long the simulated task runs. Each job has exactly one task.
- **Node:** one worker process. It offers a fixed CPU and memory capacity and runs tasks.
- **Assignment:** one placement of a job's task on a node. If a node dies, its jobs get new assignments on other nodes, so a job can have several assignments over its life, but never more than one active at a time.
- **Leader:** the one control plane replica, out of three, that is allowed to make decisions (place tasks, declare nodes dead).
- **Epoch:** a number that goes up by one every time a new leader takes over. It is the fencing token that lets everyone reject messages from a replaced leader.

## Milestone 1: project setup, protobuf contract, PostgreSQL

### What was built

- A Gradle build (Kotlin DSL) with a version catalog in `gradle/libs.versions.toml`. The Java toolchain is pinned to 21, and the foojay resolver downloads JDK 21 if the machine does not have it.
- A `proto` module holding the wire contract in three files under `proto/src/main/proto/clusterscheduler/v1/`:
  - `resources.proto`: the `Resources` message (CPU in millicores, memory in MB).
  - `control_plane.proto`: the `ControlPlane` service (`RegisterNode`, `Heartbeat`, `SubmitJob`).
  - `worker.proto`: the `Worker` service (`AssignTask`).
- `docker-compose.yml` with PostgreSQL 18.6 and a health check, so `docker compose up --wait` returns only once the database accepts connections.

The `control-plane` and `worker` modules arrive in milestone 2 with their first code. Only the control plane will talk to PostgreSQL; the worker will depend on nothing but the `proto` module.

### Who calls whom

```mermaid
flowchart LR
    client(["Job client"])
    subgraph cp["Control plane: 3 replicas"]
        leader["Leader"]
        standby["Standbys"]
    end
    worker["Worker nodes"]
    pg[("PostgreSQL")]

    client -- "SubmitJob" --> cp
    worker -- "RegisterNode, Heartbeat" --> cp
    leader -- "AssignTask" --> worker
    cp --- pg
```

### Why it is designed this way

**Workers call in to report; the leader calls out to assign.** Workers dial the control plane to register and send heartbeats. The leader dials each worker's `AssignTask` to hand it a task. The other option was to return assignments inside heartbeat responses, so workers only ever dial out. I chose push for two reasons. First, a task starts as soon as it is placed instead of waiting up to one heartbeat interval. Second, the spec requires workers to reject assignments from an old leader. That check only means something if the worker receives the request directly from a replica that might be stale. With pull, any replica would read assignments from PostgreSQL, and the worker would have nothing to check.

**Any replica records facts; only the leader makes decisions.** `RegisterNode`, `Heartbeat` and `SubmitJob` only write down what happened (a node joined, a node is alive, a job arrived), so every replica serves them by writing to PostgreSQL. Placing tasks and declaring nodes dead are decisions, and only the leader makes them, in its own loops. As a result, leader failover never stops heartbeats or job submission, and healthy nodes are not declared dead just because the leader was down. It also keeps fencing simple: only the leader's writes need an epoch check.

**Each kind of ID is chosen by whoever can make it unique and safe to retry.**
- The client picks the `job_id`, so resubmitting the same job after a timeout creates one job, not two.
- The control plane picks a new `node_id` on every registration. A worker that restarts has lost its in-memory tasks, so it should not inherit its old identity and the tasks that came with it. Its old node times out and those tasks are rescheduled.
- The leader picks an `assignment_id` for every placement. Workers remember the IDs they have seen, so a repeated `AssignTask` is harmless.

**Heartbeats send the full state, not just changes.** Each heartbeat lists every assignment the worker is running. If one heartbeat is lost, the next one carries the same information, and the reconcile loop always compares against a complete picture. Finished assignments are the one change-based field. The worker keeps sending them until a heartbeat succeeds, and the control plane ignores repeats.

**`must_reregister` handles a node that comes back.** If a node goes quiet long enough to be marked dead, its tasks are moved elsewhere. If it then comes back (for example after a network outage) and sends a heartbeat, the response tells it to stop every task and register as a new node. Without this it would keep running tasks that now also run somewhere else.

**`leader_epoch` is the sender's epoch, not the epoch the assignment was created in.** A new leader may resend an assignment created by the previous leader, for example when the old leader died after saving it but before delivering it. The request carries the new leader's own epoch, which tells the worker that the sender is the current leader.

**Small details.**
- CPU is in millicores and memory in MB, as integers, so the scheduler's arithmetic is exact.
- protoc is pinned to 3.25.9 because that is the protobuf runtime version grpc-java 1.85 depends on. Generated code must not be newer than the runtime it runs on.
- PostgreSQL's port is bound to 127.0.0.1, because the credentials in the compose file are for local development only.

### Tradeoffs

- **Push needs the leader to reach every worker.** Each worker sends its address at registration. That is trivial inside one Docker network, but workers behind NAT or a firewall would need the pull model.
- **PostgreSQL takes every heartbeat write.** 100 nodes sending one heartbeat per second is about 100 small writes per second, which is nothing for PostgreSQL. At tens of thousands of nodes you would batch heartbeats or keep them out of the database.
- **PostgreSQL is the single source of truth and the coordinator.** That gives strongly consistent state and transactions with almost no code, and leader election can reuse it instead of adding etcd or ZooKeeper. The cost is that PostgreSQL itself is a single point of failure in this setup. If it is down, no replica can make progress, which is the safe outcome: no leader can act without it.
- **The whole contract is defined up front.** Some fields (`leader_epoch`, `must_reregister`) are only used from later milestones. Protobuf lets fields be added later without breaking anything, but writing the contract first made the design easier to review as a whole.

### Interview questions

**Why does the leader push assignments to workers instead of returning them in heartbeat responses?**
Pushing starts tasks immediately instead of waiting for the next heartbeat, and it makes the worker-side fencing check meaningful: the worker sees the sender's epoch and can refuse a stale leader. The cost is that the leader must be able to reach every worker, which pull would avoid.

**If only one replica is the leader, why can standbys handle heartbeats and job submissions?**
Those calls record facts, not decisions, and all state lives in PostgreSQL, so any replica can write them. This keeps heartbeats flowing during a failover, so the new leader does not wrongly declare healthy nodes dead because nobody was listening while there was no leader.

**Why does a restarted worker get a new node ID instead of keeping its old one?**
A restart wipes the worker's in-memory tasks. If it kept its ID, the control plane would believe the old tasks were still running there. With a fresh ID, the old node simply times out like any dead node and its tasks are rescheduled through the normal path, with no special case.
