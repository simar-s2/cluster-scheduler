# cluster-scheduler

**A distributed cluster scheduler in Java: a leader-elected control plane packs jobs onto worker nodes, keeps the cluster in its desired state, and uses fencing tokens so a task is never placed on two nodes, even during failover.**

> Work in progress. The full README, with the architecture, how to run it and measured results, comes with the last milestone. See [DESIGN.md](DESIGN.md) for the design so far.

## Build

Requires Docker. Gradle downloads JDK 21 itself if it is not installed.

```sh
./gradlew build                      # compile the protobuf contract
docker compose up -d --wait postgres # start PostgreSQL
docker compose down -v               # stop it and delete its data
```

## License

MIT
