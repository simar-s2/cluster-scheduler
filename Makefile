WORKERS ?= 5
COUNT   ?= 12

.PHONY: test cluster-up cluster-down cluster-status jobs kill-leader kill-worker chaos node-failure packing results help

test: ## Unit and integration tests (Testcontainers starts PostgreSQL)
	./gradlew test

cluster-up: ## Start 3 control planes, PostgreSQL and the workers (env: WORKERS), and wait until ready
	@echo "building images (the first build takes a few minutes)..."
	@docker compose build --quiet
	@docker compose up -d --wait --scale worker=$(WORKERS) >/dev/null 2>&1
	@scripts/wait-ready.sh $(WORKERS)

cluster-down: ## Stop the cluster and delete its data
	docker compose down -v

cluster-status: ## Show who leads, how full each worker is, and where the jobs are
	@scripts/cluster-status.sh

jobs: ## Submit jobs that run for an hour (env: COUNT)
	@COUNT=$(COUNT) ./gradlew -q :experiments:submitJobs

kill-leader: ## Kill the leader's container; a standby takes over
	@leader=$$(scripts/leader.sh) && docker compose kill $$leader >/dev/null 2>&1 && echo "killed $$leader (the leader); restart it with: docker compose start"

kill-worker: ## Kill the busiest worker's container; its jobs move to other workers
	@scripts/kill-worker.sh

chaos: ## Leader crash test: take the leader down 200 times; saves results/failover-chaos.json
	./gradlew -q :experiments:failoverChaos

node-failure: ## Worker crash test: kill workers, time the recovery; saves results/node-failure.json
	./gradlew -q :experiments:nodeFailure

packing: ## Packing test: 10,000 jobs on 100 workers (env: WORKER_CONTAINERS NODES_PER_CONTAINER); saves results/packing.json
	./gradlew -q :experiments:packing

results: ## Rebuild the README's results tables from results/*.json
	./gradlew -q :experiments:readmeResults

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'
