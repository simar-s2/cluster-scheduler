package clusterscheduler.controlplane.metrics;

import clusterscheduler.controlplane.leadership.Leadership;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * Every metric the control plane reports, in one place. Components get this
 * object and update the fields directly, so finding where a number comes
 * from is a single search.
 */
public final class Metrics {
    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    public final Counter jobsSubmitted = counter("scheduler_jobs_submitted", "SubmitJob calls accepted");
    public final Counter heartbeatsReceived = counter("scheduler_heartbeats_received", "Heartbeats recorded");
    public final Counter placementsSaved = counter("scheduler_placements_saved", "Placements saved by the leader");
    public final Counter assignmentsResent = counter("scheduler_assignments_resent", "Unconfirmed assignments sent again");
    public final Counter nodesDeclaredDead = counter("scheduler_nodes_declared_dead", "Nodes that missed heartbeats");
    public final Counter jobsRequeued = counter("scheduler_jobs_requeued", "Jobs put back in the queue after a node died");
    public final Counter fencedWritesRejected =
            counter("scheduler_fenced_writes_rejected", "Leader writes refused because the epoch was stale");
    public final Timer schedulingPass = Timer.builder("scheduler_scheduling_pass")
            .description("Time for one scheduling pass on the leader that had pending jobs")
            .register(registry);

    /** Reports whether this replica leads, and under which epoch, read fresh at every scrape. */
    public void watchLeadership(Leadership leadership) {
        Gauge.builder("scheduler_is_leader", leadership, current -> current.isLeader() ? 1 : 0)
                .description("1 on the leader, 0 on standbys")
                .register(registry);
        Gauge.builder("scheduler_leader_epoch", leadership, current -> current.currentEpoch().orElse(0))
                .description("The epoch this replica leads under, 0 on standbys")
                .register(registry);
    }

    /** The Prometheus text format, served at /metrics. */
    public String scrape() {
        return registry.scrape();
    }

    private Counter counter(String name, String description) {
        return Counter.builder(name).description(description).register(registry);
    }
}
