package clusterscheduler.controlplane.scheduling;

import clusterscheduler.controlplane.state.NodeSpace;
import clusterscheduler.controlplane.state.PendingJob;

/** The scheduler's decision to run one job on one node. */
public record Placement(PendingJob job, NodeSpace node) {}
