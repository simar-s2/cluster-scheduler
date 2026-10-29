package clusterscheduler.controlplane.reconcile;

import clusterscheduler.controlplane.database.Database;
import clusterscheduler.controlplane.delivery.AssignmentSender;
import clusterscheduler.controlplane.leadership.FencedTransactions;
import clusterscheduler.controlplane.metrics.Metrics;
import clusterscheduler.controlplane.state.Assignment;
import clusterscheduler.controlplane.state.AssignmentRepository;
import clusterscheduler.controlplane.state.DeadNodes;
import clusterscheduler.controlplane.state.NodeRepository;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Brings the actual state of the cluster back in line with the desired state,
 * in the style of a Kubernetes controller.
 *
 * Desired state: every SCHEDULED job runs on a live node.
 * Actual state: what heartbeats say (which nodes are alive, which tasks run).
 *
 * Each pass fixes the two ways they drift apart:
 * 1. A node stopped heartbeating. It is declared dead and its jobs go back to
 *    the queue; the scheduling loop then places them on healthy nodes.
 * 2. An assignment was saved but the worker never confirmed it. It is sent
 *    again.
 *
 * Each pass looks at the whole current state rather than reacting to single
 * events, so a missed pass or a lost message is simply fixed by the next one.
 *
 * Declaring nodes dead is a decision, so it runs in a fenced transaction.
 * Without the fence, a replaced leader waking from a pause could declare
 * nodes dead with heartbeat data it read before the pause.
 */
public final class ReconcileLoop {
    private static final Logger log = LoggerFactory.getLogger(ReconcileLoop.class);
    private static final int MAX_RESENDS_PER_PASS = 1_000;

    private final Database database;
    private final FencedTransactions fencedTransactions;
    private final NodeRepository nodeRepository;
    private final AssignmentRepository assignmentRepository;
    private final AssignmentSender assignmentSender;
    private final Metrics metrics;
    private final long heartbeatTimeoutMillis;
    private final long resendAfterMillis;

    public ReconcileLoop(
            Database database,
            FencedTransactions fencedTransactions,
            NodeRepository nodeRepository,
            AssignmentRepository assignmentRepository,
            AssignmentSender assignmentSender,
            Metrics metrics,
            long heartbeatTimeoutMillis,
            long resendAfterMillis) {
        this.database = database;
        this.fencedTransactions = fencedTransactions;
        this.nodeRepository = nodeRepository;
        this.assignmentRepository = assignmentRepository;
        this.assignmentSender = assignmentSender;
        this.metrics = metrics;
        this.heartbeatTimeoutMillis = heartbeatTimeoutMillis;
        this.resendAfterMillis = resendAfterMillis;
    }

    /** Runs one pass as the leader of the given epoch. */
    public void runOnce(long epoch) {
        handleDeadNodes(epoch);
        resendUnconfirmedAssignments(epoch);
    }

    private void handleDeadNodes(long epoch) {
        DeadNodes deadNodes = fencedTransactions.run(
                epoch, connection -> nodeRepository.markExpiredNodesDead(connection, heartbeatTimeoutMillis));
        if (deadNodes.isEmpty()) {
            return;
        }
        metrics.nodesDeclaredDead.increment(deadNodes.nodeIds().size());
        metrics.jobsRequeued.increment(deadNodes.requeuedJobIds().size());
        log.warn("Declared {} node(s) dead and requeued {} job(s): {}",
                deadNodes.nodeIds().size(), deadNodes.requeuedJobIds().size(), deadNodes.nodeIds());
        deadNodes.nodeIds().forEach(assignmentSender::forgetNode);
    }

    private void resendUnconfirmedAssignments(long epoch) {
        List<Assignment> unconfirmed = database.inTransaction(connection ->
                assignmentRepository.findUnconfirmed(connection, resendAfterMillis, MAX_RESENDS_PER_PASS));
        if (!unconfirmed.isEmpty()) {
            metrics.assignmentsResent.increment(unconfirmed.size());
            log.info("Resending {} unconfirmed assignment(s)", unconfirmed.size());
            assignmentSender.sendAll(unconfirmed, epoch);
        }
    }
}
