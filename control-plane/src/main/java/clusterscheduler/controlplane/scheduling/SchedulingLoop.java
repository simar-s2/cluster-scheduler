package clusterscheduler.controlplane.scheduling;

import clusterscheduler.controlplane.delivery.AssignmentSender;
import clusterscheduler.controlplane.leadership.FencedTransactions;
import clusterscheduler.controlplane.state.Assignment;
import clusterscheduler.controlplane.state.AssignmentRepository;
import clusterscheduler.controlplane.state.JobRepository;
import clusterscheduler.controlplane.state.NodeRepository;
import clusterscheduler.controlplane.state.NodeSpace;
import clusterscheduler.controlplane.state.PendingJob;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One scheduling pass: read the pending queue and the free space on every
 * node, decide placements, save them, then send them to the workers.
 *
 * Saving comes before sending. If this replica dies between the two, the
 * assignments are already in PostgreSQL and get resent by whoever leads
 * next. The other order could start a task that no one has a record of.
 *
 * The reads are not fenced, but the save is. If this replica stopped being
 * leader while it was deciding, the save is rejected and nothing it decided
 * takes effect.
 */
public final class SchedulingLoop {
    private static final Logger log = LoggerFactory.getLogger(SchedulingLoop.class);

    private final FencedTransactions fencedTransactions;
    private final JobRepository jobRepository;
    private final NodeRepository nodeRepository;
    private final AssignmentRepository assignmentRepository;
    private final AssignmentSender assignmentSender;
    private final BestFitScheduler scheduler = new BestFitScheduler();
    private final int batchSize;

    public SchedulingLoop(
            FencedTransactions fencedTransactions,
            JobRepository jobRepository,
            NodeRepository nodeRepository,
            AssignmentRepository assignmentRepository,
            AssignmentSender assignmentSender,
            int batchSize) {
        this.fencedTransactions = fencedTransactions;
        this.jobRepository = jobRepository;
        this.nodeRepository = nodeRepository;
        this.assignmentRepository = assignmentRepository;
        this.assignmentSender = assignmentSender;
        this.batchSize = batchSize;
    }

    /** Runs one pass as the leader of the given epoch. */
    public void runOnce(long epoch) {
        List<PendingJob> pendingJobs = jobRepository.findPending(batchSize);
        if (pendingJobs.isEmpty()) {
            return;
        }
        List<NodeSpace> nodes = nodeRepository.findNodesWithFreeSpace();
        List<Placement> placements = scheduler.place(pendingJobs, nodes);
        if (placements.isEmpty()) {
            return;
        }
        List<Assignment> newAssignments = placements.stream().map(SchedulingLoop::toAssignment).toList();
        List<Assignment> saved = fencedTransactions.run(
                epoch, connection -> assignmentRepository.saveNew(connection, newAssignments, epoch));
        log.debug("Placed {} of {} pending jobs", saved.size(), pendingJobs.size());
        assignmentSender.sendAll(saved, epoch);
    }

    private static Assignment toAssignment(Placement placement) {
        return new Assignment(
                UUID.randomUUID().toString(),
                placement.job().jobId(),
                placement.node().nodeId(),
                placement.node().address(),
                placement.job().cpuMillis(),
                placement.job().memoryMb(),
                placement.job().durationMillis());
    }
}
