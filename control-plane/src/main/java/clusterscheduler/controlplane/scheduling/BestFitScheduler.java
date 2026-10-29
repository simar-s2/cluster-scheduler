package clusterscheduler.controlplane.scheduling;

import clusterscheduler.controlplane.state.NodeSpace;
import clusterscheduler.controlplane.state.PendingJob;
import java.util.ArrayList;
import java.util.List;

/**
 * Bin packing with best fit on two dimensions, CPU and memory.
 *
 * Jobs are taken in queue order. Each one goes to the node where it fits
 * most tightly, meaning the node that would have the least space left over
 * afterwards. Packing small gaps first keeps large gaps free for large jobs.
 *
 * This class only does arithmetic. It reads nothing and writes nothing, which
 * makes it easy to test.
 */
public final class BestFitScheduler {

    /**
     * Places as many of the jobs as fit. A job that fits on no node is left
     * out and stays pending; later, smaller jobs may still be placed.
     */
    public List<Placement> place(List<PendingJob> jobsInQueueOrder, List<NodeSpace> nodes) {
        List<FreeSpace> freeSpaces = nodes.stream().map(FreeSpace::new).toList();
        List<Placement> placements = new ArrayList<>();
        for (PendingJob job : jobsInQueueOrder) {
            FreeSpace tightest = findTightestFit(job, freeSpaces);
            if (tightest != null) {
                tightest.take(job);
                placements.add(new Placement(job, tightest.node));
            }
        }
        return placements;
    }

    /** Returns the node with the smallest leftover score, or null if the job fits nowhere. */
    private static FreeSpace findTightestFit(PendingJob job, List<FreeSpace> freeSpaces) {
        FreeSpace tightest = null;
        double tightestLeftover = Double.MAX_VALUE;
        for (FreeSpace candidate : freeSpaces) {
            if (!candidate.fits(job)) {
                continue;
            }
            double leftover = candidate.leftoverScoreAfter(job);
            // Strictly smaller, so ties go to the first node in the list.
            // The list is sorted by node ID, which keeps decisions repeatable.
            if (leftover < tightestLeftover) {
                tightest = candidate;
                tightestLeftover = leftover;
            }
        }
        return tightest;
    }

    /** The free space on one node, updated as this scheduling pass places jobs on it. */
    private static final class FreeSpace {
        private final NodeSpace node;
        private long freeCpuMillis;
        private long freeMemoryMb;

        private FreeSpace(NodeSpace node) {
            this.node = node;
            this.freeCpuMillis = node.freeCpuMillis();
            this.freeMemoryMb = node.freeMemoryMb();
        }

        private boolean fits(PendingJob job) {
            return job.cpuMillis() <= freeCpuMillis && job.memoryMb() <= freeMemoryMb;
        }

        /**
         * How much would be left, as a share of the node's capacity, summed
         * over CPU and memory. Using shares makes a millicore and a megabyte
         * comparable. 0 means the job fills the node exactly.
         */
        private double leftoverScoreAfter(PendingJob job) {
            double cpuLeftShare = (double) (freeCpuMillis - job.cpuMillis()) / node.cpuCapacityMillis();
            double memoryLeftShare = (double) (freeMemoryMb - job.memoryMb()) / node.memoryCapacityMb();
            return cpuLeftShare + memoryLeftShare;
        }

        private void take(PendingJob job) {
            freeCpuMillis -= job.cpuMillis();
            freeMemoryMb -= job.memoryMb();
        }
    }
}
