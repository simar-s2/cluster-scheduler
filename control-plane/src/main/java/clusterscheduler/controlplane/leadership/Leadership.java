package clusterscheduler.controlplane.leadership;

import java.util.OptionalLong;
import java.util.function.LongSupplier;

/**
 * This replica's own view of whether it leads, and under which epoch.
 *
 * The view expires on its own. Each successful renewal moves the deadline to
 * "when the renewal was sent + lease duration". The database sets its expiry
 * from the moment it ran the UPDATE, which is later, so this replica stops
 * acting before the database lets anyone else take over.
 *
 * That timing only reduces wasted work. Safety does not depend on it: a
 * replica that is wrong about leading is stopped by the epoch check on its
 * writes (see FencedTransactions).
 */
public final class Leadership {
    private static final long NOT_LEADER = 0;

    private final LongSupplier monotonicNanos;
    private long epoch = NOT_LEADER;
    private long validUntilNanos;

    public Leadership() {
        this(System::nanoTime);
    }

    Leadership(LongSupplier monotonicNanos) {
        this.monotonicNanos = monotonicNanos;
    }

    /** The epoch to act under right now, or empty if this replica must not make decisions. */
    public synchronized OptionalLong currentEpoch() {
        if (epoch == NOT_LEADER || monotonicNanos.getAsLong() >= validUntilNanos) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(epoch);
    }

    public boolean isLeader() {
        return currentEpoch().isPresent();
    }

    /** The epoch this replica believes it holds, even if its deadline has passed. Zero if none. */
    synchronized long heldEpoch() {
        return epoch;
    }

    synchronized void becomeLeader(long newEpoch, long validUntilNanos) {
        this.epoch = newEpoch;
        this.validUntilNanos = validUntilNanos;
    }

    synchronized void extend(long renewedEpoch, long validUntilNanos) {
        if (renewedEpoch == epoch) {
            this.validUntilNanos = validUntilNanos;
        }
    }

    /**
     * Stops leading, but only if still in the given term. A late failure from
     * an old term must not end a newer one.
     */
    public synchronized void stepDown(long endedEpoch) {
        if (endedEpoch == epoch) {
            epoch = NOT_LEADER;
        }
    }
}
