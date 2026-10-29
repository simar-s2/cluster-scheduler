package clusterscheduler.controlplane.leadership;

import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs on every replica. On each tick the leader renews its lease, and a
 * standby tries to take the lease if it has expired.
 *
 * Renewing every renewInterval with a lease of leaseDuration means a leader
 * can miss (leaseDuration / renewInterval - 1) renewals in a row, for example
 * during a slow database call, before it loses the lease.
 */
public final class LeaderElector implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(LeaderElector.class);

    private final String replicaId;
    private final LeaseRepository leaseRepository;
    private final Leadership leadership;
    private final long leaseDurationMillis;
    private final long renewIntervalMillis;
    private final ScheduledExecutorService timer =
            Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "leader-election"));

    public LeaderElector(
            String replicaId,
            LeaseRepository leaseRepository,
            Leadership leadership,
            long leaseDurationMillis,
            long renewIntervalMillis) {
        this.replicaId = replicaId;
        this.leaseRepository = leaseRepository;
        this.leadership = leadership;
        this.leaseDurationMillis = leaseDurationMillis;
        this.renewIntervalMillis = renewIntervalMillis;
    }

    public void start() {
        timer.scheduleWithFixedDelay(this::tickAndLogFailures, 0, renewIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private void tickAndLogFailures() {
        try {
            tick();
        } catch (RuntimeException failure) {
            // The database may be unreachable. Do nothing: if this replica is
            // the leader, its local deadline passes and it stops acting.
            log.warn("Leader election tick failed: {}", failure.getMessage());
        }
    }

    private void tick() {
        // Read the clock before the database call. The lease in the database
        // is at least this fresh, so a deadline counted from here is safe.
        long sentAtNanos = System.nanoTime();
        long validUntilNanos = sentAtNanos + TimeUnit.MILLISECONDS.toNanos(leaseDurationMillis);

        long heldEpoch = leadership.heldEpoch();
        if (heldEpoch != 0) {
            if (leaseRepository.renew(replicaId, heldEpoch, leaseDurationMillis)) {
                leadership.extend(heldEpoch, validUntilNanos);
            } else {
                log.warn("Replica {} lost the lease for epoch {}", replicaId, heldEpoch);
                leadership.stepDown(heldEpoch);
            }
            return;
        }

        OptionalLong newEpoch = leaseRepository.tryAcquire(replicaId, leaseDurationMillis);
        if (newEpoch.isPresent()) {
            leadership.becomeLeader(newEpoch.getAsLong(), validUntilNanos);
            log.info("Replica {} is now the leader with epoch {}", replicaId, newEpoch.getAsLong());
        }
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
