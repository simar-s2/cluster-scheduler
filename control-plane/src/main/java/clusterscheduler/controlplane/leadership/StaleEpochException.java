package clusterscheduler.controlplane.leadership;

/** A write was refused because a newer leader term has started. */
public final class StaleEpochException extends RuntimeException {
    private final long staleEpoch;

    public StaleEpochException(long staleEpoch, long currentEpoch) {
        super("epoch " + staleEpoch + " is no longer current; the lease is at epoch " + currentEpoch);
        this.staleEpoch = staleEpoch;
    }

    public long staleEpoch() {
        return staleEpoch;
    }
}
