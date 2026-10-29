package clusterscheduler.worker;

/**
 * The worker's half of fencing. PostgreSQL stops a replaced leader from
 * writing, but sending an AssignTask is not a database write. A leader can
 * save an assignment, freeze, and send it after a newer leader has taken
 * over. This check makes the worker refuse anything from an older term once
 * it has heard from a newer one.
 */
final class EpochFence {
    private long highestEpochSeen = 0;

    /** Returns true if the epoch is current, and remembers it if it is the newest so far. */
    synchronized boolean admit(long epoch) {
        if (epoch < highestEpochSeen) {
            return false;
        }
        highestEpochSeen = epoch;
        return true;
    }
}
