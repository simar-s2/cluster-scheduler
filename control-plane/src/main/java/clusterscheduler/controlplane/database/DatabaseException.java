package clusterscheduler.controlplane.database;

/** Unchecked wrapper for SQLException, so callers are not forced to handle it at every level. */
public final class DatabaseException extends RuntimeException {
    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
