package clusterscheduler.controlplane.database;

import java.sql.Connection;
import java.sql.SQLException;

/** Code that runs inside one database transaction. */
@FunctionalInterface
public interface TransactionWork<T> {
    T run(Connection connection) throws SQLException;
}
