package io.gameframe.storage.mysql;

import com.zaxxer.hikari.*;
import io.gameframe.storage.*;
import java.sql.*;
import java.util.concurrent.*;

/** SQL semantics stay explicit. Each task owns a connection; callbacks run on storage workers. */
public final class MySqlStore implements AutoCloseable {
    @FunctionalInterface public interface SqlWork<T> { T run(Connection connection) throws SQLException; }
    private final HikariDataSource pool;
    private final OrderedExecutor executor;
    public MySqlStore(String jdbcUrl, String username, String password, int connections, int capacity) {
        if (connections < 1 || capacity < 1) throw new IllegalArgumentException();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl); config.setUsername(username); config.setPassword(password);
        config.setMaximumPoolSize(connections); config.setMinimumIdle(0); config.setConnectionTimeout(5000);
        config.addDataSourceProperty("connectTimeout", "5000"); config.addDataSourceProperty("socketTimeout", "10000");
        pool = new HikariDataSource(config);
        executor = new OrderedExecutor("mysql-store", connections, capacity);
    }
    /** Use prepared statements. Do not return Connection/Statement/ResultSet or wait on actor work here. */
    public <T> CompletionStage<T> execute(Object affinity, SqlWork<T> work) {
        return executor.submit(affinity, () -> {
            try (Connection connection = pool.getConnection()) { return work.run(connection); }
            catch (SQLException e) { throw new StorageException(StorageException.Outcome.UNKNOWN, "SQL operation failed; reconcile before retry", e); }
        });
    }
    public <T> CompletionStage<T> transaction(Object affinity, SqlWork<T> work) {
        return execute(affinity, connection -> {
            connection.setAutoCommit(false);
            try {
                T value = work.run(connection); connection.commit(); return value;
            } catch (SQLException | RuntimeException | Error error) {
                try { connection.rollback(); } catch (SQLException rollback) { error.addSuppressed(rollback); }
                throw error;
            }
        });
    }
    public void close() { executor.close(); pool.close(); }
}
