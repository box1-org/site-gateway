package org.box1.gateway.support;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;

/** Explicit JDBC transactions on the site database. Every write of the gateway goes through {@link #inTransaction}. */
@ApplicationScoped
public class Db {
    private final DataSource dataSource;

    public Db(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public <T> T inTransaction(Work<T> work) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                T result = work.run(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException error) {
                rollbackQuietly(connection, error);
                throw error;
            }
        }
    }

    public DataSource dataSource() {
        return dataSource;
    }

    private static void rollbackQuietly(Connection connection, Exception cause) {
        try {
            connection.rollback();
        } catch (SQLException rollback) {
            cause.addSuppressed(rollback);
        }
    }

    /** A unit of work in one transaction. */
    @FunctionalInterface
    public interface Work<T> {
        T run(Connection connection) throws SQLException;
    }
}
