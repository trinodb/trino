/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.testing.containers.environment;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Represents a product test environment consisting of Trino and supporting services.
 * <p>
 * Environments are expensive to create (container startup, data loading) and should
 * be reused across tests when possible. The {@link EnvironmentManager} handles
 * lifecycle management and ensures only one environment runs at a time.
 * <p>
 * <b>Implementation requirements:</b>
 * <ul>
 *   <li>Must have a public no-arg constructor</li>
 *   <li>{@link #start()} must be idempotent (safe to call multiple times)</li>
 *   <li>{@link #doClose()} must release all resources (containers, connections)</li>
 * </ul>
 * <p>
 * <b>Example implementation:</b>
 * <pre>{@code
 * public class MySqlEnvironment extends ProductTestEnvironment {
 *     private Network network;
 *     private MySQLContainer<?> mysql;
 *     private TrinoContainer trino;
 *
 *     @Override
 *     public void start() {
 *         if (trino != null) return; // Already started
 *
 *         network = Network.newNetwork();
 *         mysql = new MySQLContainer<>("mysql:8.0")
 *             .withNetwork(network)
 *             .withNetworkAliases("mysql");
 *         mysql.start();
 *
 *         trino = TrinoProductTestContainer.builder()
 *             .withNetwork(network)
 *             .withCatalog("mysql", Map.of(...))
 *             .build();
 *         trino.start();
 *     }
 *
 *     @Override
 *     public Connection createTrinoConnection() throws SQLException {
 *         return TrinoProductTestContainer.createConnection(trino);
 *     }
 *
 *     @Override
 *     protected void doClose() {
 *         if (trino != null) trino.close();
 *         if (mysql != null) mysql.close();
 *         if (network != null) network.close();
 *     }
 * }
 * }</pre>
 */
public abstract class ProductTestEnvironment
        implements AutoCloseable
{
    // Tracks whether this environment is being managed by an EnvironmentManager
    private volatile boolean managed;
    private volatile boolean closeAllowed;
    private Connection trinoSessionConnection;

    /**
     * Starts the environment. This method should be idempotent - calling it
     * multiple times should have no effect after the first successful call.
     * <p>
     * Implementations should start all required containers and wait for them
     * to be ready to accept connections.
     */
    public abstract void start();

    /**
     * Creates a JDBC connection to the Trino coordinator in this environment.
     *
     * @return a new JDBC connection
     * @throws SQLException if connection cannot be established
     */
    public abstract Connection createTrinoConnection()
            throws SQLException;

    /**
     * Creates a JDBC connection to the Trino coordinator with the specified user.
     *
     * @param user the user to connect as
     * @return a new JDBC connection
     * @throws SQLException if connection cannot be established
     */
    public abstract Connection createTrinoConnection(String user)
            throws SQLException;

    /**
     * Returns the JDBC URL for connecting to the Trino coordinator.
     * Useful for diagnostic messages and debugging.
     */
    public abstract String getTrinoJdbcUrl();

    /**
     * Returns a human-readable name for this environment.
     * Used in logs and error messages.
     */
    public String getName()
    {
        return getClass().getSimpleName();
    }

    /**
     * Executes a SQL query against Trino and returns the result.
     * <p>
     * This is a convenience method for tests that need to validate query results.
     * For more complex scenarios, use {@link #createTrinoConnection()} directly.
     *
     * @param sql the SQL query to execute
     * @return the query result
     */
    public QueryResult executeTrino(String sql)
    {
        try (Statement stmt = getTrinoSessionConnection().createStatement()) {
            return executeTrinoStatement(stmt, sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Executes a SQL query against Trino as the specified user and returns the result.
     *
     * @param sql the SQL query to execute
     * @param user the user to connect as
     * @return the query result
     */
    public QueryResult executeTrino(String sql, String user)
    {
        try (Connection conn = createTrinoConnection(user);
                Statement stmt = conn.createStatement()) {
            return executeTrinoStatement(stmt, sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Executes a DDL or DML statement against Trino and returns the number of affected rows.
     * <p>
     * Use this for INSERT, UPDATE, DELETE, CREATE, DROP, and other non-SELECT statements.
     *
     * @param sql the SQL statement to execute
     * @return the number of affected rows, or 0 for statements that don't return a count
     */
    public int executeTrinoUpdate(String sql)
    {
        try (Statement stmt = getTrinoSessionConnection().createStatement()) {
            return executeTrinoUpdateStatement(stmt, sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Executes a DDL or DML statement against Trino as the specified user.
     *
     * @param sql the SQL statement to execute
     * @param user the user to connect as
     * @return the number of affected rows, or 0 for statements that don't return a count
     */
    public int executeTrinoUpdate(String sql, String user)
    {
        try (Connection conn = createTrinoConnection(user);
                Statement stmt = conn.createStatement()) {
            return executeTrinoUpdateStatement(stmt, sql);
        }
        catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * Executes multiple statements in the same session, preserving session state.
     * <p>
     * Use this when you need session-level settings like USE or SET SESSION to persist
     * across multiple statements.
     *
     * @param sessionOperations the operations to perform in the session
     * @throws RuntimeException if any SQL operation fails
     */
    public void executeTrinoInSession(TrinoSessionOperations sessionOperations)
    {
        try (Statement stmt = getTrinoSessionConnection().createStatement()) {
            sessionOperations.execute(new TrinoSession(getTrinoSessionConnection(), stmt));
        }
        catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static QueryResult executeTrinoStatement(Statement stmt, String sql)
            throws SQLException
    {
        if (stmt.execute(sql)) {
            try (ResultSet rs = stmt.getResultSet()) {
                return QueryResult.forResultSet(rs);
            }
        }

        // Keep legacy behavior for callers that use executeTrino for DDL/DML.
        return QueryResult.forRows(List.of(List.of((long) stmt.getUpdateCount())));
    }

    private static int executeTrinoUpdateStatement(Statement stmt, String sql)
            throws SQLException
    {
        if (stmt.execute(sql)) {
            try (ResultSet ignored = stmt.getResultSet()) {
                throw new IllegalStateException("SQL returned a result set for update helper: " + sql);
            }
        }
        return stmt.getUpdateCount();
    }

    /**
     * Executes multiple statements in the same session as the specified user, preserving session state.
     * <p>
     * Use this when you need session-level settings like USE or SET SESSION to persist
     * across multiple statements while connected as a specific user.
     *
     * @param user the user to connect as
     * @param sessionOperations the operations to perform in the session
     * @throws RuntimeException if any SQL operation fails
     */
    public void executeTrinoInSession(String user, TrinoSessionOperations sessionOperations)
    {
        try (Connection conn = createTrinoConnection(user);
                Statement stmt = conn.createStatement()) {
            sessionOperations.execute(new TrinoSession(conn, stmt));
        }
        catch (SQLException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Connection getTrinoSessionConnection()
            throws SQLException
    {
        if (trinoSessionConnection == null || trinoSessionConnection.isClosed()) {
            trinoSessionConnection = createTrinoConnection();
        }
        return trinoSessionConnection;
    }

    private void closeTrinoSessionConnection()
            throws SQLException
    {
        if (trinoSessionConnection == null) {
            return;
        }
        trinoSessionConnection.close();
        trinoSessionConnection = null;
    }

    final void cleanupFrameworkState()
            throws SQLException
    {
        closeTrinoSessionConnection();
    }

    /**
     * Operations to perform within a Trino session.
     */
    @FunctionalInterface
    public interface TrinoSessionOperations
    {
        void execute(TrinoSession session)
                throws SQLException;
    }

    /**
     * A Trino session that maintains state across multiple SQL executions.
     */
    public static final class TrinoSession
    {
        private final Connection connection;
        private final Statement statement;

        private TrinoSession(Connection connection, Statement statement)
        {
            this.connection = connection;
            this.statement = statement;
        }

        public QueryResult executeQuery(String sql)
                throws SQLException
        {
            try (ResultSet rs = statement.executeQuery(sql)) {
                return QueryResult.forResultSet(rs);
            }
        }

        public int executeUpdate(String sql)
                throws SQLException
        {
            return statement.executeUpdate(sql);
        }

        public Connection getConnection()
        {
            return connection;
        }
    }

    /**
     * Returns true if this environment is currently running and ready for queries.
     */
    public abstract boolean isRunning();

    /**
     * Hook invoked by {@link ProductTestExtension} before each test method.
     * <p>
     * Default implementation is a no-op. Environments may override to perform
     * per-test setup or cleanup that is environment-owned.
     */
    protected void beforeEachTest()
            throws Exception
    {
    }

    /**
     * Hook invoked by {@link ProductTestExtension} after each test method.
     * <p>
     * Default implementation is a no-op. Environments may override to perform
     * per-test cleanup for resources owned by the environment.
     */
    protected void afterEachTest()
            throws Exception
    {
    }

    /**
     * Closes the environment and releases all resources.
     * This method should be safe to call multiple times.
     * <p>
     * <b>Warning:</b> When using {@link EnvironmentManager}, do not call this method
     * directly. The manager handles lifecycle automatically. Calling close() on a
     * managed environment will log a warning with the call stack.
     */
    @Override
    public final void close()
    {
        if (managed && !closeAllowed) {
            // Environment is managed by EnvironmentManager - ignore spurious close calls.
            // This can happen when JUnit tries to close AutoCloseables in stores.
            return;
        }
        doClose();
    }

    /**
     * Internal method that actually closes the environment.
     * Subclasses implement this instead of close().
     */
    protected abstract void doClose();

    /**
     * Called by EnvironmentManager when it takes ownership of this environment.
     * Package-private to prevent external manipulation.
     */
    final void setManaged(boolean managed)
    {
        this.managed = managed;
    }

    /**
     * Called by EnvironmentManager to allow the close() call to proceed.
     * Package-private to prevent external manipulation.
     */
    final void allowClose()
    {
        this.closeAllowed = true;
    }
}
