package io.trino.plugin.teradata;

import io.trino.testing.sql.SqlExecutor;
import org.intellij.lang.annotations.Language;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class managing a JDBC connection to a Teradata test database.
 * Implements SqlExecutor for executing SQL queries.
 *
 * <p>Note: This class uses a single JDBC Connection without connection pooling.
 * Not recommended for high-concurrency scenarios.</p>
 */
public class TestTeradataDatabase
        implements AutoCloseable, SqlExecutor
{
    private final String databaseName;
    private final Connection connection;
    private final String jdbcUrl;
    private final Map<String, String> connectionProperties = new HashMap<>();

    /**
     * Creates a new TestTeradataDatabase instance using the provided configuration.
     *
     * @param config DatabaseConfig containing connection details.
     * @throws SQLException if a database access error occurs.
     * @throws ClassNotFoundException if the JDBC driver class is not found.
     */
    public TestTeradataDatabase(DatabaseConfig config)
    {
        this.databaseName = config.getDatabaseName();
        this.jdbcUrl = config.getJdbcUrl();

        connectionProperties.put("connection-url", jdbcUrl);
        connectionProperties.put("connection-user", config.getUsername());
        connectionProperties.put("connection-password", config.getPassword());
        try {
            Class.forName("com.teradata.jdbc.TeraDriver");
            this.connection = DriverManager.getConnection(jdbcUrl, config.getUsername(), config.getPassword());
        }
        catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes the given SQL statement.
     *
     * @param sql the SQL string to execute
     * @throws RuntimeException if execution fails
     */
    @Override
    public void execute(@Language("SQL") String sql)
    {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("SQL Execution Failed: " + e.getMessage(), e);
        }
    }

    /**
     * Returns a copy of connection properties.
     *
     * @return map of connection properties
     */
    public Map<String, String> getConnectionProperties()
    {
        return new HashMap<>(connectionProperties);
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public String getJdbcURL()
    {
        return jdbcUrl;
    }

    public void createTestDatabaseIfAbsent()
    {
        if (!isSchemaExists(databaseName)) {
            execute(String.format("CREATE DATABASE %s as perm=100e6;", databaseName));
        }
    }

    public void dropTestDatabaseIfExists()
    {
        execute(String.format("DELETE DATABASE %s", databaseName));
        execute(String.format("DROP DATABASE %s", databaseName));
    }

    /**
     * Checks if a schema exists using a prepared statement.
     *
     * @param schemaName the schema to check
     * @return true if exists; false otherwise
     */
    private boolean isSchemaExists(String schemaName)
    {
        String query = "SELECT COUNT(1) FROM DBC.DatabasesV WHERE DatabaseName = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setString(1, schemaName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to check schema existence: " + e.getMessage(), e);
        }
    }

    public Connection getConnection()
    {
        return connection;
    }

    /**
     * Closes the database connection.
     *
     * @throws SQLException if closing fails
     */
    @Override
    public void close()
            throws SQLException
    {
        if (!connection.isClosed()) {
            connection.close();
        }
    }
}
