package io.trino.plugin.teradata;

import io.trino.sql.query.QueryAssertions;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.CreateAndInsertDataSetup;
import io.trino.testing.datatype.DataSetup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Abstract base class for Teradata JDBC integration tests in Trino.
 * <p>
 * This class initializes a Teradata test database using a provided {@code databaseName},
 * manages its lifecycle, and provides support for creating and inserting data
 * via {@link DataSetup}.
 * </p>
 *
 * <p>Derived classes must implement {@link #initTables()} to set up the schema
 * and tables needed for testing.</p>
 *
 * <p>Database is created before all tests and dropped after all tests. A unique
 * {@code databaseName} can be passed by subclasses to allow isolated parallel test runs.</p>
 */
abstract class AbstractTeradataJDBCTest
        extends AbstractTestQueryFramework
{
    /**
     * Teradata database wrapper for executing SQL and retrieving connections.
     */
    protected final TestTeradataDatabase database;
    /**
     * Assertion helper for running queries and validating results.
     */
    protected final QueryAssertions assertions;
    /**
     * Name of the Teradata database (schema) used in testing.
     */
    protected String databaseName = "trino";
    /**
     * Configuration for the Teradata database (populated from environment).
     */
    protected DatabaseConfig config;

    /**
     * Constructs the test framework with a specific database name.
     *
     * @param databaseName the name of the Teradata database to use in tests
     */
    AbstractTeradataJDBCTest(String databaseName)
    {
        this.databaseName = databaseName;
        this.config = DatabaseConfig.fromEnvWithClearScape();
        // For non-ClearScape mode, override with the provided databaseName
        if (!this.config.isUseClearScape()) {
            this.config.setDatabaseName(databaseName);
        }
        // Use ClearScape if enabled
        this.database = new TestTeradataDatabase(config);

        try {
            this.assertions = new QueryAssertions(new TeradataQueryRunner.Builder().build());
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to initialize query assertions", e);
        }
    }

    /**
     * Creates a {@link QueryRunner} instance for Trino.
     *
     * @return a new QueryRunner for executing Trino queries
     * @throws Exception if initialization fails
     */
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TeradataQueryRunner.builder().build();
    }

    /**
     * Provides a {@link DataSetup} that creates and inserts test data into a table
     * in the configured Teradata schema.
     *
     * @param tableNamePrefix prefix used for naming the test table
     * @return a DataSetup object to create and populate the table
     */
    protected DataSetup teradataJDBCCreateAndInsert(String tableNamePrefix)
    {
        String prefix = String.format("%s.%s", databaseName, tableNamePrefix);
        return new CreateAndInsertDataSetup(database, prefix);
    }

    /**
     * Sets up the test database before all tests run.
     * Creates the schema if it does not exist and invokes {@link #initTables()}.
     */
    @BeforeAll
    public void setup()
    {
        if (!isSchemaExists(databaseName)) {
            database.execute(String.format("CREATE DATABASE %s as perm=10e6;", databaseName));
        }
        initTables();
    }

    /**
     * Cleans up the test database after all tests have run.
     * Deletes and drops the schema used for testing.
     */
    @AfterAll
    public void clean()
    {
        database.execute(String.format("DELETE DATABASE %s", databaseName));
        database.execute(String.format("DROP DATABASE %s", databaseName));
    }

    /**
     * Implemented by subclasses to define the schema and tables required for the test.
     */
    protected abstract void initTables();

    /**
     * Checks whether a given Teradata schema exists.
     *
     * @param schemaName the schema name to check
     * @return {@code true} if the schema exists, {@code false} otherwise
     * @throws RuntimeException if the query fails
     */
    private boolean isSchemaExists(String schemaName)
    {
        String query = "SELECT COUNT(1) FROM DBC.DatabasesV WHERE DatabaseName = ?";
        try (PreparedStatement stmt = database.getConnection().prepareStatement(query)) {
            stmt.setString(1, schemaName);
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to check schema existence: " + e.getMessage(), e);
        }
    }
}
