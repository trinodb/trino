package io.trino.plugin.teradata;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Integration test class for Teradata JDBC Connector.
 * Sets up schema and tables before tests and cleans up afterwards.
 */
public class TeradataJdbcConnectorTest
        extends BaseJdbcConnectorTest
{
    protected final TestTeradataDatabase database = new TestTeradataDatabase(DatabaseConfig.fromEnv());

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_SCHEMA, SUPPORTS_DELETE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return database;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TeradataQueryRunner.builder().addCoordinatorProperty("http-server.http.port", "8080").build();
    }

    @BeforeAll
    public void setup()
    {
        if (!isSchemaExists(database.getDatabaseName())) {
            database.execute(String.format("CREATE DATABASE %s as perm=10e6;", database.getDatabaseName()));
        }
        initTables();
    }

    @AfterAll
    public void clean()
    {
        database.execute(String.format("DELETE DATABASE %s", database.getDatabaseName()));
        database.execute(String.format("DROP DATABASE %s", database.getDatabaseName()));
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

    /**
     * Initializes tables in the test schema using SqlBuilder.
     */
    protected void initTables()
    {
        String schema = database.getDatabaseName();

        // Nation Table
        Map<String, String> nationColumns = new LinkedHashMap<>();
        nationColumns.put("rowNumber", "BIGINT");
        nationColumns.put("nationKey", "BIGINT");
        nationColumns.put("name", "VARCHAR(256)");
        nationColumns.put("regionKey", "BIGINT");
        nationColumns.put("comment", "VARCHAR(256)");

        String nationSql = SqlBuilder.buildCreateTableSql(schema, "nation", nationColumns);
        database.execute(nationSql);

        // Region Table
        Map<String, String> regionColumns = new LinkedHashMap<>();
        regionColumns.put("rowNumber", "BIGINT");
        regionColumns.put("regionKey", "BIGINT");
        regionColumns.put("name", "VARCHAR(256)");
        regionColumns.put("comment", "VARCHAR(256)");

        String regionSql = SqlBuilder.buildCreateTableSql(schema, "region", regionColumns);
        database.execute(regionSql);

        // Orders Table
        Map<String, String> ordersColumns = new LinkedHashMap<>();
        ordersColumns.put("rowNumber", "BIGINT");
        ordersColumns.put("orderKey", "BIGINT");
        ordersColumns.put("customerKey", "BIGINT");
        ordersColumns.put("orderStatus", "CHAR");
        ordersColumns.put("totalPriceInCents", "BIGINT");
        ordersColumns.put("orderDate", "INT");
        ordersColumns.put("orderPriority", "VARCHAR(256)");
        ordersColumns.put("clerk", "VARCHAR(256)");
        ordersColumns.put("shipPriority", "INT");
        ordersColumns.put("comment", "VARCHAR(256)");

        String ordersSql = SqlBuilder.buildCreateTableSql(schema, "orders", ordersColumns);
        database.execute(ordersSql);
    }
}
