package io.trino.plugin.teradata;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import org.junit.jupiter.api.AfterAll;

import java.util.OptionalInt;

import static io.trino.plugin.teradata.util.TeradataConstants.TERADATA_OBJECT_NAME_LIMIT;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

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
            case SUPPORTS_CREATE_VIEW,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_INSERT,
                 SUPPORTS_UPDATE,
                 SUPPORTS_ADD_COLUMN,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_TRUNCATE,
                 SUPPORTS_MERGE,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_DROP_SCHEMA_CASCADE,
                 SUPPORTS_RENAME_SCHEMA -> false;
            case SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_PREDICATE_PUSHDOWN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_LIMIT_PUSHDOWN -> true;
//                 SUPPORTS_DROP_SCHEMA_CASCADE -> true;
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
        database.createTestDatabaseIfAbsent();
        return TeradataQueryRunner.builder().addCoordinatorProperty("http-server.http.port", "8080").setInitialTables(REQUIRED_TPCH_TABLES).build();
    }

    @AfterAll
    public void cleanupTestDatabase()
    {
        database.dropTestDatabaseIfExists();
    }
    @Override
    protected OptionalInt maxSchemaNameLength() {
        return OptionalInt.of(TERADATA_OBJECT_NAME_LIMIT);
    }
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage(format("Schema name must be shorter than or equal to '%s' characters but got '%s'",TERADATA_OBJECT_NAME_LIMIT,TERADATA_OBJECT_NAME_LIMIT + 1));
    }

}
