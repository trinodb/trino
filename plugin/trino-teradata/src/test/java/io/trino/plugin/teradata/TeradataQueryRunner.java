package io.trino.plugin.teradata;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;
import org.intellij.lang.annotations.Language;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

/**
 * Sets up a QueryRunner for Teradata connector integration testing.
 */
public final class TeradataQueryRunner
{
    private static TestTeradataDatabase database;

    private TeradataQueryRunner()
    {
        // private constructor to prevent instantiation
    }

    /**
     * Returns the singleton TestTeradataDatabase instance.
     *
     * @return SqlExecutor instance
     */
    public static SqlExecutor getSqlExecutor()
    {
        return database;
    }

    public static void setTeradataDatabase(TestTeradataDatabase database)
    {
        TeradataQueryRunner.database = requireNonNull(database, "database is null");
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Starts a QueryRunner server for Teradata connector on port 8080.
     *
     * @param args unused
     * @throws Exception on error
     */
    public static void main(String[] args)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("io.trino.plugin.teradata", Level.DEBUG);
        logger.setLevel("io.trino", Level.INFO);
        DatabaseConfig dbConfig = DatabaseTestUtil.getDatabaseConfig();
        database = new TestTeradataDatabase(dbConfig);
        TeradataQueryRunner.setTeradataDatabase(database);
        QueryRunner queryRunner = builder().addCoordinatorProperty("http-server.http.port", "8080").setInitialTables(TpchTable.getTables()).build();

        Logger log = Logger.get(TeradataQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    /**
     * Builder class for constructing DistributedQueryRunner with Teradata connector.
     */
    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        protected Builder()
        {
            super(testSessionBuilder().setCatalog("teradata").setSchema(database.getDatabaseName()).build());
        }

        public static void copyTable(QueryRunner queryRunner, QualifiedObjectName table, Session session)
        {
            long start = System.nanoTime();
            @Language("SQL") String sql = String.format("CREATE TABLE %s AS SELECT * FROM %s", table.objectName(), table);
            long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);

            ((ObjectAssert) Assertions.assertThat(queryRunner.execute(session, "SELECT count(*) FROM " + table.objectName()).getOnlyValue()).as("Table is not loaded properly: %s", new Object[] {
                    table.objectName()})).isEqualTo(queryRunner.execute(session, "SELECT count(*) FROM " + table).getOnlyValue());
        }

        public static void copyTable(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String sourceTable, Session session)
        {
            QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);
            if (!database.isTableExists(sourceTable)) {
                copyTable(queryRunner, table, session);
            }
        }

        public static void copyTpchTables(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, Session session, Iterable<TpchTable<?>> tables)
        {
            for (TpchTable<?> table : tables) {
                copyTable(queryRunner, sourceCatalog, sourceSchema, table.getTableName().toLowerCase(Locale.ENGLISH), session);
            }
        }

        public static void copyTpchTables(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, Iterable<TpchTable<?>> tables)
        {
            copyTpchTables(queryRunner, sourceCatalog, sourceSchema, queryRunner.getDefaultSession(), tables);
        }

        /**
         * Adds connector properties to the builder.
         *
         * @param connectorProperties key-value properties
         * @return this Builder
         */
        @CanIgnoreReturnValue
        public Builder addConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(connectorProperties);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            super.setAdditionalSetup(runner -> {
                runner.installPlugin(new TpchPlugin());
                runner.createCatalog("tpch", "tpch");

                runner.installPlugin(new TeradataPlugin());
                runner.createCatalog("teradata", "teradata", database.getConnectionProperties());
                database.createTestDatabaseIfAbsent();

                copyTpchTables(runner, "tpch", TINY_SCHEMA_NAME, initialTables);
            });
            return super.build();
        }
    }
}
