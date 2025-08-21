package io.trino.plugin.teradata;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.teradata.clearscape.ClearScapeEnvironmentUtils;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Locale;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

/**
 * Sets up a QueryRunner for Teradata connector integration testing.
 */
public final class TeradataQueryRunner
{
    private TeradataQueryRunner()
    {
        // private constructor to prevent instantiation
    }

    public static Builder builder(TestingTeradataServer server)
    {
        return new Builder(server);
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
        TestingTeradataServer server = new TestingTeradataServer(ClearScapeEnvironmentUtils.generateUniqueEnvName(TeradataQueryRunner.class));
        QueryRunner queryRunner = builder(server).addCoordinatorProperty("http-server.http.port", "8080").setInitialTables(TpchTable.getTables()).build();

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
        private final TestingTeradataServer server;
        private List<TpchTable<?>> initialTables = ImmutableList.of();

        protected Builder(TestingTeradataServer server)
        {
            super(testSessionBuilder().setCatalog("teradata").setSchema(server.getDatabaseName()).build());
            this.server = requireNonNull(server, "server is null");
        }

        public void copyTable(QueryRunner queryRunner, QualifiedObjectName table, Session session)
        {
            @Language("SQL") String sql = String.format("CREATE TABLE %s AS SELECT * FROM %s", table.objectName(), table);
            queryRunner.execute(session, sql);
            ((ObjectAssert) Assertions.assertThat(queryRunner.execute(session, "SELECT count(*) FROM " + table.objectName()).getOnlyValue()).as("Table is not loaded properly: %s", new Object[] {
                    table.objectName()})).isEqualTo(queryRunner.execute(session, "SELECT count(*) FROM " + table).getOnlyValue());
        }

        public void copyTpchTables(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, Session session, Iterable<TpchTable<?>> tables)
        {
            for (TpchTable<?> table : tables) {
                copyTable(queryRunner, sourceCatalog, sourceSchema, table.getTableName().toLowerCase(Locale.ENGLISH), session);
            }
        }

        public void copyTpchTables(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, Iterable<TpchTable<?>> tables)
        {
            copyTpchTables(queryRunner, sourceCatalog, sourceSchema, queryRunner.getDefaultSession(), tables);
        }

        public void copyTable(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String sourceTable, Session session)
        {
            QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);
            if (!server.isTableExists(sourceTable)) {
                copyTable(queryRunner, table, session);
            }
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
                runner.createCatalog("teradata", "teradata", server.getCatalogProperties());

                copyTpchTables(runner, "tpch", TINY_SCHEMA_NAME, initialTables);
            });
            return super.build();
        }
    }
}
