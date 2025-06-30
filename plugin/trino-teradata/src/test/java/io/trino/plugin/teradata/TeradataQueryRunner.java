package io.trino.plugin.teradata;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * Sets up a QueryRunner for Teradata connector integration testing.
 */
public final class TeradataQueryRunner
{
    private static final TestTeradataDatabase database = new TestTeradataDatabase(DatabaseConfig.fromEnv());

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

        QueryRunner queryRunner = builder().addCoordinatorProperty("http-server.http.port", "8080").build();

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
        private final List<TpchTable<?>> initialTables = ImmutableList.of();

        protected Builder()
        {
            super(testSessionBuilder().setCatalog("teradata").setSchema("trino").build());
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

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            super.setAdditionalSetup(runner -> {
                runner.installPlugin(new TpchPlugin());
                runner.createCatalog("tpch", "tpch");

                runner.installPlugin(new TeradataPlugin());
                runner.createCatalog("teradata", "teradata", database.getConnectionProperties());
            });
            return super.build();
        }
    }
}
