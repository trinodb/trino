/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.starburstdata.trino.plugins.snowflake.SnowflakePlugin.SNOWFLAKE_JDBC;
import static com.starburstdata.trino.plugins.snowflake.SnowflakePlugin.SNOWFLAKE_PARALLEL;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.JDBC_URL;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.PASSWORD;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.ROLE;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.TEST_DATABASE;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.TEST_WAREHOUSE;
import static com.starburstdata.trino.plugins.snowflake.SnowflakeServer.USER;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class SnowflakeQueryRunner
{
    public static final String TPCH_CATALOG = "tpch";
    public static final String SNOWFLAKE_CATALOG = "snowflake";

    public static final String TEST_SCHEMA = "test_schema_2";

    public static final String ALICE_USER = "alice";

    public static Builder distributedBuilder()
    {
        return new Builder(SnowflakePlugin.SNOWFLAKE_DISTRIBUTED)
                .withConnectorProperties(ImmutableMap.of(
                        "snowflake.stage-schema", TEST_SCHEMA,
                        "snowflake.retry-canceled-queries", "true"));
    }

    public static Map<String, String> impersonationDisabled()
    {
        return ImmutableMap.of(
                "snowflake.role", ROLE);
    }

    public static Builder jdbcBuilder()
    {
        return new Builder(SNOWFLAKE_JDBC);
    }

    public static Builder parallelBuilder()
    {
        return new Builder(SNOWFLAKE_PARALLEL);
    }

    private static DistributedQueryRunner createSnowflakeQueryRunner(
            SnowflakeServer server,
            String connectorName,
            Optional<String> warehouse,
            Optional<String> database,
            Map<String, String> connectorProperties,
            Map<String, String> extraProperties,
            int nodeCount,
            boolean createUserContextView,
            Iterable<TpchTable<?>> tpchTables,
            Map<String, String> coordinatorProperties,
            Consumer<QueryRunner> additionalSetup)
            throws Exception
    {
        DistributedQueryRunner.Builder builder = DistributedQueryRunner.builder(createSession())
                .setNodeCount(nodeCount);
        extraProperties.forEach(builder::addExtraProperty);
        DistributedQueryRunner queryRunner = builder
                .setCoordinatorProperties(coordinatorProperties)
                .setAdditionalSetup(additionalSetup).build();

        createSnowflakeQueryRunner(
                server,
                new TestingSnowflakePlugin(),
                connectorName,
                warehouse,
                database,
                connectorProperties,
                createUserContextView,
                tpchTables,
                queryRunner.getDefaultSession(),
                queryRunner);
        return queryRunner;
    }

    protected static void createSnowflakeQueryRunner(
            SnowflakeServer server,
            Plugin snowflakePlugin,
            String connectorName,
            Optional<String> warehouse,
            Optional<String> database,
            Map<String, String> connectorProperties,
            boolean createUserContextView, Iterable<TpchTable<?>> tpchTables,
            Session session,
            DistributedQueryRunner queryRunner)
            throws SQLException
    {
        server.init();
        // Create view used for testing user/role impersonation
        if (createUserContextView) {
            database.ifPresent(databaseName ->
                    server.safeExecuteOnDatabase(
                            databaseName,
                            "CREATE VIEW IF NOT EXISTS public.user_context (user, role) AS SELECT current_user(), current_role();",
                            "GRANT SELECT ON VIEW USER_CONTEXT TO ROLE \"PUBLIC\";"));
        }

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog(TPCH_CATALOG, TPCH_CATALOG, ImmutableMap.of());

            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("connection-url", JDBC_URL)
                    .put("connection-user", USER)
                    .put("connection-password", PASSWORD)
                    .putAll(connectorProperties);
            warehouse.ifPresent(warehouseName -> properties.put("snowflake.warehouse", warehouseName));
            database.ifPresent(databaseName -> properties.put("snowflake.database", databaseName));

            queryRunner.installPlugin(snowflakePlugin);
            queryRunner.createCatalog(SNOWFLAKE_CATALOG, connectorName, properties.buildOrThrow());

            copyTpchTables(queryRunner, TPCH_CATALOG, TINY_SCHEMA_NAME, session, tpchTables);

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx", ImmutableMap.of());
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    public static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("snowflake")
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.forUser(USER)
                        .build())
                .build();
    }

    public static Session createSessionForUser(String user)
    {
        return testSessionBuilder()
                .setCatalog("snowflake")
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.forUser(user)
                        .build())
                .build();
    }

    public static class Builder
    {
        private String connectorName;
        private SnowflakeServer server = new SnowflakeServer();
        private Optional<String> warehouseName = Optional.of(TEST_WAREHOUSE);
        private Optional<String> databaseName = Optional.of(TEST_DATABASE);
        private Optional<String> schemaName = Optional.empty();
        private ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        private ImmutableMap.Builder<String, String> extraProperties = ImmutableMap.builder();
        private int nodeCount = 3;
        private Iterable<TpchTable<?>> tpchTables = new ArrayList<>();
        private boolean createUserContextView;
        private ImmutableMap.Builder<String, String> coordinatorProperties = ImmutableMap.builder();
        private Consumer<QueryRunner> additionalSetup = queryRunner -> {};

        private Builder(String connectorName)
        {
            this.connectorName = requireNonNull(connectorName, "connectorName is null");
        }

        public Builder withServer(SnowflakeServer server)
        {
            this.server = requireNonNull(server, "server is null");
            return this;
        }

        public Builder withWarehouse(Optional<String> warehouseName)
        {
            this.warehouseName = warehouseName;
            return this;
        }

        public Builder withDatabase(Optional<String> databaseName)
        {
            this.databaseName = databaseName;
            return this;
        }

        public Builder withSchema(Optional<String> schemaName)
        {
            this.schemaName = schemaName;
            return this;
        }

        // additive. TODO change name to indicate that
        public Builder withConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties.putAll(requireNonNull(connectorProperties, "connectorProperties is null"));
            return this;
        }

        public Builder withExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties.putAll(requireNonNull(extraProperties, "extraProperties is null"));
            return this;
        }

        public Builder withNodeCount(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder withTpchTables(Iterable<TpchTable<?>> tpchTables)
        {
            this.tpchTables = tpchTables;
            return this;
        }

        public Builder withCreateUserContextView()
        {
            this.createUserContextView = true;
            return this;
        }

        public Builder withAdditionalSetup(Consumer<QueryRunner> additionalSetup)
        {
            this.additionalSetup = requireNonNull(additionalSetup, "additionalSetup is null");
            return this;
        }

        public Builder withCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties.putAll(requireNonNull(coordinatorProperties, "connectorProperties is null"));
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            if (databaseName.isPresent() && schemaName.isPresent()) {
                server.createSchema(databaseName.get(), schemaName.get());
            }
            return createSnowflakeQueryRunner(
                    server,
                    connectorName,
                    warehouseName,
                    databaseName,
                    connectorProperties.buildOrThrow(),
                    extraProperties.buildOrThrow(),
                    nodeCount,
                    createUserContextView,
                    tpchTables,
                    coordinatorProperties.buildOrThrow(),
                    additionalSetup);
        }
    }

    protected SnowflakeQueryRunner()
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        SnowflakeServer server = new SnowflakeServer();
        DistributedQueryRunner queryRunner = jdbcBuilder()
                .withServer(server)
                .withConnectorProperties(impersonationDisabled())
                .withExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .build();

        Logger log = Logger.get(SnowflakeQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
