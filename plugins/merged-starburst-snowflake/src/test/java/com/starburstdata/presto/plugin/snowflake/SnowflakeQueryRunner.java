/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.plugin.jmx.JmxPlugin;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

import java.util.Map;
import java.util.Optional;

import static com.starburstdata.presto.plugin.snowflake.SnowflakePlugin.SNOWFLAKE_DISTRIBUTED;
import static com.starburstdata.presto.plugin.snowflake.SnowflakePlugin.SNOWFLAKE_JDBC;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.ACCOUNT_NAME;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.ACCOUNT_URL;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.CLIENT_ID;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.CLIENT_SECRET;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.JDBC_URL;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_PASSWORD;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_URL;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.OKTA_USER;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.PASSWORD;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.PUBLIC_DB;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.TEST_DATABASE;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.TEST_WAREHOUSE;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeServer.USER;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.QueryAssertions.copyTable;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

class SnowflakeQueryRunner
{
    private static final Logger LOG = Logger.get(SnowflakeQueryRunner.class);

    private static final String TPCH_CATALOG = "tpch";
    private static final String SNOWFLAKE_CATALOG = "snowflake";

    static final String TEST_SCHEMA = "test_schema";

    public static final String ALICE_USER = "alice";

    static Map<String, String> impersonationDisabled()
    {
        return ImmutableMap.of(
                "snowflake.role", "test_role");
    }

    static Map<String, String> impersonationEnabled()
    {
        return ImmutableMap.of(
                "snowflake.impersonation-type", "ROLE",
                "auth-to-local.config-file", SnowflakeQueryRunner.class.getClassLoader().getResource("auth-to-local.json").getPath());
    }

    static Map<String, String> oktaImpersonationEnabled(boolean roleImpersonation)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .put("user-credential-name", "okta.user")
                .put("password-credential-name", "okta.password")
                .put("okta.account-url", OKTA_URL)
                .put("snowflake.account-url", ACCOUNT_URL)
                .put("snowflake.account-name", ACCOUNT_NAME)
                .put("snowflake.client-id", CLIENT_ID)
                .put("snowflake.client-secret", CLIENT_SECRET)
                .put("snowflake.credential.use-extra-credentials", "true")
                .put("snowflake.credential.cache-ttl", "24h");
        if (roleImpersonation) {
            builder
                    .put("snowflake.impersonation-type", "ROLE_OKTA_LDAP_PASSTHROUGH")
                    .put("auth-to-local.config-file", SnowflakeQueryRunner.class.getClassLoader().getResource("auth-to-local.json").getPath());
        }
        else {
            builder.put("snowflake.impersonation-type", "OKTA_LDAP_PASSTHROUGH");
        }
        return builder.build();
    }

    static Builder distributedBuilder()
    {
        return new Builder(SNOWFLAKE_DISTRIBUTED)
                .withAdditionalProperties(ImmutableMap.of("snowflake.stage-schema", TEST_SCHEMA));
    }

    static Builder jdbcBuilder()
    {
        return new Builder(SNOWFLAKE_JDBC);
    }

    private static QueryRunner createSnowflakeQueryRunner(
            SnowflakeServer server,
            String connectorName,
            Optional<String> warehouse,
            Optional<String> database,
            Map<String, String> additionalProperties,
            int nodeCount,
            boolean useOktaCredentials)
            throws Exception
    {
        server.init();
        // Create view used for testing user/role impersonation
        server.executeOnDatabase(
                PUBLIC_DB,
                "CREATE VIEW IF NOT EXISTS public.user_context (user, role) AS SELECT current_user(), current_role();",
                "GRANT SELECT ON VIEW USER_CONTEXT TO ROLE \"PUBLIC\";");
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession(useOktaCredentials))
                .setNodeCount(nodeCount)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog(TPCH_CATALOG, TPCH_CATALOG, ImmutableMap.of());

            ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                    .put("allow-drop-table", "true")
                    .put("connection-url", JDBC_URL)
                    .put("connection-user", USER)
                    .put("connection-password", PASSWORD)
                    .putAll(additionalProperties);
            warehouse.ifPresent(warehouseName -> properties.put("snowflake.warehouse", warehouseName));
            database.ifPresent(databaseName -> properties.put("snowflake.database", databaseName));

            queryRunner.installPlugin(new TestingSnowflakePlugin());
            queryRunner.createCatalog(SNOWFLAKE_CATALOG, connectorName, properties.build());
            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx", ImmutableMap.of());
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    private static void copyTpchTables(
            QueryRunner queryRunner,
            Iterable<TpchTable<?>> tables)
    {
        LOG.info("Loading data from %s.%s...", TPCH_CATALOG, TINY_SCHEMA_NAME);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, TPCH_CATALOG, TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH), queryRunner.getDefaultSession());
        }
        LOG.info("Loading from %s.%s complete in %s", TPCH_CATALOG, TINY_SCHEMA_NAME, nanosSince(startTime).toString(SECONDS));
    }

    private static Session createSession(boolean useOktaCredentials)
    {
        return createSessionForUser(OKTA_USER, useOktaCredentials);
    }

    public static Session createSessionForUser(String user, boolean useOktaCredentials)
    {
        // in tests, we pass credentials via the extra credentials mechanism
        Map<String, String> extraCredentials = useOktaCredentials ?
                ImmutableMap.of("okta.user", OKTA_USER, "okta.password", OKTA_PASSWORD) :
                ImmutableMap.of();

        return testSessionBuilder()
                .setCatalog("snowflake")
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.forUser(user)
                        .withExtraCredentials(extraCredentials)
                        .build())
                .build();
    }

    public static class Builder
    {
        private String connectorName;
        private SnowflakeServer server = new SnowflakeServer();
        private Optional<String> warehouseName = Optional.of(TEST_WAREHOUSE);
        private Optional<String> databaseName = Optional.of(TEST_DATABASE);
        private ImmutableMap.Builder<String, String> additionalPropertiesBuilder = ImmutableMap.builder();
        private int nodeCount = 3;
        private boolean useOktaCredentials;

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

        public Builder withAdditionalProperties(Map<String, String> additionalProperties)
        {
            additionalPropertiesBuilder.putAll(requireNonNull(additionalProperties, "additionalProperties is null"));
            return this;
        }

        public Builder withNodeCount(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder withConnectionPooling()
        {
            additionalPropertiesBuilder.put("connection-pool.enabled", "true");
            additionalPropertiesBuilder.put("connection-pool.max-size", "50");
            return this;
        }

        public Builder withOktaCredentials(boolean useOktaCredentials)
        {
            this.useOktaCredentials = useOktaCredentials;
            return this;
        }

        public QueryRunner build()
                throws Exception
        {
            return createSnowflakeQueryRunner(server, connectorName, warehouseName, databaseName, additionalPropertiesBuilder.build(), nodeCount, useOktaCredentials);
        }
    }

    private SnowflakeQueryRunner()
    {
    }

    public static void main(String[] args)
            throws Exception
    {
        SnowflakeServer server = new SnowflakeServer();
        try (QueryRunner runner = jdbcBuilder()
                .withServer(server)
                .withAdditionalProperties(impersonationDisabled())
                .build()) {
            // Uncomment below when you need to recreate the data set. Be careful not to delete shared testing resources.
            //server.dropSchemaIfExistsCascade(TEST_SCHEMA);
            //server.createSchema(TEST_SCHEMA);
            //copyTpchTables(runner, TpchTable.getTables());
        }
    }
}
