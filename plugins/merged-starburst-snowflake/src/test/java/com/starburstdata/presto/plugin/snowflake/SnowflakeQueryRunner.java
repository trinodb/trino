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
import com.starburstdata.presto.testing.StarburstDistributedQueryRunner;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;

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
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

class SnowflakeQueryRunner
{
    private static final String TPCH_CATALOG = "tpch";
    private static final String SNOWFLAKE_CATALOG = "snowflake";

    static final String TEST_SCHEMA = "test_schema_2";

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
                .withConnectorProperties(ImmutableMap.of(
                        "snowflake.stage-schema", TEST_SCHEMA,
                        "snowflake.retry-canceled-queries", "true"));
    }

    static Builder jdbcBuilder()
    {
        return new Builder(SNOWFLAKE_JDBC);
    }

    private static DistributedQueryRunner createSnowflakeQueryRunner(
            SnowflakeServer server,
            String connectorName,
            Optional<String> warehouse,
            Optional<String> database,
            Map<String, String> connectorProperties,
            Map<String, String> extraProperties,
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
        DistributedQueryRunner.Builder builder = StarburstDistributedQueryRunner.builder(createSession(useOktaCredentials))
                .setNodeCount(nodeCount);
        extraProperties.forEach(builder::addExtraProperty);
        DistributedQueryRunner queryRunner = builder.build();

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
        private ImmutableMap.Builder<String, String> connectorProperties = ImmutableMap.builder();
        private ImmutableMap.Builder<String, String> extraProperties = ImmutableMap.builder();
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

        public Builder withConnectionPooling()
        {
            connectorProperties.put("connection-pool.enabled", "true");
            connectorProperties.put("connection-pool.max-size", "50");
            return this;
        }

        public Builder withOktaCredentials(boolean useOktaCredentials)
        {
            this.useOktaCredentials = useOktaCredentials;
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            return createSnowflakeQueryRunner(server, connectorName, warehouseName, databaseName, connectorProperties.build(), extraProperties.build(), nodeCount, useOktaCredentials);
        }
    }

    private SnowflakeQueryRunner()
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

        // Uncomment below when you need to recreate the data set. Be careful not to delete shared testing resources.
        //server.dropSchemaIfExistsCascade(TEST_SCHEMA);
        //server.createSchema(TEST_SCHEMA);
        //copyTpchTables(queryRunner, TPCH_CATALOG, TEST_SCHEMA, queryRunner.getDefaultSession(), TpchTable.getTables());
        Logger log = Logger.get(SnowflakeQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
