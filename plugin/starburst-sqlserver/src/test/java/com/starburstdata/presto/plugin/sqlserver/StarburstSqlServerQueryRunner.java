/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starburstdata.presto.server.StarburstEngineQueryRunner;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.sqlserver.TestingSqlServer;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.io.Resources.getResource;
import static com.starburstdata.presto.license.TestingLicenseManager.NOOP_LICENSE_MANAGER;
import static com.starburstdata.presto.plugin.jdbc.statistics.ManagedStatisticsJdbcConnector.withManagedStatistics;
import static com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest.redirectionDisabled;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public final class StarburstSqlServerQueryRunner
{
    public static final String CATALOG = "sqlserver";
    public static final String TEST_SCHEMA = "dbo";

    public static final String ALICE_USER = "alice";
    public static final String BOB_USER = "bob";
    public static final String CHARLIE_USER = "charlie";
    public static final String UNKNOWN_USER = "non_existing_user";

    private StarburstSqlServerQueryRunner() {}

    private static DistributedQueryRunner createStarburstSqlServerQueryRunner(
            TestingSqlServer sqlServer,
            Map<String, String> extraProperties,
            Function<Session, Session> sessionModifier,
            boolean unlockEnterpriseFeatures,
            Map<String, String> connectorProperties,
            Map<String, String> coordinatorProperties,
            Iterable<TpchTable<?>> tables,
            boolean shouldPartitionTables)
            throws Exception
    {
        Session session = createSession(sqlServer.getUsername());
        DistributedQueryRunner.Builder<?> builder = StarburstEngineQueryRunner.builder(session);
        extraProperties.forEach(builder::addExtraProperty);
        builder.setCoordinatorProperties(coordinatorProperties);
        DistributedQueryRunner queryRunner = builder.build();
        try {
            Session modifiedSession = sessionModifier.apply(session);

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", sqlServer.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", sqlServer.getUsername());
            connectorProperties.putIfAbsent("connection-password", sqlServer.getPassword());

            createUser(sqlServer, ALICE_USER);
            createUser(sqlServer, BOB_USER);
            createUser(sqlServer, CHARLIE_USER);
            sqlServer.execute(format(
                    "CREATE OR ALTER VIEW %s.user_context AS SELECT " +
                            "SYSTEM_USER AS system_user_column," +
                            "original_login() AS original_login_column," +
                            "suser_sname() AS suser_sname_column," +
                            "SESSION_USER AS session_user_column," +
                            "CURRENT_USER AS current_user_column",
                    TEST_SCHEMA));
            sqlServer.execute(format("GRANT SELECT ON %s.user_context to %s", TEST_SCHEMA, ALICE_USER));
            sqlServer.execute(format("GRANT SELECT ON %s.user_context to %s", TEST_SCHEMA, BOB_USER));

            queryRunner.installPlugin(unlockEnterpriseFeatures
                    ? getPluginWithLicense()
                    : new StarburstSqlServerPlugin());
            queryRunner.createCatalog(CATALOG, "sqlserver", connectorProperties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, redirectionDisabled(modifiedSession), tables);

            if (shouldPartitionTables) {
                partitionTables(sqlServer, tables);
            }

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static void partitionTables(TestingSqlServer sqlServer, Iterable<TpchTable<?>> tables)
    {
        for (TpchTable<?> table : tables) {
            String tableName = table.getTableName();
            String columnName = table.getColumns().get(0).getSimplifiedColumnName();
            sqlServer.execute(format("CREATE PARTITION FUNCTION %sPartitionFunction (bigint) AS RANGE RIGHT FOR VALUES (1, 10, 100)", tableName));
            sqlServer.execute(format("CREATE PARTITION SCHEME %sPartitionScheme AS PARTITION %sPartitionFunction ALL TO ('PRIMARY')", tableName, tableName));
            sqlServer.execute(format("CREATE CLUSTERED INDEX ix_%s ON %s(%s) ON %sPartitionScheme(%s)", tableName, tableName, columnName, tableName, columnName));
        }
    }

    private static Plugin getPluginWithLicense()
    {
        return new StarburstSqlServerPlugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return List.of(withManagedStatistics(getConnectorFactory(NOOP_LICENSE_MANAGER)));
            }
        };
    }

    private static void createUser(TestingSqlServer sqlServer, String user)
    {
        sqlServer.execute(format("CREATE LOGIN %1$s_login WITH PASSWORD = 'strong_p@ssw0rd'", user));
        sqlServer.execute(format("CREATE USER %1$s FOR LOGIN %1$s_login", user));
    }

    public static Session createSession(String user)
    {
        return testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.ofUser(user))
                .build();
    }

    public static Builder builder(TestingSqlServer server)
    {
        return new Builder(server);
    }

    public static class Builder
    {
        private final TestingSqlServer server;
        private boolean unlockEnterpriseFeatures;
        private boolean shouldPartitionTables;
        private Map<String, String> connectorProperties = emptyMap();
        private Map<String, String> coordinatorProperties = emptyMap();
        private Map<String, String> extraProperties = emptyMap();
        private Function<Session, Session> sessionModifier = Function.identity();
        private Iterable<TpchTable<?>> tables = ImmutableList.of();

        private Builder(TestingSqlServer server)
        {
            this.server = server;
        }

        public Builder withPartitionedTables()
        {
            this.shouldPartitionTables = true;
            return this;
        }

        public Builder withEnterpriseFeatures()
        {
            unlockEnterpriseFeatures = true;
            return this;
        }

        public Builder withImpersonation()
        {
            return withConnectorProperties(ImmutableMap.of("sqlserver.impersonation.enabled", "true"));
        }

        public Builder withImpersonation(String authToLocal)
        {
            return withImpersonation()
                    .withConnectorProperties(ImmutableMap.of("auth-to-local.config-file", getResource(authToLocal).getPath()));
        }

        public Builder withConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties = updateProperties(this.connectorProperties, connectorProperties);
            return this;
        }

        public Builder withCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties = updateProperties(this.coordinatorProperties, coordinatorProperties);
            return this;
        }

        public Builder withExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = updateProperties(this.extraProperties, extraProperties);
            return this;
        }

        public Builder withSessionModifier(Function<Session, Session> sessionModifier)
        {
            this.sessionModifier = requireNonNull(sessionModifier, "sessionModifier is null");
            return this;
        }

        public Builder withTables(Iterable<TpchTable<?>> tables)
        {
            this.tables = requireNonNull(tables, "tables is null");
            return this;
        }

        public Builder withStarburstStorage(JdbcDatabaseContainer<?> starburstStorage)
        {
            return withCoordinatorProperties(ImmutableMap.of(
                    "insights.jdbc.url", starburstStorage.getJdbcUrl(),
                    "insights.jdbc.user", starburstStorage.getUsername(),
                    "insights.jdbc.password", starburstStorage.getPassword()));
        }

        public Builder withManagedStatistics()
        {
            return withCoordinatorProperties(ImmutableMap.of("starburst.managed-statistics.enabled", "true"))
                    .withConnectorProperties(ImmutableMap.of(
                            "statistics.enabled", "false", // disable collecting native jdbc stats, because it's a non-deterministic process for SqlServer
                            "internal-communication.shared-secret", "internal-shared-secret", // This is required for the internal communication in the managed statistics
                            "managed-statistics.enabled", "true"));
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            return createStarburstSqlServerQueryRunner(server,
                    extraProperties,
                    sessionModifier,
                    unlockEnterpriseFeatures,
                    connectorProperties,
                    coordinatorProperties,
                    tables,
                    shouldPartitionTables);
        }
    }

    private static Map<String, String> updateProperties(Map<String, String> properties, Map<String, String> update)
    {
        return ImmutableMap.<String, String>builder()
                .putAll(requireNonNull(properties, "properties is null"))
                .putAll(requireNonNull(update, "update is null"))
                .buildOrThrow();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        TestingSqlServer testingSqlServer = new TestingSqlServer();

        DistributedQueryRunner queryRunner = builder(testingSqlServer)
                .withExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .withTables(TpchTable.getTables())
                .build();

        Logger log = Logger.get(StarburstSqlServerQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
