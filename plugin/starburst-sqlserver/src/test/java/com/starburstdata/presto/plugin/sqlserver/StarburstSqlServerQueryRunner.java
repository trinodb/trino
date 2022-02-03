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
import com.starburstdata.presto.testing.StarburstDistributedQueryRunner;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.starburstdata.presto.license.TestingLicenseManager.NOOP_LICENSE_MANAGER;
import static com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest.redirectionDisabled;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public final class StarburstSqlServerQueryRunner
{
    public static final String CATALOG = "sqlserver";
    public static final String TEST_SCHEMA = "dbo";

    public static final String ALICE_USER = "alice";
    public static final String BOB_USER = "bob";
    public static final String CHARLIE_USER = "charlie";
    public static final String UNKNOWN_USER = "non_existing_user";

    private StarburstSqlServerQueryRunner() {}

    public static DistributedQueryRunner createStarburstSqlServerQueryRunner(TestingSqlServer testingSqlServer, TpchTable<?>... tables)
            throws Exception
    {
        return createStarburstSqlServerQueryRunner(testingSqlServer, false, ImmutableMap.of(), ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createStarburstSqlServerQueryRunner(
            TestingSqlServer testingSqlServer,
            boolean unlockEnterpriseFeatures,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createStarburstSqlServerQueryRunner(
                testingSqlServer,
                Function.identity(),
                unlockEnterpriseFeatures,
                connectorProperties,
                tables);
    }

    public static DistributedQueryRunner createStarburstSqlServerQueryRunner(
            TestingSqlServer sqlServer,
            Function<Session, Session> sessionModifier,
            boolean unlockEnterpriseFeatures,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createStarburstSqlServerQueryRunner(sqlServer, ImmutableMap.of(), sessionModifier, unlockEnterpriseFeatures, connectorProperties, tables);
    }

    public static DistributedQueryRunner createStarburstSqlServerQueryRunner(
            TestingSqlServer sqlServer,
            Map<String, String> extraProperties,
            Function<Session, Session> sessionModifier,
            boolean unlockEnterpriseFeatures,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        Session session = createSession(sqlServer.getUsername());
        DistributedQueryRunner.Builder builder = StarburstDistributedQueryRunner.builder(session);
        extraProperties.forEach(builder::addExtraProperty);
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

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static Plugin getPluginWithLicense()
    {
        return new StarburstSqlServerPlugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return List.of(getConnectorFactory(NOOP_LICENSE_MANAGER));
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

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        TestingSqlServer testingSqlServer = new TestingSqlServer();

        DistributedQueryRunner queryRunner = createStarburstSqlServerQueryRunner(
                testingSqlServer,
                ImmutableMap.of("http-server.http.port", "8080"),
                Function.identity(),
                false,
                ImmutableMap.of(),
                TpchTable.getTables());

        Logger log = Logger.get(StarburstSqlServerQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
