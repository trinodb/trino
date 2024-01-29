/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getenv;

public final class SapHanaQueryRunner
{
    public static final String GRANTED_USER = "alice";
    public static final String NON_GRANTED_USER = "bob";
    private static final boolean TESTCONTAINERS_REUSE_ENABLE = parseBoolean(getenv("TESTCONTAINERS_REUSE_ENABLE"));

    private SapHanaQueryRunner() {}

    public static DistributedQueryRunner createSapHanaQueryRunner(
            TestingSapHanaServer server,
            Map<String, String> connectorProperties,
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createSapHanaQueryRunner(server, "saphana", connectorProperties, extraProperties, tables);
    }

    public static DistributedQueryRunner createSapHanaQueryRunner(
            TestingSapHanaServer server,
            String catalogName,
            Map<String, String> connectorProperties,
            Map<String, String> extraProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = null;
        try {
            DistributedQueryRunner.Builder<?> builder = DistributedQueryRunner.builder(createSession(catalogName));
            extraProperties.forEach(builder::addExtraProperty);
            queryRunner = builder.build();

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", server.getJdbcUrl());
            connectorProperties.putIfAbsent("connection-user", server.getUser());
            connectorProperties.putIfAbsent("connection-password", server.getPassword());

            List<String> queries = ImmutableList.of(
                    "CREATE SCHEMA tpch",
                    "CREATE USER " + GRANTED_USER,
                    "CREATE USER " + NON_GRANTED_USER,
                    "GRANT ALL PRIVILEGES ON SCHEMA tpch TO " + GRANTED_USER);

            if (TESTCONTAINERS_REUSE_ENABLE) {
                try {
                    // in case when environment was already initialized by previous run, queries will fail,
                    // and server::executeWithRetry will retry, and, still, eventually will fail.
                    // this leads to longer startup time for local environment.
                    // To mitigate that single try with server::execute is used, which is the acceptable trade-off during local testing.
                    queries.forEach(server::execute);
                }
                catch (RuntimeException e) {
                    System.err.println("Environment was not initialized properly. Either because local environment was already initialized by previous run, or... it's actual fail."
                            + " TESTCONTAINERS_REUSE_ENABLE=true, so you know what you do." + e.getMessage());
                }
            }
            else {
                queries.forEach(server::executeWithRetry);
            }
            queryRunner.installPlugin(new TestingSapHanaPlugin());
            queryRunner.createCatalog(catalogName, "sap_hana", connectorProperties);

            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(catalogName), tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, server);
            throw e;
        }
    }

    public static Session createSession(String catalogName)
    {
        return createSession(GRANTED_USER, catalogName);
    }

    public static Session createSession(String user, String catalogName)
    {
        return testSessionBuilder()
                .setCatalog(catalogName)
                .setSchema("tpch")
                .setIdentity(Identity.ofUser(user))
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        TestingSapHanaServer sapHanaServer = TestingSapHanaServer.create();
        DistributedQueryRunner queryRunner = createSapHanaQueryRunner(
                sapHanaServer,
                ImmutableMap.<String, String>builder()
                        .put("connection-url", sapHanaServer.getJdbcUrl())
                        .buildOrThrow(),
                ImmutableMap.of("http-server.http.port", "8080"),
                TpchTable.getTables());

        Logger log = Logger.get(SapHanaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
