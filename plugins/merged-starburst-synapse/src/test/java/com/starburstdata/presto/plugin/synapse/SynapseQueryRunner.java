/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.synapse;

import com.google.common.collect.ImmutableMap;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.starburstdata.presto.testing.StarburstDistributedQueryRunner;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.jmx.JmxPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;

import static com.starburstdata.presto.plugin.synapse.SynapseServer.JDBC_URL;
import static com.starburstdata.presto.plugin.synapse.SynapseServer.PASSWORD;
import static com.starburstdata.presto.plugin.synapse.SynapseServer.USERNAME;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class SynapseQueryRunner
{
    private SynapseQueryRunner() {}

    private static final Logger log = Logger.get(SynapseQueryRunner.class);

    private static final int ERROR_USER_EXISTS = 15023;
    private static final int ERROR_OBJECT_EXISTS = 2714;

    public static final String DEFAULT_CATALOG_NAME = "synapse";
    public static final String TEST_SCHEMA = "dbo";

    public static final String ALICE_USER = "alice";
    public static final String BOB_USER = "bob";
    public static final String CHARLIE_USER = "charlie";
    public static final String UNKNOWN_USER = "non_existing_user";

    public static DistributedQueryRunner createSynapseQueryRunner(
            SynapseServer synapseServer,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createSynapseQueryRunner(
                Map.of(),
                synapseServer,
                DEFAULT_CATALOG_NAME,
                connectorProperties,
                tables);
    }

    public static DistributedQueryRunner createSynapseQueryRunner(
            Map<String, String> extraProperties,
            SynapseServer synapseServer,
            String catalogName,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        Session session = createSession(USERNAME, catalogName);
        DistributedQueryRunner.Builder builder = StarburstDistributedQueryRunner.builder(session);
        extraProperties.forEach(builder::addExtraProperty);
        DistributedQueryRunner queryRunner = builder.build();
        try {
            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", JDBC_URL);
            connectorProperties.putIfAbsent("connection-user", USERNAME);
            connectorProperties.putIfAbsent("connection-password", PASSWORD);

            createUser(synapseServer, ALICE_USER, TEST_SCHEMA);
            createUser(synapseServer, BOB_USER, TEST_SCHEMA);
            createUser(synapseServer, CHARLIE_USER, TEST_SCHEMA);

            try {
                synapseServer.execute(format(
                        "CREATE VIEW %s.user_context AS SELECT " +
                                "SESSION_USER AS session_user_column," +
                                "CURRENT_USER AS current_user_column",
                        TEST_SCHEMA));
            }
            catch (RuntimeException e) {
                if (!(e.getCause() instanceof SQLServerException) || ((SQLServerException) e.getCause()).getErrorCode() != ERROR_OBJECT_EXISTS) {
                    throw e;
                }
            }

            synapseServer.execute(format("GRANT SELECT ON %s.user_context to %s", TEST_SCHEMA, ALICE_USER));
            synapseServer.execute(format("GRANT SELECT ON %s.user_context to %s", TEST_SCHEMA, BOB_USER));

            queryRunner.installPlugin(new TestingSynapsePlugin());

            queryRunner.createCatalog(catalogName, "synapse", connectorProperties);

            copyTpchTablesIfNotExists(queryRunner, "tpch", TINY_SCHEMA_NAME, session, tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static void createUser(SynapseServer synapseServer, String user, String schema)
    {
        try {
            synapseServer.execute(format("CREATE USER %s WITHOUT LOGIN WITH DEFAULT_SCHEMA = %s", user, schema));
        }
        catch (RuntimeException e) {
            if (e.getCause() instanceof SQLServerException && ((SQLServerException) e.getCause()).getErrorCode() != ERROR_USER_EXISTS) {
                throw e;
            }
        }
    }

    public static Session createSession(String user)
    {
        return createSession(user, DEFAULT_CATALOG_NAME);
    }

    public static Session createSession(String user, String catalog)
    {
        return testSessionBuilder()
                .setCatalog(catalog)
                .setSchema(TEST_SCHEMA)
                .setIdentity(Identity.ofUser(user))
                .build();
    }

    private static void copyTpchTablesIfNotExists(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTableIfNotExist(queryRunner, sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH), session);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTableIfNotExist(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String sourceTable, Session session)
    {
        QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);
        long start = System.nanoTime();
        log.info("Running import for %s", table.getObjectName());
        String sql = format("CREATE TABLE IF NOT EXISTS %s AS SELECT * FROM %s", table.getObjectName(), table);
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, table.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();

        @SuppressWarnings("resource")
        DistributedQueryRunner queryRunner = createSynapseQueryRunner(
                Map.of("http-server.http.port", "8080"),
                new SynapseServer(), // dummy
                DEFAULT_CATALOG_NAME,
                Map.of(),
                TpchTable.getTables());

        Logger log = Logger.get(SynapseQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
