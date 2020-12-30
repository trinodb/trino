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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.jmx.JmxPlugin;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static com.starburstdata.presto.plugin.synapse.SynapseServer.JDBC_URL;
import static com.starburstdata.presto.plugin.synapse.SynapseServer.PASSWORD;
import static com.starburstdata.presto.plugin.synapse.SynapseServer.USERNAME;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class SynapseQueryRunner
{
    private SynapseQueryRunner() {}

    private static final Logger log = Logger.get(SynapseQueryRunner.class);

    public static final String CATALOG = "synapse";
    public static final String TEST_SCHEMA = "dbo";

    public static final String ALICE_USER = "alice";
    public static final String BOB_USER = "bob";
    public static final String CHARLIE_USER = "charlie";
    public static final String UNKNOWN_USER = "non_existing_user";

    public static DistributedQueryRunner createSynapseQueryRunner(SynapseServer synapseServer, TpchTable<?>... tables)
            throws Exception
    {
        return createSynapseQueryRunner(synapseServer, true, ImmutableMap.of(), ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createSynapseQueryRunner(
            SynapseServer synapseServer,
            boolean unlockEnterpriseFeatures,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createSynapseQueryRunner(
                synapseServer,
                unlockEnterpriseFeatures,
                Function.identity(),
                connectorProperties,
                tables);
    }

    public static DistributedQueryRunner createSynapseQueryRunner(
            SynapseServer synapseServer,
            boolean unlockEnterpriseFeatures,
            Function<Session, Session> sessionModifier,
            Map<String, String> connectorProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        Session session = createSession(USERNAME);
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session).build();
        try {
            Session modifiedSession = sessionModifier.apply(session);

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
            connectorProperties.putIfAbsent("connection-url", JDBC_URL);
            connectorProperties.putIfAbsent("connection-user", USERNAME);
            connectorProperties.putIfAbsent("connection-password", PASSWORD);
            connectorProperties.putIfAbsent("allow-drop-table", "true");

            synapseServer.execute(format("DROP USER %s", ALICE_USER));
            synapseServer.execute(format("DROP USER %s", BOB_USER));
            synapseServer.execute(format("DROP USER %s", CHARLIE_USER));
            createUser(synapseServer, ALICE_USER, TEST_SCHEMA);
            createUser(synapseServer, BOB_USER, TEST_SCHEMA);
            createUser(synapseServer, CHARLIE_USER, TEST_SCHEMA);

            // CREATE OR ALTER VIEW not supported
            synapseServer.execute(format("DROP VIEW %s.user_context", TEST_SCHEMA));
            synapseServer.execute(format(
                    "CREATE VIEW %s.user_context AS SELECT " +
                            "SESSION_USER AS session_user_column," +
                            "CURRENT_USER AS current_user_column",
                    TEST_SCHEMA));

            synapseServer.execute(format("GRANT SELECT ON %s.user_context to %s", TEST_SCHEMA, ALICE_USER));
            synapseServer.execute(format("GRANT SELECT ON %s.user_context to %s", TEST_SCHEMA, BOB_USER));

            queryRunner.installPlugin(unlockEnterpriseFeatures
                    ? new TestingSynapsePlugin()
                    : new StarburstSynapsePlugin());

            queryRunner.createCatalog(CATALOG, "synapse", connectorProperties);

            // TODO(https://starburstdata.atlassian.net/browse/PRESTO-5096) Resource cleaner for test objects in Synapse
            copyTpchTablesIfNotExists(queryRunner, "tpch", TINY_SCHEMA_NAME, modifiedSession, tables);

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    private static void createUser(SynapseServer synapseServer, String user, String schema)
    {
        synapseServer.execute(format("CREATE USER %s WITHOUT LOGIN WITH DEFAULT_SCHEMA = %s", user, schema));
    }

    public static Session createSession(String user)
    {
        return testSessionBuilder()
                .setCatalog(CATALOG)
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
}
