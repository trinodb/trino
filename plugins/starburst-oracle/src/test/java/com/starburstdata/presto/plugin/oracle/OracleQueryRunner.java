/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Runnables;
import com.starburstdata.presto.plugin.jdbc.kerberos.KerberosConnectionFactory;
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

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.ALICE_USER;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.USER;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.connectionProperties;
import static com.starburstdata.presto.redirection.AbstractTableScanRedirectionTest.redirectionDisabled;
import static io.airlift.log.Level.DEBUG;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTable;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class OracleQueryRunner
{
    private static final Logger LOG = Logger.get(OracleQueryRunner.class);

    private static final String ORACLE_CATALOG = "oracle";

    private OracleQueryRunner() {}

    private static QueryRunner createOracleQueryRunner(
            boolean unlockEnterpriseFeatures,
            Map<String, String> connectorProperties,
            Function<Session, Session> sessionModifier,
            Iterable<TpchTable<?>> tables,
            int nodesCount,
            Map<String, String> coordinatorProperties,
            Runnable createUsers,
            Runnable provisionTables)
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel(KerberosConnectionFactory.class.getName(), DEBUG);

        Session session = sessionModifier.apply(createSession(ALICE_USER));
        QueryRunner queryRunner = StarburstDistributedQueryRunner.builder(session)
                .setNodeCount(nodesCount)
                .setCoordinatorProperties(coordinatorProperties)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

            createUsers.run();

            if (unlockEnterpriseFeatures) {
                queryRunner.installPlugin(new TestingStarburstOraclePlugin());
            }
            else {
                queryRunner.installPlugin(new StarburstOraclePlugin());
            }

            queryRunner.createCatalog(ORACLE_CATALOG, ORACLE_CATALOG, connectorProperties);

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx", ImmutableMap.of());

            provisionTables(session, queryRunner, tables);

            provisionTables.run();
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    private static synchronized void provisionTables(Session session, QueryRunner queryRunner, Iterable<TpchTable<?>> tables)
    {
        Set<String> existingTables = queryRunner.listTables(session, ORACLE_CATALOG, session.getSchema().orElse(USER)).stream()
                .map(QualifiedObjectName::getObjectName)
                .collect(toImmutableSet());

        Streams.stream(tables)
                .filter(table -> !existingTables.contains(table.getTableName().toLowerCase(ENGLISH)))
                .forEach(table -> copyTable(queryRunner, "tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH), redirectionDisabled(session)));
    }

    public static Session createSession(String user)
    {
        return createSession(user, USER);
    }

    public static Session createSession(String user, String schema)
    {
        return testSessionBuilder()
                .setCatalog(ORACLE_CATALOG)
                .setSchema(schema)
                .setIdentity(Identity.ofUser(user))
                .build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private boolean unlockEnterpriseFeatures;
        private Map<String, String> connectorProperties = emptyMap();
        private Function<Session, Session> sessionModifier = Function.identity();
        private Iterable<TpchTable<?>> tables = ImmutableList.of();
        private int nodesCount = 3;
        private Map<String, String> coordinatorProperties = emptyMap();
        private Runnable createUsers = OracleTestUsers::createStandardUsers;
        private Runnable provisionTables = Runnables.doNothing();

        private Builder() {}

        public Builder withUnlockEnterpriseFeatures(boolean unlockEnterpriseFeatures)
        {
            this.unlockEnterpriseFeatures = unlockEnterpriseFeatures;
            return this;
        }

        public Builder withConnectorProperties(Map<String, String> connectorProperties)
        {
            this.connectorProperties = requireNonNull(connectorProperties, "connectorProperties is null");
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

        public Builder withNodesCount(int nodesCount)
        {
            verify(nodesCount > 0, "nodesCount should be greater than 0");
            this.nodesCount = nodesCount;
            return this;
        }

        public Builder withCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties = requireNonNull(coordinatorProperties, "coordinatorProperties is null");
            return this;
        }

        public Builder withCreateUsers(Runnable runnable)
        {
            this.createUsers = requireNonNull(runnable, "createUsers is null");
            return this;
        }

        public Builder withProvisionTables(Runnable runnable)
        {
            this.provisionTables = requireNonNull(runnable, "provisionTables is null");
            return this;
        }

        public QueryRunner build()
                throws Exception
        {
            return createOracleQueryRunner(unlockEnterpriseFeatures, connectorProperties, sessionModifier, tables, nodesCount, coordinatorProperties, createUsers, provisionTables);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel(KerberosConnectionFactory.class.getName(), DEBUG);

        // using single node so JMX stats can be queried
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) OracleQueryRunner.builder()
                .withConnectorProperties(connectionProperties())
                .withNodesCount(1)
                .withCoordinatorProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .withTables(TpchTable.getTables())
                .build();

        LOG.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
