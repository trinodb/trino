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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.jdbc.KerberosConnectionFactory;
import io.prestosql.plugin.jmx.JmxPlugin;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.USER;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.executeInOracle;
import static io.airlift.log.Level.DEBUG;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tests.QueryAssertions.copyTable;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class OracleQueryRunner
{
    private static final Logger LOG = Logger.get(OracleQueryRunner.class);

    private static final String ORACLE_CATALOG = "oracle";
    public static final String ALICE_USER = "alice";
    public static final String BOB_USER = "bob";
    public static final String CHARLIE_USER = "charlie";
    public static final String UNKNOWN_USER = "non_existing_user";

    private OracleQueryRunner() {}

    public static QueryRunner createOracleQueryRunner(
            Map<String, String> connectorProperties,
            Function<Session, Session> sessionModifier,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createOracleQueryRunner(connectorProperties, sessionModifier, tables, 2);
    }

    private static QueryRunner createOracleQueryRunner(
            Map<String, String> connectorProperties,
            Function<Session, Session> sessionModifier,
            Iterable<TpchTable<?>> tables,
            int nodesCount)
            throws Exception
    {
        return createOracleQueryRunner(connectorProperties, sessionModifier, tables, nodesCount, ImmutableMap.of());
    }

    public static QueryRunner createOracleQueryRunner(
            Map<String, String> connectorProperties,
            Function<Session, Session> sessionModifier,
            Iterable<TpchTable<?>> tables,
            int nodesCount,
            Map<String, String> coordinatorProperties)
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel(KerberosConnectionFactory.class.getName(), DEBUG);

        Session session = sessionModifier.apply(createSession(ALICE_USER));
        QueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(nodesCount)
                .setCoordinatorProperties(coordinatorProperties)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

            String kerberizedUser = "test";
            createKerberizedUser(kerberizedUser);

            createUser(ALICE_USER, kerberizedUser);
            createUser(BOB_USER, kerberizedUser);
            createUser(CHARLIE_USER, kerberizedUser);

            executeInOracle(
                    "CREATE OR REPLACE VIEW user_context AS " +
                            "SELECT " +
                            "sys_context('USERENV', 'SESSION_USER') AS session_user_column," +
                            "sys_context('USERENV', 'SESSION_SCHEMA') AS session_schema_column," +
                            "sys_context('USERENV', 'CURRENT_SCHEMA') AS current_schema_column," +
                            "sys_context('USERENV', 'PROXY_USER') AS proxy_user_column " +
                            "FROM dual");
            executeInOracle(format("GRANT SELECT ON user_context to %s", ALICE_USER));
            executeInOracle(format("GRANT SELECT ON user_context to %s", BOB_USER));

            queryRunner.installPlugin(new OraclePlugin());
            queryRunner.createCatalog(ORACLE_CATALOG, ORACLE_CATALOG, connectorProperties);

            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx", ImmutableMap.of());

            provisionTables(session, queryRunner, tables);
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    private static void createKerberizedUser(String user)
    {
        try {
            executeInOracle(format("CREATE USER %s IDENTIFIED EXTERNALLY AS '%s@TESTING-KRB.STARBURSTDATA.COM'", user, user));
        }
        catch (RuntimeException e) {
            propagateUnlessUserAlreadyExists(e);
        }
        executeInOracle("GRANT CONNECT,RESOURCE TO " + user);
        executeInOracle("GRANT UNLIMITED TABLESPACE TO " + user);
        executeInOracle("GRANT CREATE ANY TABLE TO " + user);
        executeInOracle("GRANT DROP ANY TABLE TO " + user);
        executeInOracle("GRANT INSERT ANY TABLE TO " + user);
        executeInOracle("GRANT SELECT ANY TABLE TO " + user);
        executeInOracle("GRANT ALTER ANY TABLE TO " + user);
        executeInOracle("GRANT LOCK ANY TABLE TO " + user);
        executeInOracle("GRANT ANALYZE ANY TO " + user);
    }

    private static void createUser(String user, String kerberizedUser)
    {
        try {
            executeInOracle(format("CREATE USER %s IDENTIFIED BY \"vier1Str0ngP@55vvord\"", user));
        }
        catch (RuntimeException e) {
            propagateUnlessUserAlreadyExists(e);
        }
        executeInOracle(format("ALTER USER %s GRANT CONNECT THROUGH %s", user, USER));
        executeInOracle(format("ALTER USER %s GRANT CONNECT THROUGH %s", user, kerberizedUser));
        executeInOracle(format("ALTER USER %s QUOTA UNLIMITED ON USERS", user));
        executeInOracle(format("GRANT CREATE SESSION TO %s", user));
    }

    private static void propagateUnlessUserAlreadyExists(RuntimeException e)
    {
        if (e.getCause() instanceof SQLException && ((SQLException) e.getCause()).getErrorCode() == 1920) {
            // ORA-01920: user name '<user>' conflicts with another user or role name
        }
        else {
            throw e;
        }
    }

    private static synchronized void provisionTables(Session session, QueryRunner queryRunner, Iterable<TpchTable<?>> tables)
    {
        Set<String> existingTables = queryRunner.listTables(session, ORACLE_CATALOG, session.getSchema().orElse(USER)).stream()
                .map(QualifiedObjectName::getObjectName)
                .collect(toImmutableSet());

        Streams.stream(tables)
                .filter(table -> !existingTables.contains(table.getTableName().toLowerCase(ENGLISH)))
                .forEach(table -> copyTable(queryRunner, "tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH), session));
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
                .setIdentity(new Identity(user, Optional.empty()))
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel(KerberosConnectionFactory.class.getName(), DEBUG);

        // using single node so JMX stats can be queried
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) createOracleQueryRunner(
                ImmutableMap.of(),
                Function.identity(),
                TpchTable.getTables(),
                1,
                ImmutableMap.of("http-server.http.port", "8080"));
        LOG.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
