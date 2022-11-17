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

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createSession;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.ALICE_USER;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.BOB_USER;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.CHARLIE_USER;
import static com.starburstdata.presto.plugin.oracle.OracleTestUsers.UNKNOWN_USER;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.executeInOracle;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.withSynonym;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.withTable;
import static com.starburstdata.presto.plugin.oracle.TestingStarburstOracleServer.withView;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

@Test
public abstract class BaseOracleImpersonationTest
        extends AbstractTestQueryFramework
{
    protected abstract String getProxyUser();

    @Test
    public void testImpersonation()
    {
        String proxyUser = getProxyUser().toUpperCase(ENGLISH);
        assertQuery(
                createSession(ALICE_USER),
                "SELECT * FROM user_context",
                format("SELECT 'ALICE', 'ALICE', 'ALICE', '%s'", proxyUser));
        assertQuery(
                createSession(BOB_USER),
                "SELECT * FROM user_context",
                format("SELECT 'BOB', 'BOB', 'BOB', '%s'", proxyUser));
        assertQueryFails(
                createSession(CHARLIE_USER),
                "SELECT * FROM user_context",
                ".*Table 'oracle.*' does not exist");
        assertQueryFails(
                createSession(UNKNOWN_USER),
                "SELECT * FROM user_context",
                "ORA-01017: invalid username/password; logon denied\n");
    }

    @Test
    public void testAccessToTable()
            throws Exception
    {
        String tableName = getProxyUser() + ".some_table" + "_" + randomNameSuffix();
        try (AutoCloseable ignore = withTable(tableName, "(col_1) AS SELECT 'a' FROM dual")) {
            Session aliceSession = createSession(ALICE_USER, getProxyUser());

            assertQueryFails(
                    aliceSession,
                    "SELECT * FROM " + tableName,
                    ".*Table 'oracle.*' does not exist");

            executeInOracle(format("GRANT SELECT ON %s TO alice", tableName));

            assertQuery(
                    aliceSession,
                    "SELECT * FROM " + tableName,
                    "VALUES 'a'");
        }
    }

    @Test
    public void testAccessToView()
            throws Exception
    {
        String suffix = "_" + randomNameSuffix();
        String tableName = getProxyUser() + ".some_table_for_view" + suffix;
        String viewName = getProxyUser() + ".some_view" + suffix;
        try (AutoCloseable ignore = withTable(tableName, "(col_1) AS SELECT 'a' FROM dual");
                AutoCloseable ignore2 = withView(viewName, "SELECT * FROM " + tableName)) {
            Session aliceSession = createSession(ALICE_USER, getProxyUser());

            assertQueryFails(
                    aliceSession,
                    "SELECT * FROM " + viewName,
                    ".*Table 'oracle.*' does not exist");

            executeInOracle(format("GRANT SELECT ON %s TO alice", viewName));

            assertQuery(
                    aliceSession,
                    "SELECT * FROM " + viewName,
                    "VALUES 'a'");
        }
    }

    @Test
    public void testAccessToSynonym()
            throws Exception
    {
        String suffix = "_" + randomNameSuffix();
        String tableName = getProxyUser() + ".some_table_for_synonym" + suffix;
        String synonymName = getProxyUser() + ".some_synonym" + suffix;
        try (AutoCloseable ignore = withTable(tableName, "(col_1) AS SELECT 'a' FROM dual");
                AutoCloseable ignore2 = withSynonym(synonymName, tableName)) {
            Session aliceSession = createSession(ALICE_USER, getProxyUser());

            assertQueryFails(
                    aliceSession,
                    "SELECT * FROM " + synonymName,
                    ".*Table 'oracle.*' does not exist");

            executeInOracle(format("GRANT SELECT ON %s TO alice", synonymName));

            assertQuery(
                    aliceSession,
                    "SELECT * FROM " + synonymName,
                    "VALUES 'a'");
        }
    }
}
