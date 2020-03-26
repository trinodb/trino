/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starburstdata.presto.plugin.oracle;

import io.prestosql.Session;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.ALICE_USER;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.BOB_USER;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.CHARLIE_USER;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.UNKNOWN_USER;
import static com.starburstdata.presto.plugin.oracle.OracleQueryRunner.createSession;
import static com.starburstdata.presto.plugin.oracle.TestingOracleServer.executeInOracle;
import static io.prestosql.tests.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

@Test
public abstract class BaseOracleImpersonationTest
        extends AbstractTestQueryFramework
{
    protected BaseOracleImpersonationTest(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

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
                ".*Table oracle.* not exist");
        assertQueryFails(
                createSession(UNKNOWN_USER),
                "SELECT * FROM user_context",
                "ORA-01017: invalid username/password; logon denied\n");
    }

    @Test
    public void testAccessToTable()
            throws Exception
    {
        String tableName = getProxyUser() + ".some_table" + "_" + randomTableSuffix();
        try (AutoCloseable ignore = withTable(tableName, "(col_1) AS SELECT 'a' FROM dual")) {
            Session aliceSession = createSession(ALICE_USER, getProxyUser());

            assertQueryFails(
                    aliceSession,
                    "SELECT * FROM " + tableName,
                    ".*Table oracle.* does not exist");

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
        String suffix = "_" + randomTableSuffix();
        String tableName = getProxyUser() + ".some_table_for_view" + suffix;
        String viewName = getProxyUser() + ".some_view" + suffix;
        try (AutoCloseable ignore = withTable(tableName, "(col_1) AS SELECT 'a' FROM dual");
                AutoCloseable ignore2 = withView(viewName, "SELECT * FROM " + tableName)) {
            Session aliceSession = createSession(ALICE_USER, getProxyUser());

            assertQueryFails(
                    aliceSession,
                    "SELECT * FROM " + viewName,
                    ".*Table oracle.* does not exist");

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
        String suffix = "_" + randomTableSuffix();
        String tableName = getProxyUser() + ".some_table_for_synonym" + suffix;
        String synonymName = getProxyUser() + ".some_synonym" + suffix;
        try (AutoCloseable ignore = withTable(tableName, "(col_1) AS SELECT 'a' FROM dual");
                AutoCloseable ignore2 = withSynonym(synonymName, tableName)) {
            Session aliceSession = createSession(ALICE_USER, getProxyUser());

            assertQueryFails(
                    aliceSession,
                    "SELECT * FROM " + synonymName,
                    ".*Table oracle.* does not exist");

            executeInOracle(format("GRANT SELECT ON %s TO alice", synonymName));

            assertQuery(
                    aliceSession,
                    "SELECT * FROM " + synonymName,
                    "VALUES 'a'");
        }
    }

    private static AutoCloseable withTable(String tableName, String tableDefinition)
    {
        executeInOracle(format("CREATE TABLE %s %s", tableName, tableDefinition));
        return () -> executeInOracle(format("DROP TABLE %s", tableName));
    }

    private static AutoCloseable withView(String tableName, String tableDefinition)
    {
        executeInOracle(format("CREATE VIEW %s AS %s", tableName, tableDefinition));
        return () -> executeInOracle(format("DROP VIEW %s", tableName));
    }

    private static AutoCloseable withSynonym(String tableName, String tableDefinition)
    {
        executeInOracle(format("CREATE SYNONYM %s FOR %s", tableName, tableDefinition));
        return () -> executeInOracle(format("DROP SYNONYM %s", tableName));
    }
}
