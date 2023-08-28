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
package io.trino.plugin.oracle;

import com.google.common.collect.ImmutableMap;
import io.airlift.testing.Closeables;
import io.trino.Session;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.COLUMN_ALIAS_MAX_CHARS;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.JOIN_PUSHDOWN_ENABLED;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_SCHEMA;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Test(singleThreaded = true)
public class TestOracleConnectorTest
        extends BaseOracleConnectorTest
{
    private static final String CREATE_ORDERS_SQL = """
        CREATE TABLE orders_1 as
            SELECT orderkey as %s,
                custkey,
                orderstatus,
                totalprice,
                orderdate,
                orderpriority,
                clerk,
                shippriority,
                comment
            FROM orders
    """;

    private static final String SELECT_PUSHDOWN_JOIN = """
       SELECT c.custkey, o.%s, n.nationkey
            FROM %s o JOIN customer c ON c.custkey = o.custkey
            JOIN nation n ON c.nationkey = n.nationkey
    """;
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = new TestingOracleServer();
        return OracleQueryRunner.createOracleQueryRunner(
                oracleServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("connection-url", oracleServer.getJdbcUrl())
                        .put("connection-user", TEST_USER)
                        .put("connection-password", TEST_PASS)
                        .put("oracle.connection-pool.enabled", "false")
                        .put("oracle.remarks-reporting.enabled", "true")
                        .buildOrThrow(),
                REQUIRED_TPCH_TABLES);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws Exception
    {
        Closeables.closeAll(oracleServer);
        oracleServer = null;
    }

    @BeforeMethod
    @AfterMethod(alwaysRun = true)
    public void cleanupTemporaryTable() {
        try {
            assertUpdate("DROP TABLE orders_1");
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * This test helps to tune TupleDomain simplification threshold.
     */
    @Test
    public void testNativeMultipleInClauses()
    {
        String longInClauses = range(0, 10)
                .mapToObj(value -> getLongInClause(value * 1_000, 1_000))
                .collect(joining(" OR "));
        onRemoteDatabase().execute(format("SELECT count(*) FROM %s.orders WHERE %s", TEST_SCHEMA, longInClauses));
    }

    private String getLongInClause(int start, int length)
    {
        String longValues = range(start, start + length)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        return "orderkey IN (" + longValues + ")";
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return oracleServer::execute;
    }

    @Test
    public void testPushdownJoinWithLongNameSucceedsWhenConfiguredCorrectly()
    {
        testPushdownJoinWithLongName(30);
    }

    @Test
    public void testPushdownJoinWithLongNameFailsWhenMaxAllowedCharsIsMisconfigured()
    {
        try {
            testPushdownJoinWithLongName(31);
        }
        catch (AssertionError ex) {
            assertThat(ex.getCause().getCause().getMessage()).isEqualTo("ORA-00972: identifier is too long\n");
        }
    }

    private void testPushdownJoinWithLongName(int maxChars)
    {
        String baseColumnName = "test_pushdown_" + randomNameSuffix();
        int maxLength = 30;
        String validColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());

        assertUpdate(format(CREATE_ORDERS_SQL, validColumnName), "VALUES 15000");
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty("oracle", JOIN_PUSHDOWN_ENABLED, "true")
                .setCatalogSessionProperty("oracle", COLUMN_ALIAS_MAX_CHARS, String.valueOf(maxChars))
                .build();
        assertQuery(
                session,
                format(SELECT_PUSHDOWN_JOIN, validColumnName, "orders_1"),
                format(SELECT_PUSHDOWN_JOIN, "orderkey", "orders"));
    }
}
