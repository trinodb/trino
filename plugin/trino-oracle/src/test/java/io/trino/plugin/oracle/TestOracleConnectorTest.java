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
import org.testng.annotations.Test;

import static io.trino.plugin.jdbc.ColumnWithAliasFormatter.DEFAULT_COLUMN_ALIAS_LENGTH;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.JOIN_PUSHDOWN_ENABLED;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_SCHEMA;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;

@Test(singleThreaded = true)
public class TestOracleConnectorTest
        extends BaseOracleConnectorTest
{
    private static final int MAX_CHARS_COLUMN_ALIAS = DEFAULT_COLUMN_ALIAS_LENGTH;

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
    public void testPushdownJoinWithLongNameSucceeds()
    {
        tryCleanupTemporaryTable();
        try {
            String baseColumnName = "test_pushdown_" + randomNameSuffix();
            String validColumnName = baseColumnName + "z".repeat(MAX_CHARS_COLUMN_ALIAS - baseColumnName.length());

            assertUpdate(format("""
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
                    """, validColumnName), "VALUES 15000");

            Session session = Session.builder(getSession())
                    .setCatalogSessionProperty("oracle", JOIN_PUSHDOWN_ENABLED, "true")
                    .build();
            assertQuery(session,
                    format("""
                            SELECT c.custkey, o.%s, n.nationkey
                                 FROM orders_1 o JOIN customer c ON c.custkey = o.custkey
                                 JOIN nation n ON c.nationkey = n.nationkey
                            """, validColumnName),
                    """
                                SELECT c.custkey, o.orderkey, n.nationkey
                                FROM orders o JOIN customer c ON c.custkey = o.custkey
                                JOIN nation n ON c.nationkey = n.nationkey
                            """);
        }
        finally {
            tryCleanupTemporaryTable();
        }
    }

    private void tryCleanupTemporaryTable()
    {
        try {
            assertUpdate("DROP TABLE orders_1");
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
