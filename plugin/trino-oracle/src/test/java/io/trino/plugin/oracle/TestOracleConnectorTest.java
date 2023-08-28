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
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.trino.plugin.oracle.OracleClient.ORACLE_MAXIMUM_IDENTIFIER_LENGTH;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_PASS;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_SCHEMA;
import static io.trino.plugin.oracle.TestingOracleServer.TEST_USER;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class TestOracleConnectorTest
        extends BaseOracleConnectorTest
{
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
    public void testJoinPushdownWithLongIdentifiers()
    {
        String maximumColumnIdentifier = "a".repeat(ORACLE_MAXIMUM_IDENTIFIER_LENGTH);

        // Verify that the ORACLE_MAXIMUM_IDENTIFIER_LENGTH is correct for the Oracle version
        // used in the test.
        oracleServer.execute("SELECT 1 AS " + maximumColumnIdentifier + " FROM DUAL");
        assertThatThrownBy(() -> {
            oracleServer.execute("SELECT 1 AS " + maximumColumnIdentifier + "a FROM DUAL");
        }).hasMessageContaining("ORA-00972: identifier is too long");

        try (TestTable table = new TestTable(onRemoteDatabase(), "join_long_", "(%s decimal(19, 0))".formatted(maximumColumnIdentifier))) {
            assertThat(query(joinPushdownEnabled(getSession()), """
                SELECT c.custkey, o.%s, n.nationkey
                FROM %s o JOIN customer c ON c.custkey = o.%s
                JOIN nation n ON c.nationkey = n.nationkey""".formatted(maximumColumnIdentifier, table.getName(), maximumColumnIdentifier)))
            .isFullyPushedDown();
        }
    }
}
