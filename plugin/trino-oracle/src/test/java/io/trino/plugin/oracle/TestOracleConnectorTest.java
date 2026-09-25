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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.plugin.oracle.TestingOracleServer.TEST_SCHEMA;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestOracleConnectorTest
        extends BaseOracleConnectorTest
{
    private TestingOracleServer oracleServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        oracleServer = new TestingOracleServer();
        return OracleQueryRunner.builder(oracleServer)
                .addConnectorProperties(ImmutableMap.<String, String>builder()
                        .put("oracle.connection-pool.enabled", "false")
                        .put("oracle.remarks-reporting.enabled", "true")
                        .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @AfterAll
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

    @Test
    public void testCreateTableWithIndex()
    {
        try (TestTable table = newTrinoTable(
                "test_create_index_",
                "(a int, b varchar(10)) WITH (index = ARRAY['idx_a(a)'])")) {
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 0");
            assertThat(query("SELECT count(*) FROM TABLE(system.query(query => 'SELECT count(*) FROM user_indexes WHERE table_name = ''" + table.getName().toUpperCase(ENGLISH) + "'''))"))
                    .matches("VALUES BIGINT '1'");
        }
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return new SqlExecutor()
        {
            @Override
            public boolean supportsMultiRowInsert()
            {
                return false;
            }

            @Override
            public void execute(String sql)
            {
                oracleServer.execute(sql);
            }
        };
    }
}
