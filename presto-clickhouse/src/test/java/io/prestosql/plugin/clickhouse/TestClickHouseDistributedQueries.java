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
package io.prestosql.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.AbstractTestDistributedQueries;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.sql.JdbcSqlExecutor;
import io.prestosql.testing.sql.TestTable;
import io.prestosql.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;

@Test
public class TestClickHouseDistributedQueries
        extends AbstractTestDistributedQueries
{
    private TestingClickHouseServer clickhouseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = new TestingClickHouseServer();
        return createClickHouseQueryRunner(
                clickhouseServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        // caching here speeds up tests highly, caching is not used in smoke tests
                        .put("metadata.cache-ttl", "10m")
                        .put("metadata.cache-missing", "true")
                        .build(),
                TpchTable.getTables());
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        clickhouseServer.close();
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    // @Override
    // protected boolean supportsArrays()
    // {
    //     // Arrays are supported conditionally. Check the defaults.
    //     return new ClickHouseConfig().getArrayMapping() != ClickHouseConfig.ArrayMapping.DISABLED;
    // }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                new JdbcSqlExecutor(clickhouseServer.getJdbcUrl()),
                "tpch.table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    public void testCommentTable()
    {
        // PostgreSQL connector currently does not support comment on table
        assertQueryFails("COMMENT ON TABLE orders IS 'hello'", "This connector does not support setting table comments");
    }

    @Override
    public void testDelete()
    {
        // delete is not supported
    }

    // PostgreSQL specific tests should normally go in TestClickHouseIntegrationSmokeTest
}
