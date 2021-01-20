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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestDistributedQueries;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.JdbcSqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;

@Test
public class TestClickHouseDistributedQueries
        extends AbstractTestDistributedQueries
{
    private TestingClickHouseServer clickhouseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = closeAfterClass(new TestingClickHouseServer());
        // caching here speeds up tests highly, caching is not used in smoke tests
        return createClickHouseQueryRunner(clickhouseServer, ImmutableMap.of(), ImmutableMap.<String, String>builder()
                .put("metadata.cache-ttl", "10m")
                .put("metadata.cache-missing", "true")
                .put("allow-drop-table", "true")
                .build(), TpchTable.getTables());
    }

    @Override
    protected boolean supportsDelete()
    {
        return false;
    }

    @Override
    protected boolean supportsArrays()
    {
        return false;
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
    }

    @Override
    protected boolean supportsCommentOnTable()
    {
        return false;
    }

    @Override
    public void testCommentColumn()
    {
        // currently does not support
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    @Test(dataProvider = "testColumnNameDataProvider")
    public void testColumnName(String columnName)
    {
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testRenameColumn()
    {
        // TODO
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testDropColumn()
    {
        // TODO
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    public void testAddColumn()
    {
        // TODO
        throw new SkipException("TODO: test not implemented yet");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                new JdbcSqlExecutor(clickhouseServer.getJdbcUrl(), clickhouseServer.getProperties()),
                "tpch.tbl",
                "(col_required Int64," +
                        "col_nullable Nullable(Int64)," +
                        "col_default Nullable(Int64) DEFAULT 43," +
                        "col_nonnull_default Int64 DEFAULT 42," +
                        "col_required2 Int64) ENGINE=Log");
    }

    @Override
    @Test(dataProvider = "testDataMappingSmokeTestDataProvider")
    public void testDataMappingSmokeTest(DataMappingTestSetup dataMappingTestSetup)
    {
        // currently does not support
        throw new SkipException("TODO: test not implemented yet");
    }
}
