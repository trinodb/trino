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

import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.Test;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;

public class TestClickHouseLatestTypeMapping
        extends BaseClickHouseTypeMapping
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickhouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE));
        return createClickHouseQueryRunner(clickhouseServer);
    }

    @Test
    public void testDoubleCorrectness()
    {
        // TODO https://github.com/trinodb/trino/issues/19138 Fix correctness issue
        try (TestTable table = new TestTable(onRemoteDatabase(), "tpch.test_incorrect_double", "(d double) ENGINE=Log")) {
            onRemoteDatabase().execute("INSERT INTO " + table.getName() + " VALUES (CAST('2.225E-307' AS double))");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES CAST('2.225E-307' AS double)");
            assertQuery("SELECT true FROM " + table.getName() + " WHERE d = CAST('2.225E-307' AS double)", "VALUES true");
        }
    }
}
