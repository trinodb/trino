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
package io.trino.plugin.mysql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseAutomaticJoinPushdownTest;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.testng.SkipException;

import static io.trino.plugin.mysql.MySqlQueryRunner.createMySqlQueryRunner;
import static java.lang.String.format;

public class TestMySqlAutomaticJoinPushdown
        extends BaseAutomaticJoinPushdownTest
{
    private TestingMySqlServer mySqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        // Using MySQL 8.0.15 because test is sensitive to quality of NDV estimation and 5.5.46 we use
        // in other places does very poor job; for 10000 row table with 1000 distinct keys it estimates NDV at ~2500
        mySqlServer = closeAfterClass(new TestingMySqlServer("mysql:8.0.30", false));

        return createMySqlQueryRunner(
                mySqlServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("metadata.cache-ttl", "0m")
                        .put("metadata.cache-missing", "false")
                        .buildOrThrow(),
                ImmutableList.of());
    }

    @Override
    public void testJoinPushdownWithEmptyStatsInitially()
    {
        throw new SkipException("MySQL statistics are automatically collected");
    }

    @Override
    protected void gatherStats(String tableName)
    {
        for (MaterializedRow row : computeActual("SHOW COLUMNS FROM " + tableName)) {
            String columnName = (String) row.getField(0);
            String columnType = (String) row.getField(1);
            if (columnType.startsWith("varchar")) {
                // varchar index require length
                continue;
            }
            onRemoteDatabase(format("CREATE INDEX \"%2$s\" ON %1$s (\"%2$s\")", tableName, columnName).replace("\"", "`"));
        }
        onRemoteDatabase("ANALYZE TABLE " + tableName.replace("\"", "`"));
    }

    protected void onRemoteDatabase(String sql)
    {
        try (Handle handle = Jdbi.open(() -> mySqlServer.createConnection())) {
            handle.execute("USE tpch");
            handle.execute(sql);
        }
    }
}
