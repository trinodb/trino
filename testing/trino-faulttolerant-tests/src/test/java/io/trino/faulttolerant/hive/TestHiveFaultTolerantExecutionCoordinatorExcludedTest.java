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
package io.trino.faulttolerant.hive;

import io.trino.Session;
import io.trino.plugin.exchange.filesystem.FileSystemExchangePlugin;
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.FaultTolerantExecutionConnectorTestHelper;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;

import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerProperties;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

/**
 * Smoke test for fault-tolerant execution when scheduling tasks on coordinator is disabled.
 */
@Isolated
@TestInstance(PER_CLASS)
public class TestHiveFaultTolerantExecutionCoordinatorExcludedTest
        extends AbstractTestQueryFramework
{
    private MinioStorage minioStorage;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.minioStorage = new MinioStorage("test-exchange-spooling-" + randomNameSuffix());
        minioStorage.start();

        return HiveQueryRunner.builder()
                .setExtraProperties(FaultTolerantExecutionConnectorTestHelper.getExtraProperties())
                .addCoordinatorProperty("node-scheduler.include-coordinator", "false")
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new FileSystemExchangePlugin());
                    runner.loadExchangeManager("filesystem", getExchangeManagerProperties(minioStorage));
                })
                .setInitialTables(List.of(TpchTable.NATION))
                .build();
    }

    @Test
    public void testInsert()
    {
        String query = "SELECT name, nationkey, regionkey FROM nation";

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_", "AS " + query + " WITH NO DATA")) {
            assertQuery("SELECT count(*) FROM " + table.getName(), "SELECT 0");

            assertUpdate("INSERT INTO " + table.getName() + " " + query, 25);

            assertQuery("SELECT * FROM " + table.getName(), query);

            assertUpdate("INSERT INTO " + table.getName() + " (nationkey) VALUES (-1)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (nationkey) VALUES (null)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (name) VALUES ('name-dummy-1')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (nationkey, name) VALUES (-2, 'name-dummy-2')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (name, nationkey) VALUES ('name-dummy-3', -3)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (regionkey) VALUES (1234)", 1);

            assertQuery("SELECT * FROM " + table.getName(), query
                    + " UNION ALL SELECT null, -1, null"
                    + " UNION ALL SELECT null, null, null"
                    + " UNION ALL SELECT 'name-dummy-1', null, null"
                    + " UNION ALL SELECT 'name-dummy-2', -2, null"
                    + " UNION ALL SELECT 'name-dummy-3', -3, null"
                    + " UNION ALL SELECT null, null, 1234");

            // UNION query produces columns in the opposite order
            // of how they are declared in the table schema
            assertUpdate(
                    "INSERT INTO " + table.getName() + " (nationkey, name, regionkey) " +
                            "SELECT nationkey, name, regionkey FROM nation " +
                            "UNION ALL " +
                            "SELECT nationkey, name, regionkey FROM nation",
                    50);
        }
    }

    @Test
    public void testCreateTableAsSelect()
    {
        String tableName = "test_ctas" + randomNameSuffix();

        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames(tableName, "name", "regionkey");
        assertUpdate("DROP TABLE " + tableName);

        // existing table
        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT nationkey, regionkey FROM nation", 0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

        Session session = getSession();
        String table = "test_ctas_" + randomNameSuffix();
        assertUpdate(session, "CREATE TABLE " + table + " AS " + "SELECT nationkey, name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertQuery(session, "SELECT * FROM " + table, "SELECT nationkey, name, regionkey FROM nation");
        assertUpdate(session, "DROP TABLE " + table);

        assertThat(getQueryRunner().tableExists(session, table)).isFalse();
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        if (minioStorage != null) {
            minioStorage.close();
            minioStorage = null;
        }
    }
}
