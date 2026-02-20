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
package io.trino.tests.product.hudi;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Hudi and Spark compatibility.
 * Uses HDFS for table storage instead of S3.
 */
@ProductTest
@RequiresEnvironment(HudiEnvironment.class)
@TestGroup.Hudi
class TestHudiSparkCompatibility
{
    private static final String COW_TABLE_TYPE = "cow";
    private static final String MOR_TABLE_TYPE = "mor";

    @Test
    void testCopyOnWriteShowCreateTable(HudiEnvironment env)
    {
        String tableName = "test_hudi_cow_show_create_" + randomNameSuffix();
        String warehouseDir = env.getWarehouseDirectory();

        createNonPartitionedTable(env, tableName, COW_TABLE_TYPE);

        try {
            String trinoShowCreate = (String) env.executeTrino("SHOW CREATE TABLE hudi.default." + tableName).getOnlyValue();
            assertThat(trinoShowCreate)
                    .contains("CREATE TABLE hudi.default." + tableName)
                    .contains("_hoodie_commit_time varchar")
                    .contains("_hoodie_commit_seqno varchar")
                    .contains("_hoodie_record_key varchar")
                    .contains("_hoodie_partition_path varchar")
                    .contains("_hoodie_file_name varchar")
                    .contains("id bigint")
                    .contains("name varchar")
                    .contains("price integer")
                    .contains("ts bigint")
                    .contains("location = 'hdfs://hadoop-master:9000" + warehouseDir + "/" + tableName + "'");

            String sparkShowCreate = (String) env.executeSpark("SHOW CREATE TABLE default." + tableName).getOnlyValue();
            assertThat(sparkShowCreate)
                    .contains("CREATE TABLE default." + tableName)
                    .contains("USING hudi")
                    .contains("LOCATION 'hdfs://hadoop-master:9000" + warehouseDir + "/" + tableName + "'")
                    .contains("'type' = 'cow'");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE default." + tableName);
        }
    }

    @Test
    void testCopyOnWriteTableSelect(HudiEnvironment env)
    {
        String tableName = "test_hudi_cow_select_" + randomNameSuffix();

        createNonPartitionedTable(env, tableName, COW_TABLE_TYPE);

        List<Row> expectedRows = List.of(
                row(1L, "a1"),
                row(2L, "a2"));

        try {
            assertThat(env.executeSpark("SELECT id, name FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT id, name FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE default." + tableName);
        }
    }

    @Test
    void testCopyOnWritePartitionedTableSelect(HudiEnvironment env)
    {
        String tableName = "test_hudi_cow_partitioned_select_" + randomNameSuffix();

        createPartitionedTable(env, tableName, COW_TABLE_TYPE);

        List<Row> expectedRows = List.of(
                row(1L, "a1", 1000L, "2021-12-09", "10"),
                row(2L, "a2", 1000L, "2021-12-09", "11"));

        try {
            assertThat(env.executeSpark("SELECT id, name, ts, dt, hh FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT id, name, ts, dt, hh FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);

            List<Row> filteredRows = List.of(row(2L, "a2", 1000L));
            assertThat(env.executeTrino("SELECT id, name, ts FROM hudi.default." + tableName + " WHERE dt = '2021-12-09' AND hh = '11'"))
                    .containsOnly(filteredRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE default." + tableName);
        }
    }

    @Test
    void testCopyOnWriteTableSelectAfterUpdate(HudiEnvironment env)
    {
        String tableName = "test_hudi_cow_select_after_update" + randomNameSuffix();

        createPartitionedTable(env, tableName, COW_TABLE_TYPE);

        List<Row> expectedRows = List.of(
                row(1L, "a1"),
                row(2L, "a2"));

        try {
            assertThat(env.executeSpark("SELECT id, name FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT id, name FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);

            env.executeSparkUpdate("UPDATE default." + tableName + " SET name = 'a1_1', ts = 1001 WHERE id = 1");
            List<Row> expectedRowsAfterUpdate = List.of(
                    row(1L, "a1_1", 1001L),
                    row(2L, "a2", 1000L));
            assertThat(env.executeSpark("SELECT id, name, ts FROM default." + tableName))
                    .containsOnly(expectedRowsAfterUpdate);
            assertThat(env.executeTrino("SELECT id, name, ts FROM hudi.default." + tableName))
                    .containsOnly(expectedRowsAfterUpdate);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE default." + tableName);
        }
    }

    @Test
    void testMergeOnReadTableSelect(HudiEnvironment env)
    {
        String tableName = "test_hudi_mor_select_" + randomNameSuffix();

        createNonPartitionedTable(env, tableName, MOR_TABLE_TYPE);

        List<Row> expectedRows = List.of(
                row(1L, "a1", 20, 1000L),
                row(2L, "a2", 40, 2000L));

        try {
            assertThat(env.executeSpark("SELECT id, name, price, ts FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT id, name, price, ts FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE default." + tableName);
        }
    }

    @Test
    void testMergeOnReadTableSelectAfterUpdate(HudiEnvironment env)
    {
        String tableName = "test_hudi_mor_update" + randomNameSuffix();

        createNonPartitionedTable(env, tableName, MOR_TABLE_TYPE);

        List<Row> expectedRows = List.of(
                row(1L, "a1", 20, 1000L),
                row(2L, "a2", 40, 2000L));

        try {
            assertThat(env.executeSpark("SELECT id, name, price, ts FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT id, name, price, ts FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);

            env.executeSparkUpdate("UPDATE default." + tableName + " SET ts = 2020 WHERE id = 2");
            List<Row> expectedRowsAfterUpdate = List.of(
                    row(1L, "a1", 20, 1000L),
                    row(2L, "a2", 40, 2020L));
            assertThat(env.executeSpark("SELECT id, name, price, ts FROM default." + tableName))
                    .containsOnly(expectedRowsAfterUpdate);
            // NOTE: MOR Snapshot queries are not supported yet.
            // "_ro" suffix to the table indicates read-optimized query.
            assertThat(env.executeTrino("SELECT id, name, price, ts FROM hudi.default." + tableName + "_ro"))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE default." + tableName);
        }
    }

    @Test
    void testMergeOnReadPartitionedTableSelect(HudiEnvironment env)
    {
        String tableName = "test_hudi_mor_partitioned_select_" + randomNameSuffix();

        createPartitionedTable(env, tableName, MOR_TABLE_TYPE);

        List<Row> expectedRows = List.of(
                row(1L, "a1", 1000L, "2021-12-09", "10"),
                row(2L, "a2", 1000L, "2021-12-09", "11"));

        try {
            assertThat(env.executeSpark("SELECT id, name, ts, dt, hh FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT id, name, ts, dt, hh FROM hudi.default." + tableName + "_ro"))
                    .containsOnly(expectedRows);

            List<Row> filteredRows = List.of(row(2L, "a2", 1000L));
            assertThat(env.executeTrino("SELECT id, name, ts FROM hudi.default." + tableName + "_ro WHERE dt = '2021-12-09' AND hh = '11'"))
                    .containsOnly(filteredRows);
        }
        finally {
            env.executeSparkUpdate("DROP TABLE default." + tableName);
        }
    }

    @Test
    void testCopyOnWriteTableSelectWithSessionProperties(HudiEnvironment env)
    {
        String tableName = "test_hudi_cow_select_session_props" + randomNameSuffix();

        createNonPartitionedTable(env, tableName, COW_TABLE_TYPE);

        try {
            assertThat(env.executeTrino("SELECT id, name FROM hudi.default." + tableName))
                    .containsOnly(List.of(
                            row(1L, "a1"),
                            row(2L, "a2")));
            // Execute SET SESSION and SELECT in the same session
            env.executeTrinoInSession(session -> {
                session.executeUpdate(
                        "SET SESSION hudi.columns_to_hide = ARRAY['_hoodie_commit_time','_hoodie_commit_seqno','_hoodie_record_key','_hoodie_partition_path','_hoodie_file_name']");
                assertThat(session.executeQuery("SELECT * FROM hudi.default." + tableName).rows())
                        .containsExactlyInAnyOrder(
                                List.of(1L, "a1", 20, 1000L),
                                List.of(2L, "a2", 40, 2000L));
            });
        }
        finally {
            env.executeSparkUpdate("DROP TABLE default." + tableName);
        }
    }

    @Test
    void testTimelineTable(HudiEnvironment env)
    {
        String tableName = "test_hudi_timeline_system_table_" + randomNameSuffix();
        createNonPartitionedTable(env, tableName, COW_TABLE_TYPE);
        try {
            assertThat(env.executeTrino("SELECT action, state FROM hudi.default.\"" + tableName + "$timeline\""))
                    .containsOnly(row("commit", "COMPLETED"));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testTimelineTableRedirect(HudiEnvironment env)
    {
        String tableName = "test_hudi_timeline_system_table_redirect_" + randomNameSuffix();
        String nonExistingTableName = tableName + "_non_existing";
        createNonPartitionedTable(env, tableName, COW_TABLE_TYPE);
        try {
            assertThat(env.executeTrino("SELECT action, state FROM hive.default.\"" + tableName + "$timeline\""))
                    .containsOnly(row("commit", "COMPLETED"));
            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.default.\"" + nonExistingTableName + "$timeline\""))
                    .rootCause()
                    .hasMessageContaining("Table 'hive.default.\"" + nonExistingTableName + "$timeline\"' does not exist");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    void testReadCopyOnWriteTableWithReplaceCommits(HudiEnvironment env)
    {
        String tableName = "test_hudi_cow_replace_commits_select_" + randomNameSuffix();
        String warehouseDir = env.getWarehouseDirectory();

        env.executeSparkUpdate("CREATE TABLE default." + tableName +
                "(id bigint, name string, ts bigint)" +
                "USING hudi " +
                "TBLPROPERTIES (" +
                " type = 'cow'," +
                " primaryKey = 'id'," +
                " preCombineField = 'ts'," +
                " hoodie.clustering.inline = 'true'," +
                " hoodie.clustering.inline.max.commits = '1')" +
                "LOCATION 'hdfs://hadoop-master:9000" + warehouseDir + "/" + tableName + "'");

        try {
            env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'a1', 1000), (2, 'a2', 2000)");
            assertThat(env.executeTrino("SELECT id, name FROM hudi.default." + tableName))
                    .containsOnly(row(1L, "a1"), row(2L, "a2"));
        }
        finally {
            env.executeSparkUpdate("DROP TABLE default." + tableName);
        }
    }

    private void createNonPartitionedTable(HudiEnvironment env, String tableName, String tableType)
    {
        String warehouseDir = env.getWarehouseDirectory();

        env.executeSparkUpdate(
                "CREATE TABLE default." + tableName + " (" +
                        "  id bigint," +
                        "  name string," +
                        "  price int," +
                        "  ts bigint)" +
                        "USING hudi " +
                        "TBLPROPERTIES (" +
                        "  type = '" + tableType + "'," +
                        "  primaryKey = 'id'," +
                        "  preCombineField = 'ts')" +
                        "LOCATION 'hdfs://hadoop-master:9000" + warehouseDir + "/" + tableName + "'");

        env.executeSparkUpdate("INSERT INTO default." + tableName + " VALUES (1, 'a1', 20, 1000), (2, 'a2', 40, 2000)");
    }

    private void createPartitionedTable(HudiEnvironment env, String tableName, String tableType)
    {
        String warehouseDir = env.getWarehouseDirectory();

        env.executeSparkUpdate(
                "CREATE TABLE default." + tableName + " (" +
                        "  id bigint," +
                        "  name string," +
                        "  ts bigint," +
                        "  dt string," +
                        "  hh string)" +
                        "USING hudi " +
                        "TBLPROPERTIES (" +
                        "  type = '" + tableType + "'," +
                        "  primaryKey = 'id'," +
                        "  preCombineField = 'ts')" +
                        "PARTITIONED BY (dt, hh) " +
                        "LOCATION 'hdfs://hadoop-master:9000" + warehouseDir + "/" + tableName + "'");

        env.executeSparkUpdate("INSERT INTO default." + tableName + " PARTITION (dt, hh) SELECT 1 AS id, 'a1' AS name, 1000 AS ts, '2021-12-09' AS dt, '10' AS hh");
        env.executeSparkUpdate("INSERT INTO default." + tableName + " PARTITION (dt = '2021-12-09', hh='11') SELECT 2, 'a2', 1000");
    }
}
