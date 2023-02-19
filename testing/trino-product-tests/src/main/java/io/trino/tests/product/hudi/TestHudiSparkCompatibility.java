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

import com.google.common.collect.ImmutableList;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HUDI;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onHudi;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestHudiSparkCompatibility
        extends ProductTest
{
    private static final String COW_TABLE_TYPE = "cow";
    private static final String MOR_TABLE_TYPE = "mor";

    private String bucketName;

    @BeforeTestWithContext
    public void setUp()
    {
        bucketName = requireNonNull(System.getenv("S3_BUCKET"), "Environment variable not set: S3_BUCKET");
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteShowCreateTable()
    {
        String tableName = "test_hudi_cow_show_create_" + randomNameSuffix();

        createNonPartitionedTable(tableName, COW_TABLE_TYPE);

        try {
            Assertions.assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE hudi.default." + tableName).getOnlyValue())
                    .isEqualTo(format(
                            "CREATE TABLE hudi.default.%s (\n" +
                                    "   _hoodie_commit_time varchar,\n" +
                                    "   _hoodie_commit_seqno varchar,\n" +
                                    "   _hoodie_record_key varchar,\n" +
                                    "   _hoodie_partition_path varchar,\n" +
                                    "   _hoodie_file_name varchar,\n" +
                                    "   id bigint,\n" +
                                    "   name varchar,\n" +
                                    "   price integer,\n" +
                                    "   ts bigint\n" +
                                    ")\n" +
                                    "WITH (\n" +
                                    "   location = 's3://%s/%s'\n" +
                                    ")",
                            tableName,
                            bucketName,
                            tableName));
            String lastCommitTimeSync = (String) onHudi().executeQuery("show TBLPROPERTIES " + tableName + " ('last_commit_time_sync')").project(2).getOnlyValue();
            Assertions.assertThat((String) onHudi().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .isEqualTo(format("""
                                    CREATE TABLE default.%s (
                                      _hoodie_commit_time STRING,
                                      _hoodie_commit_seqno STRING,
                                      _hoodie_record_key STRING,
                                      _hoodie_partition_path STRING,
                                      _hoodie_file_name STRING,
                                      id BIGINT,
                                      name STRING,
                                      price INT,
                                      ts BIGINT)
                                    USING hudi
                                    LOCATION 's3://%s/%s'
                                    TBLPROPERTIES (
                                      'last_commit_time_sync' = '%s',
                                      'preCombineField' = 'ts',
                                      'primaryKey' = 'id',
                                      'type' = 'cow')
                                    """,
                            tableName,
                            bucketName,
                            tableName,
                            lastCommitTimeSync));
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSelect()
    {
        String tableName = "test_hudi_cow_select_" + randomNameSuffix();

        createNonPartitionedTable(tableName, COW_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1"),
                row(2, "a2"));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWritePartitionedTableSelect()
    {
        String tableName = "test_hudi_cow_partitioned_select_" + randomNameSuffix();

        createPartitionedTable(tableName, COW_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1", 1000, "2021-12-09", "10"),
                row(2, "a2", 1000, "2021-12-09", "11"));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name, ts, dt, hh FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name, ts, dt, hh FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);

            expectedRows = ImmutableList.of(row(2, "a2", 1000));
            assertThat(onTrino().executeQuery("SELECT id, name, ts FROM hudi.default." + tableName + " WHERE dt = '2021-12-09' AND hh = '11'"))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSelectAfterUpdate()
    {
        String tableName = "test_hudi_cow_select_after_update" + randomNameSuffix();

        createPartitionedTable(tableName, COW_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1"),
                row(2, "a2"));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);

            onHudi().executeQuery("UPDATE default." + tableName + " SET name = 'a1_1', ts = 1001 WHERE id = 1");
            expectedRows = ImmutableList.of(
                    row(1, "a1_1", 1001),
                    row(2, "a2", 1000));
            assertThat(onHudi().executeQuery("SELECT id, name, ts FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name, ts FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testMergeOnReadTableSelect()
    {
        String tableName = "test_hudi_mor_select_" + randomNameSuffix();

        createNonPartitionedTable(tableName, MOR_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1", 20, 1000),
                row(2, "a2", 40, 2000));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name, price, ts FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name, price, ts FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testMergeOnReadTableSelectAfterUpdate()
    {
        String tableName = "test_hudi_mor_update" + randomNameSuffix();

        createNonPartitionedTable(tableName, MOR_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1", 20, 1000),
                row(2, "a2", 40, 2000));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name, price, ts FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name, price, ts FROM hudi.default." + tableName))
                    .containsOnly(expectedRows);

            onHudi().executeQuery("UPDATE default." + tableName + " SET ts = 2020 WHERE id = 2");
            List<QueryAssert.Row> expectedRowsAfterUpdate = ImmutableList.of(
                    row(1, "a1", 20, 1000),
                    row(2, "a2", 40, 2020));
            assertThat(onHudi().executeQuery("SELECT id, name, price, ts FROM default." + tableName))
                    .containsOnly(expectedRowsAfterUpdate);
            // NOTE: MOR Snapshot queries are not supported yet.
            // "_ro" suffix to the table indicates read-optimized query.
            assertThat(onTrino().executeQuery("SELECT id, name, price, ts FROM hudi.default." + tableName + "_ro"))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testMergeOnReadPartitionedTableSelect()
    {
        String tableName = "test_hudi_mor_partitioned_select_" + randomNameSuffix();

        createPartitionedTable(tableName, MOR_TABLE_TYPE);

        List<QueryAssert.Row> expectedRows = ImmutableList.of(
                row(1, "a1", 1000, "2021-12-09", "10"),
                row(2, "a2", 1000, "2021-12-09", "11"));

        try {
            assertThat(onHudi().executeQuery("SELECT id, name, ts, dt, hh FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(onTrino().executeQuery("SELECT id, name, ts, dt, hh FROM hudi.default." + tableName + "_ro"))
                    .containsOnly(expectedRows);

            expectedRows = ImmutableList.of(row(2, "a2", 1000));
            assertThat(onTrino().executeQuery("SELECT id, name, ts FROM hudi.default." + tableName + "_ro WHERE dt = '2021-12-09' AND hh = '11'"))
                    .containsOnly(expectedRows);
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testCopyOnWriteTableSelectWithSessionProperties()
    {
        String tableName = "test_hudi_cow_select_session_props" + randomNameSuffix();

        createNonPartitionedTable(tableName, COW_TABLE_TYPE);

        try {
            assertThat(onTrino().executeQuery("SELECT id, name FROM hudi.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row(1, "a1"),
                            row(2, "a2")));
            onTrino().executeQuery(
                    "SET SESSION hudi.columns_to_hide = ARRAY['_hoodie_commit_time','_hoodie_commit_seqno','_hoodie_record_key','_hoodie_partition_path','_hoodie_file_name']");
            assertThat(onTrino().executeQuery("SELECT * FROM hudi.default." + tableName))
                    .containsOnly(ImmutableList.of(
                            row(1, "a1", 20, 1000),
                            row(2, "a2", 40, 2000)));
        }
        finally {
            onHudi().executeQuery("DROP TABLE default." + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testTimelineTable()
    {
        String tableName = "test_hudi_timeline_system_table_" + randomNameSuffix();
        createNonPartitionedTable(tableName, COW_TABLE_TYPE);
        try {
            assertThat(onTrino().executeQuery(format("SELECT action, state FROM hudi.default.\"%s$timeline\"", tableName)))
                    .containsOnly(row("commit", "COMPLETED"));
        }
        finally {
            onHudi().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testTimelineTableRedirect()
    {
        String tableName = "test_hudi_timeline_system_table_redirect_" + randomNameSuffix();
        String nonExistingTableName = tableName + "_non_existing";
        createNonPartitionedTable(tableName, COW_TABLE_TYPE);
        try {
            assertThat(onTrino().executeQuery(format("SELECT action, state FROM hive.default.\"%s$timeline\"", tableName)))
                    .containsOnly(row("commit", "COMPLETED"));
            assertQueryFailure(() -> onTrino().executeQuery(format("SELECT * FROM hive.default.\"%s$timeline\"", nonExistingTableName)))
                    .hasMessageMatching(".*Table 'hive.default.test_hudi_timeline_system_table_redirect_.*_non_existing\\$timeline' does not exist");
        }
        finally {
            onHudi().executeQuery("DROP TABLE " + tableName);
        }
    }

    private void createNonPartitionedTable(String tableName, String tableType)
    {
        onHudi().executeQuery(format(
                """
                        CREATE TABLE default.%s (
                          id bigint,
                          name string,
                          price int,
                          ts bigint)
                        USING hudi
                        TBLPROPERTIES (
                          type = '%s',
                          primaryKey = 'id',
                          preCombineField = 'ts')
                        LOCATION 's3://%s/%s'""",
                tableName,
                tableType,
                bucketName,
                tableName));

        onHudi().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 'a1', 20, 1000), (2, 'a2', 40, 2000)");
    }

    private void createPartitionedTable(String tableName, String tableType)
    {
        onHudi().executeQuery(format(
                """
                        CREATE TABLE default.%s (
                          id bigint,
                          name string,
                          ts bigint,
                          dt string,
                          hh string)
                        USING hudi
                        TBLPROPERTIES (
                          type = '%s',
                          primaryKey = 'id',
                          preCombineField = 'ts')
                        PARTITIONED BY (dt, hh)
                        LOCATION 's3://%s/%s'""",
                tableName,
                tableType,
                bucketName,
                tableName));

        onHudi().executeQuery("INSERT INTO default." + tableName + " PARTITION (dt, hh) SELECT 1 AS id, 'a1' AS name, 1000 AS ts, '2021-12-09' AS dt, '10' AS hh");
        onHudi().executeQuery("INSERT INTO default." + tableName + " PARTITION (dt = '2021-12-09', hh='11') SELECT 2, 'a2', 1000");
    }
}
