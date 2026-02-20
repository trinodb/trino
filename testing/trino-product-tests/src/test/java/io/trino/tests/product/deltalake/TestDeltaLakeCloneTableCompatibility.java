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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.minio.MinioClient;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests Delta Lake clone table compatibility between Trino and Spark.
 * <p>
 * Tests that are only DELTA_LAKE_DATABRICKS are not included here.
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeCloneTableCompatibility
{
    @Test
    void testTableChangesOnShallowCloneTable(DeltaLakeMinioEnvironment env)
    {
        String bucketName = env.getBucketName();
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String clonedTable = "test_dl_clone_tableV1_" + randomNameSuffix();
        String directoryName = "databricks-tablechanges-compatibility-test-";
        String changeDataPrefix = "/_change_data";
        try {
            env.executeSparkUpdate("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    "LOCATION 's3://" + bucketName + "/" + directoryName + baseTable + "'");
            env.executeSparkUpdate("INSERT INTO default." + baseTable + " VALUES (1, 'a')");
            env.executeSparkUpdate("CREATE TABLE default." + clonedTable +
                    " SHALLOW CLONE default." + baseTable +
                    " TBLPROPERTIES (delta.enableChangeDataFeed = true)" +
                    " LOCATION 's3://" + bucketName + "/" + directoryName + clonedTable + "'");
            env.executeSparkUpdate("INSERT INTO default." + clonedTable + " VALUES (2, 'b')");

            List<String> cdfFilesPostOnlyInsert = getFilesFromTableDirectory(env, bucketName, directoryName + clonedTable + changeDataPrefix);
            assertThat(cdfFilesPostOnlyInsert).isEmpty();

            env.executeSparkUpdate("UPDATE default." + clonedTable + " SET a_int = a_int + 1");
            List<String> cdfFilesPostOnlyInsertAndUpdate = getFilesFromTableDirectory(env, bucketName, directoryName + clonedTable + changeDataPrefix);
            assertThat(cdfFilesPostOnlyInsertAndUpdate).hasSize(2);

            List<Row> expectedRowsClonedTableOnTrino = List.of(
                    row(2, "b", "insert", 1L),
                    row(1, "a", "update_preimage", 2L),
                    row(2, "a", "update_postimage", 2L),
                    row(2, "b", "update_preimage", 2L),
                    row(3, "b", "update_postimage", 2L));
            // TODO https://github.com/trinodb/trino/issues/21183 Fix below assertion when Trino is able to infer `base table inserts on shallow cloned table`
            assertThat(env.executeTrino("SELECT a_int, b_string, _change_type, _commit_version FROM TABLE(delta.system.table_changes('default', '" + clonedTable + "', 0))"))
                    .containsOnly(expectedRowsClonedTableOnTrino);

            List<Row> expectedRowsClonedTableOnSpark = List.of(
                    row(1, "a", "insert", 0L),
                    row(2, "b", "insert", 1L),
                    row(1, "a", "update_preimage", 2L),
                    row(2, "a", "update_postimage", 2L),
                    row(2, "b", "update_preimage", 2L),
                    row(3, "b", "update_postimage", 2L));
            assertThat(env.executeSpark(
                    "SELECT a_int, b_string, _change_type, _commit_version FROM table_changes('default." + clonedTable + "', 0)"))
                    .containsOnly(expectedRowsClonedTableOnSpark);

            List<Row> expectedRows = List.of(row(2, "a"), row(3, "b"));
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTable)).containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTable)).containsOnly(expectedRows);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + baseTable);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + clonedTable);
        }
    }

    @Test
    void testShallowCloneTableDrop(DeltaLakeMinioEnvironment env)
    {
        String bucketName = env.getBucketName();
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String clonedTable = "test_dl_clone_tableV1_" + randomNameSuffix();
        String directoryName = "databricks-shallowclone-drop-compatibility-test-";
        try {
            env.executeSparkUpdate("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    "LOCATION 's3://" + bucketName + "/" + directoryName + baseTable + "'");

            env.executeSparkUpdate("INSERT INTO default." + baseTable + " VALUES (1, 'a')");

            env.executeSparkUpdate("CREATE TABLE default." + clonedTable +
                    " SHALLOW CLONE default." + baseTable +
                    " LOCATION 's3://" + bucketName + "/" + directoryName + clonedTable + "'");

            Row expectedRow = row(1, "a");
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRow);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRow);
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTable))
                    .containsOnly(expectedRow);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTable))
                    .containsOnly(expectedRow);

            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + clonedTable);

            assertThat(env.executeSpark("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRow);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRow);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + baseTable);
        }
    }

    @Test
    void testVacuumOnShallowCloneTable(DeltaLakeMinioEnvironment env)
    {
        String bucketName = env.getBucketName();
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String clonedTable = "test_dl_clone_tableV1_" + randomNameSuffix();
        String directoryName = "databricks-vacuum-compatibility-test-";
        try {
            env.executeSparkUpdate("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    "LOCATION 's3://" + bucketName + "/" + directoryName + baseTable + "'" +
                    " TBLPROPERTIES (" +
                    " 'delta.columnMapping.mode'='name' )");

            env.executeSparkUpdate("INSERT INTO default." + baseTable + " VALUES (1, 'a')");
            List<String> baseTableActiveDataFiles = getActiveDataFiles(env, baseTable);
            List<String> baseTableAllDataFiles = getFilesFromTableDirectory(env, bucketName, directoryName + baseTable);
            assertThat(baseTableActiveDataFiles).hasSize(1).isEqualTo(baseTableAllDataFiles);

            env.executeSparkUpdate("CREATE TABLE default." + clonedTable +
                    " SHALLOW CLONE default." + baseTable +
                    " LOCATION 's3://" + bucketName + "/" + directoryName + clonedTable + "'");
            env.executeSparkUpdate("INSERT INTO default." + clonedTable + " VALUES (2, 'b')");
            List<String> clonedTableV1ActiveDataFiles = getActiveDataFiles(env, clonedTable);
            // size is 2 because, distinct path returns files which is union of base table (as of cloned version) and newly added file in cloned table
            assertThat(clonedTableV1ActiveDataFiles).hasSize(2);
            List<String> clonedTableV1AllDataFiles = getFilesFromTableDirectory(env, bucketName, directoryName + clonedTable);
            // size is 1 because, data file within shallow cloned folder is only 1 post the above insert
            assertThat(clonedTableV1AllDataFiles).hasSize(1);

            env.executeSparkUpdate("UPDATE default." + clonedTable + " SET a_int = a_int + 1");
            List<String> clonedTableV2ActiveDataFiles = getActiveDataFiles(env, clonedTable);
            // size is 2 because, referenced file from base table and relative file post above insert are both re-written
            assertThat(clonedTableV2ActiveDataFiles).hasSize(2);
            List<String> clonedTableV2AllDataFiles = getFilesFromTableDirectory(env, bucketName, directoryName + clonedTable);
            assertThat(clonedTableV2AllDataFiles).hasSize(3);

            List<String> toBeVacuumedDataFilesFromDryRun = getToBeVacuumedDataFilesFromDryRun(env, clonedTable);
            // only the clonedTableV1AllDataFiles should be deleted, which is of size 1 and should not contain any files/paths from base table
            assertThat(toBeVacuumedDataFilesFromDryRun).hasSize(1)
                    .hasSameElementsAs(clonedTableV1AllDataFiles)
                    .doesNotContainAnyElementsOf(baseTableAllDataFiles);

            executeTrinoVacuumWithZeroRetention(env, clonedTable);
            List<String> clonedTableV4ActiveDataFiles = getActiveDataFiles(env, clonedTable);
            // size of active data files should remain same
            assertThat(clonedTableV4ActiveDataFiles).hasSize(2)
                    .containsExactlyInAnyOrderElementsOf(clonedTableV2ActiveDataFiles); // DISTINCT "$path" doesn't guarantee order
            List<String> clonedTableV4AllDataFiles = getFilesFromTableDirectory(env, bucketName, directoryName + clonedTable);
            // size of all data files should be 2 post vacuum
            assertThat(clonedTableV4ActiveDataFiles).hasSize(2)
                    .hasSameElementsAs(clonedTableV4AllDataFiles);

            List<Row> expectedRowsClonedTable = List.of(row(2, "a"), row(3, "b"));
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTable))
                    .containsOnly(expectedRowsClonedTable);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTable))
                    .containsOnly(expectedRowsClonedTable);
            assertThat(env.executeTrino("SELECT DISTINCT \"$path\" FROM delta.default." + clonedTable).rows())
                    .hasSameElementsAs(env.executeSpark("SELECT distinct _metadata.file_path FROM default." + clonedTable).rows());

            Row expectedRow = row(1, "a");
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRow);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRow);
            assertThat(env.executeTrino("SELECT DISTINCT \"$path\" FROM delta.default." + clonedTable).rows())
                    .hasSameElementsAs(env.executeSpark("SELECT distinct _metadata.file_path FROM default." + clonedTable).rows());

            List<String> baseTableActiveDataFilesPostVacuumOnShallowClonedTable = getActiveDataFiles(env, baseTable);
            List<String> baseTableAllDataFilesPostVacuumOnShallowClonedTable = getFilesFromTableDirectory(env, bucketName, directoryName + baseTable);
            // nothing should've changed with respect to the base table
            assertThat(baseTableActiveDataFilesPostVacuumOnShallowClonedTable)
                    .hasSameElementsAs(baseTableAllDataFilesPostVacuumOnShallowClonedTable)
                    .hasSameElementsAs(baseTableActiveDataFiles)
                    .hasSameElementsAs(baseTableAllDataFiles);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + baseTable);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + clonedTable);
        }
    }

    @Test
    void testReadFromSchemaChangedShallowCloneTablePartitioned(DeltaLakeMinioEnvironment env)
    {
        testReadSchemaChangedCloneTable(true, env);
    }

    @Test
    void testReadFromSchemaChangedShallowCloneTableNonPartitioned(DeltaLakeMinioEnvironment env)
    {
        testReadSchemaChangedCloneTable(false, env);
    }

    private void testReadSchemaChangedCloneTable(boolean partitioned, DeltaLakeMinioEnvironment env)
    {
        String bucketName = env.getBucketName();
        String directoryName = "/databricks-compatibility-test-";
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String clonedTableV1 = "test_dl_clone_tableV1_" + randomNameSuffix();
        String clonedTableV2 = "test_dl_clone_tableV2_" + randomNameSuffix();
        String clonedTableV3 = "test_dl_clone_tableV3_" + randomNameSuffix();
        String clonedTableV4 = "test_dl_clone_tableV4_" + randomNameSuffix();
        try {
            env.executeSparkUpdate("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    (partitioned ? "PARTITIONED BY (b_string) " : "") +
                    "LOCATION 's3://" + bucketName + directoryName + baseTable + "'" +
                    " TBLPROPERTIES (" +
                    " 'delta.columnMapping.mode'='name' )");

            env.executeSparkUpdate("INSERT INTO default." + baseTable + " VALUES (1, 'a')");

            Row expectedRow = row(1, "a");
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRow);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRow);

            env.executeSparkUpdate("ALTER TABLE default." + baseTable + " add columns (c_string string, d_int int)");

            env.executeSparkUpdate("INSERT INTO default." + baseTable + " VALUES (2, 'b', 'c', 3)");

            env.executeSparkUpdate("CREATE TABLE default." + clonedTableV1 +
                    " SHALLOW CLONE default." + baseTable + " VERSION AS OF 1 " +
                    "LOCATION 's3://" + bucketName + directoryName + clonedTableV1 + "'");

            Row expectedRowV1 = row(1, "a");
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable + " VERSION AS OF 1"))
                    .containsOnly(expectedRowV1);
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTableV1))
                    .containsOnly(expectedRowV1);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTableV1))
                    .containsOnly(expectedRowV1);

            env.executeSparkUpdate("CREATE TABLE default." + clonedTableV2 +
                    " SHALLOW CLONE default." + baseTable + " VERSION AS OF 2 " +
                    "LOCATION 's3://" + bucketName + directoryName + clonedTableV2 + "'");

            Row expectedRowV2 = row(1, "a", null, null);
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable + " VERSION AS OF 2"))
                    .containsOnly(expectedRowV2);
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTableV2))
                    .containsOnly(expectedRowV2);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTableV2))
                    .containsOnly(expectedRowV2);

            env.executeSparkUpdate("CREATE TABLE default." + clonedTableV3 +
                    " SHALLOW CLONE default." + baseTable + " VERSION AS OF 3 " +
                    "LOCATION 's3://" + bucketName + directoryName + clonedTableV3 + "'");

            List<Row> expectedRowsV3 = List.of(row(1, "a", null, null), row(2, "b", "c", 3));
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRowsV3);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRowsV3);
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable + " VERSION AS OF 3"))
                    .containsOnly(expectedRowsV3);
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTableV3))
                    .containsOnly(expectedRowsV3);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTableV3))
                    .containsOnly(expectedRowsV3);

            env.executeSparkUpdate("ALTER TABLE default." + baseTable + " DROP COLUMN c_string");
            env.executeSparkUpdate("CREATE TABLE default." + clonedTableV4 +
                    " SHALLOW CLONE default." + baseTable + " VERSION AS OF 4 " +
                    "LOCATION 's3://" + bucketName + directoryName + clonedTableV4 + "'");

            List<Row> expectedRowsV4 = List.of(row(1, "a", null), row(2, "b", 3));
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRowsV4);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRowsV4);
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable + " VERSION AS OF 4"))
                    .containsOnly(expectedRowsV4);
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTableV4))
                    .containsOnly(expectedRowsV4);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTableV4))
                    .containsOnly(expectedRowsV4);

            if (partitioned) {
                List<Row> expectedPartitionRows = List.of(row("a"), row("b"));
                assertThat(env.executeSpark("SELECT b_string FROM default." + baseTable))
                        .containsOnly(expectedPartitionRows);
                assertThat(env.executeTrino("SELECT b_string FROM delta.default." + baseTable))
                        .containsOnly(expectedPartitionRows);
                assertThat(env.executeSpark("SELECT b_string FROM default." + baseTable + " VERSION AS OF 3"))
                        .containsOnly(expectedPartitionRows);
                assertThat(env.executeSpark("SELECT b_string FROM default." + clonedTableV3))
                        .containsOnly(expectedPartitionRows);
                assertThat(env.executeTrino("SELECT b_string FROM delta.default." + clonedTableV3))
                        .containsOnly(expectedPartitionRows);
            }

            env.executeSparkUpdate("INSERT INTO default." + clonedTableV4 + " VALUES (3, 'c', 3)");
            env.executeTrinoUpdate("INSERT INTO delta.default." + clonedTableV4 + " VALUES (4, 'd', 4)");

            List<Row> expectedRowsV5 = List.of(row(1, "a", null), row(2, "b", 3), row(3, "c", 3), row(4, "d", 4));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTableV4))
                    .containsOnly(expectedRowsV5);
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTableV4))
                    .containsOnly(expectedRowsV5);
            // _metadata.file_path is spark substitute of Trino's "$path"
            assertThat(env.executeTrino("SELECT DISTINCT \"$path\" FROM delta.default." + clonedTableV4).rows())
                    .hasSameElementsAs(env.executeSpark("SELECT distinct _metadata.file_path FROM default." + clonedTableV4).rows());

            env.executeSparkUpdate("DELETE FROM default." + clonedTableV4 + " WHERE a_int in (1, 2)");

            List<Row> expectedRowsV6 = List.of(row(3, "c", 3), row(4, "d", 4));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTableV4))
                    .containsOnly(expectedRowsV6);
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTableV4))
                    .containsOnly(expectedRowsV6);
            assertThat(env.executeTrino("SELECT DISTINCT \"$path\" FROM delta.default." + clonedTableV4).rows())
                    .hasSameElementsAs(env.executeSpark("SELECT distinct _metadata.file_path FROM default." + clonedTableV4).rows());
        }
        finally {
            // Use Trino for shallow clone drop (consistent with original test for SHALLOW type)
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + baseTable);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + clonedTableV1);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + clonedTableV2);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + clonedTableV3);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + clonedTableV4);
        }
    }

    @Test
    void testShallowCloneTableMergeNonPartitioned(DeltaLakeMinioEnvironment env)
    {
        testShallowCloneTableMerge(false, env);
    }

    @Test
    void testShallowCloneTableMergePartitioned(DeltaLakeMinioEnvironment env)
    {
        testShallowCloneTableMerge(true, env);
    }

    private void testShallowCloneTableMerge(boolean partitioned, DeltaLakeMinioEnvironment env)
    {
        String bucketName = env.getBucketName();
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String clonedTable = "test_dl_clone_tableV1_" + randomNameSuffix();
        String directoryName = "databricks-merge-clone-compatibility-test-";
        try {
            env.executeSparkUpdate("CREATE TABLE default." + baseTable +
                    " (id INT, v STRING, part DATE) USING delta " +
                    (partitioned ? "PARTITIONED BY (part) " : "") +
                    "LOCATION 's3://" + bucketName + "/" + directoryName + baseTable + "'");

            env.executeSparkUpdate("INSERT INTO default." + baseTable + " " +
                    "VALUES (1, 'A', TIMESTAMP '2024-01-01'), " +
                    "(2, 'B', TIMESTAMP '2024-01-01'), " +
                    "(3, 'C', TIMESTAMP '2024-02-02'), " +
                    "(4, 'D', TIMESTAMP '2024-02-02')");

            env.executeSparkUpdate("CREATE TABLE default." + clonedTable +
                    " SHALLOW CLONE default." + baseTable +
                    " LOCATION 's3://" + bucketName + "/" + directoryName + clonedTable + "'");

            List<Row> expectedRows = List.of(
                    row(1, "A", Date.valueOf("2024-01-01")),
                    row(2, "B", Date.valueOf("2024-01-01")),
                    row(3, "C", Date.valueOf("2024-02-02")),
                    row(4, "D", Date.valueOf("2024-02-02")));
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRows);

            // update on cloned table
            env.executeTrinoUpdate("UPDATE delta.default." + clonedTable + " SET v = 'xxx' WHERE id in (1,3)");
            // source table not change
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRows);
            List<Row> expectedRowsAfterUpdate = List.of(
                    row(1, "xxx", Date.valueOf("2024-01-01")),
                    row(2, "B", Date.valueOf("2024-01-01")),
                    row(3, "xxx", Date.valueOf("2024-02-02")),
                    row(4, "D", Date.valueOf("2024-02-02")));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTable))
                    .containsOnly(expectedRowsAfterUpdate);
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRows);
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTable))
                    .containsOnly(expectedRowsAfterUpdate);

            // merge on cloned table
            String mergeSql = format("""
                  MERGE INTO %s t
                  USING (VALUES (3, 'yyy', TIMESTAMP '2025-01-01'), (4, 'zzz', TIMESTAMP '2025-02-02'), (5, 'kkk', TIMESTAMP '2025-03-03')) AS s(id, v, part)
                  ON (t.id = s.id)
                    WHEN MATCHED AND s.v = 'zzz' THEN DELETE
                    WHEN MATCHED THEN UPDATE SET v = s.v
                    WHEN NOT MATCHED THEN INSERT (id, v, part) VALUES(s.id, s.v, s.part)
                    """, "delta.default." + clonedTable);
            env.executeTrinoUpdate(mergeSql);

            List<Row> expectedRowsAfterMerge = List.of(
                    row(1, "xxx", Date.valueOf("2024-01-01")),
                    row(2, "B", Date.valueOf("2024-01-01")),
                    row(3, "yyy", Date.valueOf("2024-02-02")),
                    row(5, "kkk", Date.valueOf("2025-03-03")));
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTable))
                    .containsOnly(expectedRowsAfterMerge);
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable))
                    .containsOnly(expectedRows);
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTable))
                    .containsOnly(expectedRowsAfterMerge);

            // access base table after drop cloned table
            env.executeTrinoUpdate("DROP TABLE delta.default." + clonedTable);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + baseTable);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + clonedTable);
        }
    }

    @Test
    void testReadShallowCloneTableWithSourceDeletionVectorPartitioned(DeltaLakeMinioEnvironment env)
    {
        testReadShallowCloneTableWithSourceDeletionVector(true, env);
    }

    @Test
    void testReadShallowCloneTableWithSourceDeletionVectorNonPartitioned(DeltaLakeMinioEnvironment env)
    {
        testReadShallowCloneTableWithSourceDeletionVector(false, env);
    }

    private void testReadShallowCloneTableWithSourceDeletionVector(boolean partitioned, DeltaLakeMinioEnvironment env)
    {
        String bucketName = env.getBucketName();
        String baseTable = "test_dv_base_table_" + randomNameSuffix();
        String clonedTable = "test_dv_clone_table_" + randomNameSuffix();
        String directoryName = "clone-deletion-vector-compatibility-test-";
        try {
            env.executeSparkUpdate("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    (partitioned ? "PARTITIONED BY (b_string) " : "") +
                    "LOCATION 's3://" + bucketName + "/" + directoryName + baseTable + "'" +
                    "TBLPROPERTIES ('delta.enableDeletionVectors'='true')");

            env.executeSparkUpdate("INSERT INTO " + baseTable + " VALUES (1, 'aaa'), (2, 'aaa'), (3, 'bbb'), (4, 'bbb')");
            // enforce the rows into one file, so that later is partial delete of the data file instead of remove all rows.
            // This allows the cloned table to reference the same deletion vector but different offset
            // and help us to test the read process of 'p' type deletion vector better.
            env.executeSparkUpdate("OPTIMIZE " + baseTable);
            env.executeSparkUpdate("DELETE FROM default." + baseTable + " WHERE a_int IN (2, 3)");

            env.executeSparkUpdate("CREATE TABLE default." + clonedTable +
                    " SHALLOW CLONE default." + baseTable +
                    " LOCATION 's3://" + bucketName + "/" + directoryName + clonedTable + "'");

            List<Row> expectedRows = List.of(row(1, "aaa"), row(4, "bbb"));
            assertThat(env.executeSpark("SELECT * FROM default." + baseTable)).containsOnly(expectedRows);
            assertThat(env.executeSpark("SELECT * FROM default." + clonedTable)).containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + baseTable)).containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + clonedTable)).containsOnly(expectedRows);

            assertThat(getDeletionVectorType(env, baseTable)).isNotEqualTo("p");
            assertThat(getDeletionVectorType(env, clonedTable)).isEqualTo("p");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + baseTable);
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + clonedTable);
        }
    }

    private String getDeletionVectorType(DeltaLakeMinioEnvironment env, String tableName)
    {
        return (String) env.executeTrino(
                """
                SELECT json_extract_scalar(elem, '$.add.deletionVector.storageType') AS storage_type
                FROM (
                    SELECT CAST(transaction AS JSON) AS json_arr
                    FROM delta.default."%s$transactions"
                    ORDER BY version
                ) t, UNNEST(CAST(t.json_arr AS ARRAY(JSON))) AS u(elem)
                WHERE json_extract_scalar(elem, '$.add.deletionVector.storageType') IS NOT NULL
                LIMIT 1
                """.formatted(tableName))
                .getOnlyValue();
    }

    @SuppressWarnings("unchecked")
    private List<String> getActiveDataFiles(DeltaLakeMinioEnvironment env, String tableName)
    {
        return (List<String>) (List<?>) env.executeTrino("SELECT DISTINCT \"$path\" FROM delta.default." + tableName).column(1);
    }

    @SuppressWarnings("unchecked")
    private List<String> getToBeVacuumedDataFilesFromDryRun(DeltaLakeMinioEnvironment env, String tableName)
    {
        try (Connection connection = env.createSparkConnection();
                Statement statement = connection.createStatement()) {
            // Spark session properties are connection-scoped; apply and query in one session.
            statement.execute("SET spark.databricks.delta.retentionDurationCheck.enabled = false");
            try (ResultSet resultSet = statement.executeQuery("VACUUM default." + tableName + " RETAIN 0 HOURS DRY RUN")) {
                return (List<String>) (List<?>) QueryResult.forResultSet(resultSet).column(1);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to run Spark VACUUM DRY RUN for table: " + tableName, e);
        }
    }

    private void executeTrinoVacuumWithZeroRetention(DeltaLakeMinioEnvironment env, String tableName)
    {
        try (Connection connection = env.createTrinoConnection();
                Statement statement = connection.createStatement()) {
            // Trino session properties are connection-scoped; apply and call in one session.
            statement.execute("SET SESSION delta.vacuum_min_retention = '0s'");
            statement.execute("CALL delta.system.vacuum('default', '" + tableName + "', '0s')");
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to vacuum Delta table with zero retention: " + tableName, e);
        }
    }

    private List<String> getFilesFromTableDirectory(DeltaLakeMinioEnvironment env, String bucketName, String directory)
    {
        try (MinioClient minioClient = env.createMinioClient()) {
            return minioClient.listObjects(bucketName, directory).stream()
                    .filter(key -> !key.contains("/_delta_log"))
                    .map(key -> format("s3://%s/%s", bucketName, key))
                    .toList();
        }
    }
}
