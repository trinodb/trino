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
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.dropDeltaTableWithRetry;
import static org.assertj.core.api.Assertions.assertThat;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeCloneTableCompatibilityDatabricks
{
    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testReadFromSchemaChangedDeepCloneTable(DeltaLakeDatabricksEnvironment env)
    {
        testReadSchemaChangedCloneTable(env, true);
        testReadSchemaChangedCloneTable(env, false);
    }

    private void testReadSchemaChangedCloneTable(DeltaLakeDatabricksEnvironment env, boolean partitioned)
    {
        String directoryName = "/databricks-compatibility-test-";
        String baseTable = "test_dl_base_table_" + randomNameSuffix();
        String clonedTableV1 = "test_dl_clone_tableV1_" + randomNameSuffix();
        String clonedTableV2 = "test_dl_clone_tableV2_" + randomNameSuffix();
        String clonedTableV3 = "test_dl_clone_tableV3_" + randomNameSuffix();
        String clonedTableV4 = "test_dl_clone_tableV4_" + randomNameSuffix();
        try {
            env.executeDatabricksSql("CREATE TABLE default." + baseTable +
                    " (a_int INT, b_string STRING) USING delta " +
                    (partitioned ? "PARTITIONED BY (b_string) " : "") +
                    "LOCATION 's3://" + env.getBucketName() + directoryName + baseTable + "'" +
                    " TBLPROPERTIES ('delta.columnMapping.mode'='name')");

            env.executeDatabricksSql("INSERT INTO default." + baseTable + " VALUES (1, 'a')");

            Row expectedRow = row(1, "a");
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + baseTable)).containsOnly(expectedRow);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + baseTable)).containsOnly(expectedRow);

            env.executeDatabricksSql("ALTER TABLE default." + baseTable + " add columns (c_string string, d_int int)");
            env.executeDatabricksSql("INSERT INTO default." + baseTable + " VALUES (2, 'b', 'c', 3)");

            env.executeDatabricksSql("CREATE TABLE default." + clonedTableV1 +
                    " DEEP CLONE default." + baseTable + " VERSION AS OF 1 " +
                    "LOCATION 's3://" + env.getBucketName() + directoryName + clonedTableV1 + "'");

            Row expectedRowV1 = row(1, "a");
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + baseTable + " VERSION AS OF 1")).containsOnly(expectedRowV1);
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + clonedTableV1)).containsOnly(expectedRowV1);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + clonedTableV1)).containsOnly(expectedRowV1);

            env.executeDatabricksSql("CREATE TABLE default." + clonedTableV2 +
                    " DEEP CLONE default." + baseTable + " VERSION AS OF 2 " +
                    "LOCATION 's3://" + env.getBucketName() + directoryName + clonedTableV2 + "'");

            Row expectedRowV2 = row(1, "a", null, null);
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + baseTable + " VERSION AS OF 2")).containsOnly(expectedRowV2);
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + clonedTableV2)).containsOnly(expectedRowV2);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + clonedTableV2)).containsOnly(expectedRowV2);

            env.executeDatabricksSql("CREATE TABLE default." + clonedTableV3 +
                    " DEEP CLONE default." + baseTable + " VERSION AS OF 3 " +
                    "LOCATION 's3://" + env.getBucketName() + directoryName + clonedTableV3 + "'");

            List<Row> expectedRowsV3 = List.of(row(1, "a", null, null), row(2, "b", "c", 3));
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + baseTable)).containsOnly(expectedRowsV3);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + baseTable)).containsOnly(expectedRowsV3);
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + baseTable + " VERSION AS OF 3")).containsOnly(expectedRowsV3);
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + clonedTableV3)).containsOnly(expectedRowsV3);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + clonedTableV3)).containsOnly(expectedRowsV3);

            env.executeDatabricksSql("ALTER TABLE default." + baseTable + " DROP COLUMN c_string");
            env.executeDatabricksSql("CREATE TABLE default." + clonedTableV4 +
                    " DEEP CLONE default." + baseTable + " VERSION AS OF 4 " +
                    "LOCATION 's3://" + env.getBucketName() + directoryName + clonedTableV4 + "'");

            List<Row> expectedRowsV4 = List.of(row(1, "a", null), row(2, "b", 3));
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + baseTable)).containsOnly(expectedRowsV4);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + baseTable)).containsOnly(expectedRowsV4);
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + baseTable + " VERSION AS OF 4")).containsOnly(expectedRowsV4);
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + clonedTableV4)).containsOnly(expectedRowsV4);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + clonedTableV4)).containsOnly(expectedRowsV4);

            if (partitioned) {
                List<Row> expectedPartitionRows = List.of(row("a"), row("b"));
                assertThat(env.executeDatabricksSql("SELECT b_string FROM default." + baseTable)).containsOnly(expectedPartitionRows);
                assertThat(env.executeTrinoSql("SELECT b_string FROM delta.default." + baseTable)).containsOnly(expectedPartitionRows);
                assertThat(env.executeDatabricksSql("SELECT b_string FROM default." + baseTable + " VERSION AS OF 3")).containsOnly(expectedPartitionRows);
                assertThat(env.executeDatabricksSql("SELECT b_string FROM default." + clonedTableV3)).containsOnly(expectedPartitionRows);
                assertThat(env.executeTrinoSql("SELECT b_string FROM delta.default." + clonedTableV3)).containsOnly(expectedPartitionRows);
            }

            env.executeDatabricksSql("INSERT INTO default." + clonedTableV4 + " VALUES (3, 'c', 3)");
            env.executeTrinoSql("INSERT INTO delta.default." + clonedTableV4 + " VALUES (4, 'd', 4)");

            List<Row> expectedRowsV5 = List.of(row(1, "a", null), row(2, "b", 3), row(3, "c", 3), row(4, "d", 4));
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + clonedTableV4)).containsOnly(expectedRowsV5);
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + clonedTableV4)).containsOnly(expectedRowsV5);
            assertThat(env.executeTrinoSql("SELECT DISTINCT \"$path\" FROM delta.default." + clonedTableV4).getRows())
                    .hasSameElementsAs(env.executeDatabricksSql("SELECT distinct _metadata.file_path FROM default." + clonedTableV4).getRows());

            env.executeDatabricksSql("DELETE FROM default." + clonedTableV4 + " WHERE a_int in (1, 2)");

            List<Row> expectedRowsV6 = List.of(row(3, "c", 3), row(4, "d", 4));
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + clonedTableV4)).containsOnly(expectedRowsV6);
            assertThat(env.executeDatabricksSql("SELECT * FROM default." + clonedTableV4)).containsOnly(expectedRowsV6);
            assertThat(env.executeTrinoSql("SELECT DISTINCT \"$path\" FROM delta.default." + clonedTableV4).getRows())
                    .hasSameElementsAs(env.executeDatabricksSql("SELECT distinct _metadata.file_path FROM default." + clonedTableV4).getRows());
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + baseTable);
            dropDeltaTableWithRetry(env, "default." + clonedTableV1);
            dropDeltaTableWithRetry(env, "default." + clonedTableV2);
            dropDeltaTableWithRetry(env, "default." + clonedTableV3);
            dropDeltaTableWithRetry(env, "default." + clonedTableV4);
        }
    }
}
