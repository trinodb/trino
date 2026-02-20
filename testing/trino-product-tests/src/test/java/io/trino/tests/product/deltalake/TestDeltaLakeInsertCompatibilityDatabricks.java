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

import com.google.common.collect.ImmutableList;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.DeltaLakeDatabricksUtilsJunit.dropDeltaTableWithRetry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(DeltaLakeDatabricksEnvironment.class)
@TestGroup.ConfiguredFeatures
@TestGroup.DeltaLakeDatabricks
@TestGroup.ProfileSpecificTests
class TestDeltaLakeInsertCompatibilityDatabricks
{
    @Test
    @TestGroup.DeltaLakeDatabricks143
    @TestGroup.DeltaLakeDatabricks154
    @TestGroup.DeltaLakeDatabricks164
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testPartitionedInsertCompatibility(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_dl_partitioned_insert_" + randomNameSuffix();

        env.executeDatabricksSql("" +
                "CREATE TABLE default." + tableName +
                "         (a_number INT, a_string STRING)" +
                "         USING delta " +
                "         PARTITIONED BY (a_number)" +
                "         LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

        try {
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (1,'ala'), (2, 'kota')");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (3, 'osla')");
            env.executeDatabricksSql("INSERT INTO default." + tableName + " VALUES (null, 'mysz')");
            env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES (3, 'psa'), (4, 'bobra')");
            env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES (4, 'lwa'), (5, 'jeza')");
            env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES (null, 'kon')");

            List<Row> expectedRows = ImmutableList.of(
                    row(1, "ala"),
                    row(2, "kota"),
                    row(3, "osla"),
                    row(3, "psa"),
                    row(4, "bobra"),
                    row(4, "lwa"),
                    row(5, "jeza"),
                    row(null, "mysz"),
                    row(null, "kon"));

            assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrinoSql("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);

            assertThat(env.executeTrinoSql("SELECT \"$path\" FROM delta.default." + tableName + " WHERE a_number IS NULL").column(1))
                    .hasSize(2)
                    .allMatch(path -> ((String) path).contains("/a_number=__HIVE_DEFAULT_PARTITION__/"));
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"NONE", "SNAPPY", "LZ4", "ZSTD", "GZIP"})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testCompression(String compressionCodec, DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_compression_" + compressionCodec + "_" + randomNameSuffix();
        String trinoTableName = "delta.default." + tableName;
        String location = "s3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName;

        env.executeTrinoSql("CREATE TABLE " + trinoTableName + " WITH (location = '" + location + "') " +
                "AS TABLE tpch.tiny.nation WITH NO DATA");

        try {
            if ("LZ4".equals(compressionCodec)) {
                assertThatThrownBy(() -> env.executeTrinoSql("SET SESSION delta.compression_codec = '" + compressionCodec + "'"))
                        .hasMessageContaining("Unsupported codec: LZ4");
            }
            else {
                env.executeTrinoSql("SET SESSION delta.compression_codec = '" + compressionCodec + "'");

                env.executeTrinoSql("INSERT INTO " + trinoTableName + " TABLE tpch.tiny.nation");
                List<Row> expected = env.executeTrinoSql("TABLE tpch.tiny.nation").getRows();
                assertThat(env.executeTrinoSql("SELECT * FROM " + trinoTableName))
                        .containsOnly(expected);
                assertThat(env.executeDatabricksSql("SELECT * FROM default." + tableName))
                        .containsOnly(expected);
            }
        }
        finally {
            env.executeTrinoSql("DROP TABLE " + trinoTableName);
        }
    }

    @Test
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    void testWritesToTableWithGeneratedColumnFails(DeltaLakeDatabricksEnvironment env)
    {
        String tableName = "test_writes_into_table_with_generated_column_" + randomNameSuffix();
        try {
            env.executeDatabricksSql("CREATE TABLE default." + tableName + " (a INT, b BOOLEAN GENERATED ALWAYS AS (CAST(true AS BOOLEAN))) " +
                    "USING DELTA " +
                    "LOCATION 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "'");

            env.executeDatabricksSql("INSERT INTO default." + tableName + " (a) VALUES (1), (2), (3)");

            assertThat(env.executeTrinoSql("SELECT a, b FROM delta.default." + tableName))
                    .containsOnly(row(1, true), row(2, true), row(3, true));

            assertThatThrownBy(() -> env.executeTrinoSql("INSERT INTO delta.default." + tableName + " VALUES (1, false)"))
                    .hasMessageContaining("Writing to tables with generated columns is not supported");
            assertThatThrownBy(() -> env.executeTrinoSql("UPDATE delta.default." + tableName + " SET a = 3 WHERE b = true"))
                    .hasMessageContaining("Writing to tables with generated columns is not supported");
            assertThatThrownBy(() -> env.executeTrinoSql("DELETE FROM delta.default." + tableName + " WHERE a = 3"))
                    .hasMessageContaining("Writing to tables with generated columns is not supported");
            assertThatThrownBy(() -> env.executeTrinoSql("MERGE INTO delta.default." + tableName + " t USING delta.default." + tableName + " s " +
                    "ON (t.a = s.a) WHEN MATCHED THEN UPDATE SET b = false"))
                    .hasMessageContaining("Writing to tables with generated columns is not supported");
        }
        finally {
            dropDeltaTableWithRetry(env, "default." + tableName);
        }
    }
}
