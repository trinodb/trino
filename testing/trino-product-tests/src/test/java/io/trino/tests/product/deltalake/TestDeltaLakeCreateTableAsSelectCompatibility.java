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
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Delta Lake CREATE TABLE AS SELECT compatibility with Spark/Delta OSS.
 * <p>
 */
@ProductTest
@RequiresEnvironment(DeltaLakeMinioEnvironment.class)
@TestGroup.DeltaLakeMinio
class TestDeltaLakeCreateTableAsSelectCompatibility
{
    @Test
    void testCreateFromTrinoWithDefaultPartitionValues(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_create_partitioned_table_default_as_" + randomNameSuffix();

        try {
            assertThat(env.executeTrinoUpdate(
                    "CREATE TABLE delta.default." + tableName + "(number_partition, string_partition, a_value) " +
                            "WITH (" +
                            "   location = 's3://" + env.getBucketName() + "/databricks-compatibility-test-" + tableName + "', " +
                            "   partitioned_by = ARRAY['number_partition', 'string_partition']) " +
                            "AS VALUES (NULL, 'partition_a', 'jarmuz'), (1, NULL, 'brukselka'), (NULL, NULL, 'kalafior')"))
                    .isEqualTo(3);

            List<Row> expectedRows = ImmutableList.of(
                    row(null, "partition_a", "jarmuz"),
                    row(1, null, "brukselka"),
                    row(null, null, "kalafior"));

            assertThat(env.executeSpark("SELECT * FROM default." + tableName))
                    .containsOnly(expectedRows);
            assertThat(env.executeTrino("SELECT * FROM delta.default." + tableName))
                    .containsOnly(expectedRows);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS delta.default." + tableName);
        }
    }

    @Test
    void testCreateTableWithUnsupportedPartitionType(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_ctas_unsupported_column_types_" + randomNameSuffix();
        String tableLocation = "s3://%s/databricks-compatibility-test-%s".formatted(env.getBucketName(), tableName);
        try {
            assertThatThrownBy(() -> env.executeTrinoUpdate("" +
                    "CREATE TABLE delta.default." + tableName + " WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "') AS SELECT 1 a, array[1] part"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");
            assertThatThrownBy(() -> env.executeTrinoUpdate("" +
                    "CREATE TABLE delta.default." + tableName + " WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "') AS SELECT 1 a, map() part"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");
            assertThatThrownBy(() -> env.executeTrinoUpdate("" +
                    "CREATE TABLE delta.default." + tableName + " WITH (partitioned_by = ARRAY['part'], location = '" + tableLocation + "') AS SELECT 1 a, row(1) part"))
                    .hasMessageContaining("Using array, map or row type on partitioned columns is unsupported");

            assertThatThrownBy(() -> env.executeSparkUpdate(
                    "CREATE TABLE default." + tableName + " USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "' AS SELECT 1 a, array(1) part"))
                    .hasStackTraceContaining("partition column");
            assertThatThrownBy(() -> env.executeSparkUpdate(
                    "CREATE TABLE default." + tableName + " USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "' AS SELECT 1 a, map() part"))
                    .hasStackTraceContaining("partition column");
            assertThatThrownBy(() -> env.executeSparkUpdate(
                    "CREATE TABLE default." + tableName + " USING DELTA PARTITIONED BY (part) LOCATION '" + tableLocation + "' AS SELECT 1 a, named_struct('x', 1) part"))
                    .hasStackTraceContaining("partition column");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }

    @Test
    void testCreateTableAsSelectWithAllPartitionColumns(DeltaLakeMinioEnvironment env)
    {
        String tableName = "test_dl_ctas_with_all_partition_columns_" + randomNameSuffix();
        String tableDirectory = "databricks-compatibility-test-" + tableName;

        try {
            assertThatThrownBy(() -> env.executeTrinoUpdate("" +
                    "CREATE TABLE delta.default." + tableName + " " +
                    "WITH (partitioned_by = ARRAY['part'], location = 's3://" + env.getBucketName() + "/" + tableDirectory + "')" +
                    "AS SELECT 1 part"))
                    .hasMessageContaining("Using all columns for partition columns is unsupported");
            assertThatThrownBy(() -> env.executeTrinoUpdate("" +
                    "CREATE TABLE delta.default." + tableName + " " +
                    "WITH (partitioned_by = ARRAY['part', 'another_part'], location = 's3://" + env.getBucketName() + "/" + tableDirectory + "')" +
                    "AS SELECT 1 part, 2 another_part"))
                    .hasMessageContaining("Using all columns for partition columns is unsupported");

            assertThatThrownBy(() -> env.executeSparkUpdate("" +
                    "CREATE TABLE default." + tableName + " " +
                    "USING DELTA " +
                    "PARTITIONED BY (part)" +
                    "LOCATION 's3://" + env.getBucketName() + "/" + tableDirectory + "'" +
                    "AS SELECT 1 part"))
                    .hasStackTraceContaining("Cannot use all columns for partition columns");
            assertThatThrownBy(() -> env.executeSparkUpdate("" +
                    "CREATE TABLE default." + tableName + " " +
                    "USING DELTA " +
                    "PARTITIONED BY (part, another_part)" +
                    "LOCATION 's3://" + env.getBucketName() + "/" + tableDirectory + "'" +
                    "SELECT 1 part, 2 another_part"))
                    .hasStackTraceContaining("Cannot use all columns for partition columns");
        }
        finally {
            env.executeSparkUpdate("DROP TABLE IF EXISTS default." + tableName);
        }
    }
}
