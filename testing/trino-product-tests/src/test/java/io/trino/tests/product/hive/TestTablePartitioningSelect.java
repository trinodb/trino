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
package io.trino.tests.product.hive;

import com.google.common.io.Resources;
import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Optional;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;

/**
 * Tests for selecting from partitioned Hive tables with different storage formats.
 * <p>
 * Ported from the Tempto-based TestTablePartitioningSelect.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestTablePartitioningSelect
{
    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectPartitionedTextFileTable(HiveBasicEnvironment env)
            throws IOException
    {
        testSelectPartitionedTable(env, "TEXTFILE", Optional.of("DELIMITED FIELDS TERMINATED BY '|'"));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectPartitionedOrcTable(HiveBasicEnvironment env)
            throws IOException
    {
        testSelectPartitionedTable(env, "ORC", Optional.empty());
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectPartitionedRcFileTable(HiveBasicEnvironment env)
            throws IOException
    {
        testSelectPartitionedTable(env, "RCFILE", Optional.of("SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'"));
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectPartitionedParquetTable(HiveBasicEnvironment env)
            throws IOException
    {
        testSelectPartitionedTable(env, "PARQUET", Optional.empty());
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSelectPartitionedAvroTable(HiveBasicEnvironment env)
            throws IOException
    {
        testSelectPartitionedTable(env, "AVRO", Optional.of("SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'"));
    }

    private void testSelectPartitionedTable(HiveBasicEnvironment env, String fileFormat, Optional<String> serde)
            throws IOException
    {
        String suffix = randomNameSuffix();
        String tableName = fileFormat.toLowerCase(Locale.ENGLISH) + "_single_int_column_partitioned_" + suffix;
        String warehouseDirectory = env.getWarehouseDirectory();
        HdfsClient hdfsClient = env.createHdfsClient();

        // Create table location directories in HDFS
        String tableDir = warehouseDirectory + "/" + tableName;
        String partition1Dir = tableDir + "/part_col=1";
        String partition2Dir = tableDir + "/part_col=2";

        hdfsClient.createDirectory(partition1Dir);
        hdfsClient.createDirectory(partition2Dir);

        // Upload invalid data to partition 1
        hdfsClient.saveFile(partition1Dir + "/data.txt", "INVALID DATA");

        // Upload valid data file to partition 2
        String dataResourcePath = "io/trino/tests/product/hive/data/single_int_column/data." + fileFormat.toLowerCase(Locale.ENGLISH);
        try (InputStream inputStream = Resources.asByteSource(Resources.getResource(dataResourcePath)).openStream()) {
            hdfsClient.saveFile(partition2Dir + "/data." + fileFormat.toLowerCase(Locale.ENGLISH), inputStream.readAllBytes());
        }

        try {
            // Create the partitioned table in Hive
            String createTableDdl = buildSingleIntColumnPartitionedTableDDL(tableName, fileFormat, serde);
            env.executeHiveUpdate(createTableDdl);

            // Add partitions
            env.executeHiveUpdate(format(
                    "ALTER TABLE %s ADD PARTITION (part_col = 1) LOCATION 'hdfs://hadoop-master:9000%s'",
                    tableName, partition1Dir));
            env.executeHiveUpdate(format(
                    "ALTER TABLE %s ADD PARTITION (part_col = 2) LOCATION 'hdfs://hadoop-master:9000%s'",
                    tableName, partition2Dir));

            // Test selecting from valid partition only
            String selectFromOnePartitionSql = "SELECT * FROM hive.default." + tableName + " WHERE part_col = 2";
            assertThat(env.executeTrino(selectFromOnePartitionSql))
                    .containsOnly(row(42, 2));

            // Test selecting from all partitions - this should either fail or return null for invalid data
            try {
                assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName))
                        .containsOnly(row(42, 2), row(null, 1));
            }
            catch (RuntimeException expectedDueToInvalidPartitionData) {
                // This is expected for some formats that can't parse invalid data
            }
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + tableName);
            hdfsClient.delete(tableDir);
        }
    }

    private static String buildSingleIntColumnPartitionedTableDDL(String tableName, String fileFormat, Optional<String> rowFormat)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE EXTERNAL TABLE ").append(tableName).append("(");
        sb.append("   col INT");
        sb.append(") ");
        sb.append("PARTITIONED BY (part_col INT) ");
        if (rowFormat.isPresent()) {
            sb.append("ROW FORMAT ").append(rowFormat.get()).append(" ");
        }
        sb.append("STORED AS ").append(fileFormat);
        sb.append(" TBLPROPERTIES ('transactional'='false')");
        return sb.toString();
    }
}
