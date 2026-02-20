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
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

/**
 * Tests for Parquet files with reordered columns.
 * <p>
 * Ported from the Tempto-based TestParquetFileWithReorderedColumns.
 */
@ProductTest
@RequiresEnvironment(HiveSparkEnvironment.class)
@TestGroup.HiveSpark
class TestParquetFileWithReorderedColumns
{
    @Test
    void testReadParquetFileWithReorderedColumns(HiveSparkEnvironment env)
            throws IOException
    {
        String warehouseDirectory = env.getWarehouseDirectory();
        HdfsClient hdfsClient = env.createHdfsClient();

        // Create directory and upload parquet file
        hdfsClient.createDirectory(warehouseDirectory + "/TestParquetFileWithReorderedColumns");
        try (InputStream inputStream = Resources.asByteSource(Resources.getResource("parquet/reordered_columns.parquet")).openStream()) {
            hdfsClient.saveFile(warehouseDirectory + "/TestParquetFileWithReorderedColumns/reordered_columns.parquet", inputStream.readAllBytes());
        }

        String sourceTableName = "test_reordered_columns_table_" + randomNameSuffix();
        String tableName = "test_read_reordered_columns_table_" + randomNameSuffix();
        try {
            env.executeTrinoUpdate(format(
                    "CREATE TABLE hive.default.%s" +
                            " (id bigint, buyplan_style_detail_id bigint, last_modified_on bigint) " +
                            "WITH ( " +
                            "   format = 'PARQUET', " +
                            "   external_location = 'hdfs://hadoop-master:9000%s/TestParquetFileWithReorderedColumns/' " +
                            ")",
                    sourceTableName,
                    warehouseDirectory));

            // Write parquet file with Trino parquet writer using an existing file as the source which reproduces the problem of Apache Spark not reading the file
            env.executeTrinoUpdate(format("CREATE TABLE hive.default.%s WITH (format = 'PARQUET') AS SELECT * FROM hive.default.%s", tableName, sourceTableName));

            String sql = "SELECT COUNT(*), SUM(id), SUM(buyplan_style_detail_id), SUM(last_modified_on) FROM hive.default." + tableName;
            assertThat(env.executeTrino(sql))
                    .containsExactlyInOrder(row(50438L, 323043905052L, 67694121262L, 83905381446283000L));

            assertThat(env.executeHive("SELECT COUNT(*), SUM(id), SUM(buyplan_style_detail_id), SUM(last_modified_on) FROM default." + tableName))
                    .containsExactlyInOrder(row(50438L, 323043905052L, 67694121262L, 83905381446283000L));

            assertThat(env.executeSpark("SELECT COUNT(*), SUM(id), SUM(buyplan_style_detail_id), SUM(last_modified_on) FROM default." + tableName))
                    .containsExactlyInOrder(row(50438L, 323043905052L, 67694121262L, 83905381446283000L));
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + sourceTableName);
            hdfsClient.delete(warehouseDirectory + "/TestParquetFileWithReorderedColumns");
        }
    }
}
