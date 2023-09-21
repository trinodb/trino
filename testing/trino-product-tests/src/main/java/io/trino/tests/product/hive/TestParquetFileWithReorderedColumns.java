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
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.io.InputStream;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE_SPARK;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onSpark;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetFileWithReorderedColumns
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @BeforeMethodWithContext
    public void setup()
            throws Exception
    {
        hdfsClient.createDirectory(warehouseDirectory + "/TestParquetFileWithReorderedColumns");
        try (InputStream inputStream = Resources.asByteSource(Resources.getResource("parquet/reordered_columns.parquet")).openStream()) {
            hdfsClient.saveFile(warehouseDirectory + "/TestParquetFileWithReorderedColumns/reordered_columns.parquet", inputStream);
        }
    }

    @AfterMethodWithContext
    public void cleanup()
    {
        hdfsClient.delete(warehouseDirectory + "/TestParquetFileWithReorderedColumns");
    }

    @Test(groups = {HIVE_SPARK, PROFILE_SPECIFIC_TESTS})
    public void testReadParquetFileWithReorderedColumns()
    {
        String sourceTableName = "test_reordered_columns_table_" + randomNameSuffix();
        String tableName = "test_read_reordered_columns_table_" + randomNameSuffix();
        try {
            onTrino().executeQuery(format(
                    "CREATE TABLE %s" +
                            " (id bigint, buyplan_style_detail_id bigint, last_modified_on bigint) " +
                            "WITH ( " +
                            "   format = 'PARQUET', " +
                            "   external_location = 'hdfs://hadoop-master:9000%s/TestParquetFileWithReorderedColumns/' " +
                            ")",
                    sourceTableName,
                    warehouseDirectory));
            // Write parquet file with Trino parquet writer using an existing file as the source which reproduces the problem of Apache Spark not reading the file
            onTrino().executeQuery(format("CREATE TABLE %s WITH (format = 'PARQUET') AS SELECT * FROM %s", tableName, sourceTableName));
            String sql = "SELECT COUNT(*), SUM(id), SUM(buyplan_style_detail_id), SUM(last_modified_on) FROM " + tableName;
            assertThat(onTrino().executeQuery(sql))
                    .containsExactlyInOrder(row(50438L, 323043905052L, 67694121262L, 83905381446283000L));

            assertThat(onHive().executeQuery(sql))
                    .containsExactlyInOrder(row(50438L, 323043905052L, 67694121262L, 83905381446283000L));

            assertThat(onSpark().executeQuery(sql))
                    .containsExactlyInOrder(row(50438L, 323043905052L, 67694121262L, 83905381446283000L));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
            onTrino().executeQuery("DROP TABLE " + sourceTableName);
        }
    }
}
