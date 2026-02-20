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

import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.services.junit.Flaky;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

import static com.google.common.io.Resources.getResource;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Parquet tables using SymlinkTextInputFormat.
 * <p>
 * Ported from the Tempto-based TestParquetSymlinkInputFormat.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
class TestParquetSymlinkInputFormat
{
    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSymlinkTable(HiveStorageFormatsEnvironment env)
            throws Exception
    {
        String warehouseDirectory = env.getWarehouseDirectory();
        HdfsClient hdfsClient = env.createHdfsClient();

        String table = "test_parquet_symlink";
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);

        env.executeHiveUpdate("" +
                "CREATE TABLE " + table +
                "(col int) " +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_parquet_symlink";

        try {
            saveResourceOnHdfs(hdfsClient, "data.parquet", dataDir + "/data.parquet");
            hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/data.parquet", dataDir));
            assertThat(env.executeTrino("SELECT * FROM hive.default." + table)).containsExactlyInOrder(row(42));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);
            hdfsClient.delete(dataDir);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSymlinkTableWithSymlinkFileContainingNonExistentPath(HiveStorageFormatsEnvironment env)
            throws Exception
    {
        String warehouseDirectory = env.getWarehouseDirectory();
        HdfsClient hdfsClient = env.createHdfsClient();

        String table = "test_parquet_invalid_symlink";
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);

        env.executeHiveUpdate("" +
                "CREATE TABLE " + table +
                "(col int) " +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_parquet_invalid_symlink";

        try {
            saveResourceOnHdfs(hdfsClient, "data.parquet", dataDir + "/data.parquet");
            hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/data.parquet\nhdfs:%s/missingfile.parquet", dataDir, dataDir));
            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.default." + table))
                    .hasMessageMatching(".*Manifest file from the location \\[.*data_test_parquet_invalid_symlink\\] contains non-existent path:.*missingfile.parquet");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);
            hdfsClient.delete(dataDir);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSymlinkTableWithMultipleParentDirectories(HiveStorageFormatsEnvironment env)
            throws Exception
    {
        String warehouseDirectory = env.getWarehouseDirectory();
        HdfsClient hdfsClient = env.createHdfsClient();

        String table = "test_parquet_symlink_with_multiple_parents";
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);

        env.executeHiveUpdate("" +
                "CREATE TABLE " + table +
                "(value int) " +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_parquet_symlink_with_multiple_parents";
        String anotherDataDir = warehouseDirectory + "/data2_test_parquet_symlink_with_multiple_parents";

        try {
            saveResourceOnHdfs(hdfsClient, "data.parquet", dataDir + "/data.parquet");
            saveResourceOnHdfs(hdfsClient, "data.parquet", anotherDataDir + "/data.parquet");
            hdfsClient.saveFile(dataDir + "/dontread.txt", "This file will cause an error if read as avro.");
            hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/data.parquet\nhdfs:%s/data.parquet", dataDir, anotherDataDir));
            assertThat(env.executeTrino("SELECT COUNT(*) as cnt FROM hive.default." + table)).containsExactlyInOrder(row(2L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);
            hdfsClient.delete(dataDir);
            hdfsClient.delete(anotherDataDir);
        }
    }

    private void saveResourceOnHdfs(HdfsClient hdfsClient, String resource, String location)
            throws IOException
    {
        hdfsClient.delete(location);
        try (InputStream inputStream = getResource(Path.of("io/trino/tests/product/hive/data/single_int_column/", resource).toString()).openStream()) {
            byte[] content = inputStream.readAllBytes();
            hdfsClient.saveFile(location, content);
        }
    }
}
