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

/**
 * Tests for Avro tables with SymlinkTextInputFormat.
 * <p>
 * Ported from the Tempto-based TestAvroSymlinkInputFormat.
 * <p>
 * These tests verify that Trino can correctly read from Hive tables that use
 * SymlinkTextInputFormat to reference Avro data files stored in different locations.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
class TestAvroSymlinkInputFormat
{
    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSymlinkTable(HiveStorageFormatsEnvironment env)
            throws Exception
    {
        String warehouseDirectory = env.getWarehouseDirectory();
        HdfsClient hdfsClient = env.createHdfsClient();

        String table = "test_avro_symlink";
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);

        env.executeHiveUpdate("" +
                "CREATE TABLE " + table +
                " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                "WITH SERDEPROPERTIES ('avro.schema.literal'='{" +
                "\"namespace\": \"io.trino.tests.product.hive\"," +
                "\"name\": \"test_avro_symlink\"," +
                "\"type\": \"record\"," +
                "\"fields\": [" +
                "{ \"name\":\"string_col\", \"type\":\"string\"}," +
                "{ \"name\":\"int_col\", \"type\":\"int\" }" +
                "]}') " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_avro_symlink";

        try {
            saveResourceOnHdfs(hdfsClient, "original_data.avro", dataDir + "/original_data.avro");
            hdfsClient.saveFile(dataDir + "/dontread.txt", "This file will cause an error if read as avro.");
            hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/original_data.avro", dataDir));
            assertThat(env.executeTrino("SELECT * FROM hive.default." + table)).containsExactlyInOrder(row("someValue", 1));
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

        String table = "test_avro_symlink_with_multiple_parents";
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);

        env.executeHiveUpdate("" +
                "CREATE TABLE " + table +
                " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                "WITH SERDEPROPERTIES ('avro.schema.literal'='{" +
                "\"namespace\": \"io.trino.tests.product.hive\"," +
                "\"name\": \"test_avro_symlink\"," +
                "\"type\": \"record\"," +
                "\"fields\": [" +
                "{ \"name\":\"string_col\", \"type\":\"string\"}," +
                "{ \"name\":\"int_col\", \"type\":\"int\" }" +
                "]}') " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_avro_symlink_with_multiple_parents";
        String anotherDataDir = warehouseDirectory + "/data2_test_avro_symlink_with_multiple_parents";

        try {
            saveResourceOnHdfs(hdfsClient, "original_data.avro", dataDir + "/original_data.avro");
            saveResourceOnHdfs(hdfsClient, "original_data.avro", anotherDataDir + "/more_data.avro");
            hdfsClient.saveFile(dataDir + "/dontread.txt", "This file will cause an error if read as avro.");
            hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/original_data.avro\nhdfs:%s/more_data.avro", dataDir, anotherDataDir));
            assertThat(env.executeTrino("SELECT COUNT(*) as cnt FROM hive.default." + table)).containsExactlyInOrder(row(2L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);
            hdfsClient.delete(dataDir);
            hdfsClient.delete(anotherDataDir);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSymlinkTableWithNestedDirectory(HiveStorageFormatsEnvironment env)
            throws Exception
    {
        String warehouseDirectory = env.getWarehouseDirectory();
        HdfsClient hdfsClient = env.createHdfsClient();

        String table = "test_avro_symlink_with_nested_directory";
        env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);

        env.executeHiveUpdate("" +
                "CREATE TABLE " + table +
                " ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " +
                "WITH SERDEPROPERTIES ('avro.schema.literal'='{" +
                "\"namespace\": \"io.trino.tests.product.hive\"," +
                "\"name\": \"test_avro_symlink\"," +
                "\"type\": \"record\"," +
                "\"fields\": [" +
                "{ \"name\":\"string_col\", \"type\":\"string\"}," +
                "{ \"name\":\"int_col\", \"type\":\"int\" }" +
                "]}') " +
                "STORED AS " +
                "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' " +
                "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'");

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_avro_symlink_with_nested_directory/nested_directory";

        try {
            saveResourceOnHdfs(hdfsClient, "original_data.avro", dataDir + "/original_data.avro");
            // Note: original test used hdfs:// (with double slash) for this test case
            hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs://%s/original_data.avro", dataDir));
            assertThat(env.executeTrino("SELECT * FROM hive.default." + table)).containsExactlyInOrder(row("someValue", 1));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);
            hdfsClient.delete(warehouseDirectory + "/data_test_avro_symlink_with_nested_directory");
        }
    }

    private void saveResourceOnHdfs(HdfsClient hdfsClient, String resource, String location)
            throws IOException
    {
        hdfsClient.delete(location);
        try (InputStream inputStream = getResource(Path.of("io/trino/tests/product/hive/data/avro/", resource).toString()).openStream()) {
            byte[] content = inputStream.readAllBytes();
            hdfsClient.saveFile(location, content);
        }
    }
}
