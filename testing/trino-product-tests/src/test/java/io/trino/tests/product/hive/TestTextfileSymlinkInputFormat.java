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

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_ISSUES;
import static io.trino.tests.product.utils.HadoopTestUtils.RETRYABLE_FAILURES_MATCH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Hive tables using SymlinkTextInputFormat.
 * <p>
 * Ported from the Tempto-based TestTextfileSymlinkInputFormat.
 * <p>
 * SymlinkTextInputFormat allows Hive/Trino to read data from files listed in a symlink manifest file,
 * enabling flexible data organization where table metadata points to a manifest file that in turn
 * references the actual data files.
 */
@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormats
class TestTextfileSymlinkInputFormat
{
    // Content of the single_int_column/data.textfile resource - a single integer value
    private static final String DATA_TEXTFILE_CONTENT = "42";

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSymlinkTable(HiveStorageFormatsEnvironment env)
            throws Exception
    {
        String table = "test_textfile_symlink";
        String warehouseDirectory = env.getWarehouseDirectory();
        HdfsClient hdfsClient = env.createHdfsClient();

        env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);
        env.executeHiveUpdate(
                """
                CREATE TABLE %s (col int)
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
                """.formatted(table));

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_textfile_symlink";

        try {
            // Save the data file content directly
            hdfsClient.createDirectory(dataDir);
            hdfsClient.saveFile(dataDir + "/data.textfile", DATA_TEXTFILE_CONTENT);

            // Create the symlink manifest file pointing to the data file
            hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/data.textfile", dataDir));

            assertThat(env.executeTrino("SELECT * FROM hive.default." + table)).containsExactlyInOrder(row(42));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE " + table);
            hdfsClient.delete(dataDir);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSymlinkTableWithSymlinkFileContainingNonExistentPath(HiveStorageFormatsEnvironment env)
            throws Exception
    {
        String table = "test_textfile_invalid_symlink";
        String warehouseDirectory = env.getWarehouseDirectory();
        HdfsClient hdfsClient = env.createHdfsClient();

        env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);
        env.executeHiveUpdate(
                """
                CREATE TABLE %s (col int)
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
                """.formatted(table));

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_textfile_invalid_symlink";

        try {
            // Save a valid data file
            hdfsClient.createDirectory(dataDir);
            hdfsClient.saveFile(dataDir + "/data.textfile", DATA_TEXTFILE_CONTENT);

            // Create a symlink manifest file that references both a valid and non-existent file
            hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/data.textfile\nhdfs:%s/missingfile.textfile", dataDir, dataDir));

            assertThatThrownBy(() -> env.executeTrino("SELECT * FROM hive.default." + table))
                    .hasMessageMatching(".*Manifest file from the location \\[.*data_test_textfile_invalid_symlink\\] contains non-existent path:.*missingfile.textfile");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE " + table);
            hdfsClient.delete(dataDir);
        }
    }

    @Test
    @Flaky(issue = RETRYABLE_FAILURES_ISSUES, match = RETRYABLE_FAILURES_MATCH)
    void testSymlinkTableWithMultipleParentDirectories(HiveStorageFormatsEnvironment env)
            throws Exception
    {
        String table = "test_textfile_symlink_with_multiple_parents";
        String warehouseDirectory = env.getWarehouseDirectory();
        HdfsClient hdfsClient = env.createHdfsClient();

        env.executeHiveUpdate("DROP TABLE IF EXISTS " + table);
        env.executeHiveUpdate(
                """
                CREATE TABLE %s (value int)
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
                OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\
                """.formatted(table));

        String tableRoot = warehouseDirectory + '/' + table;
        String dataDir = warehouseDirectory + "/data_test_textfile_symlink_with_multiple_parents";
        String anotherDataDir = warehouseDirectory + "/data2_test_textfile_symlink_with_multiple_parents";

        try {
            // Create both data directories with text files
            hdfsClient.createDirectory(dataDir);
            hdfsClient.saveFile(dataDir + "/data.textfile", DATA_TEXTFILE_CONTENT);

            hdfsClient.createDirectory(anotherDataDir);
            hdfsClient.saveFile(anotherDataDir + "/data.textfile", DATA_TEXTFILE_CONTENT);

            // This file will cause an error if read as textfile, but it won't be read
            // because the symlink only references the data.textfile files.
            // We create an invalid textfile format to simulate the original .avro file behavior.
            hdfsClient.saveFile(dataDir + "/dontread.avro", "INVALID_BINARY_CONTENT_THAT_WILL_FAIL_TO_PARSE");

            // Create symlink manifest referencing only the text files from both directories
            hdfsClient.saveFile(tableRoot + "/symlink.txt", format("hdfs:%s/data.textfile\nhdfs:%s/data.textfile", dataDir, anotherDataDir));

            // Should count 2 rows (one from each directory), ignoring the .avro file
            assertThat(env.executeTrino("SELECT COUNT(*) as cnt FROM hive.default." + table)).containsExactlyInOrder(row(2L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE " + table);
            hdfsClient.delete(dataDir);
            hdfsClient.delete(anotherDataDir);
        }
    }
}
