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
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for TEXTFILE format Hive tables with skip header/footer functionality.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestTextFileHiveTable
{
    // Content of the textfile: header, value, footer (3 lines)
    private static final String SINGLE_COLUMN_CONTENT = "header\nvalue\nfooter\n";

    private String warehouseDirectory;

    @BeforeEach
    void setup(HiveBasicEnvironment env)
    {
        this.warehouseDirectory = env.getWarehouseDirectory();

        HdfsClient hdfsClient = env.createHdfsClient();
        hdfsClient.createDirectory(warehouseDirectory + "/TestTextFileHiveTable/single_column");
        hdfsClient.saveFile(warehouseDirectory + "/TestTextFileHiveTable/single_column/single_column.textfile", SINGLE_COLUMN_CONTENT);
    }

    @AfterEach
    void cleanup(HiveBasicEnvironment env)
    {
        try {
            env.createHdfsClient().delete(warehouseDirectory + "/TestTextFileHiveTable");
        }
        catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    @Test
    void testCreateTextFileSkipHeaderFooter(HiveBasicEnvironment env)
    {
        // Test skip header
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_textfile_skip_header");
        env.executeTrinoUpdate(format(
                "CREATE TABLE hive.default.test_create_textfile_skip_header" +
                        " (name varchar) " +
                        "WITH ( " +
                        "   format = 'TEXTFILE', " +
                        "   external_location = 'hdfs://hadoop-master:9000%s/TestTextFileHiveTable/single_column', " +
                        "   skip_header_line_count = 1 " +
                        ")",
                warehouseDirectory));
        try {
            assertThat(env.executeTrino("SELECT * FROM hive.default.test_create_textfile_skip_header")).containsOnly(row("value"), row("footer"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_textfile_skip_header");
        }

        // Test skip footer
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_textfile_skip_footer");
        env.executeTrinoUpdate(format(
                "CREATE TABLE hive.default.test_create_textfile_skip_footer" +
                        " (name varchar) " +
                        "WITH ( " +
                        "   format = 'TEXTFILE', " +
                        "   external_location = 'hdfs://hadoop-master:9000%s/TestTextFileHiveTable/single_column', " +
                        "   skip_footer_line_count = 1 " +
                        ")",
                warehouseDirectory));
        try {
            assertThat(env.executeTrino("SELECT * FROM hive.default.test_create_textfile_skip_footer")).containsOnly(row("header"), row("value"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_textfile_skip_footer");
        }

        // Test skip header and footer
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_textfile_skip_header_footer");
        env.executeTrinoUpdate(format(
                "CREATE TABLE hive.default.test_create_textfile_skip_header_footer" +
                        " (name varchar) " +
                        "WITH ( " +
                        "   format = 'TEXTFILE', " +
                        "   external_location = 'hdfs://hadoop-master:9000%s/TestTextFileHiveTable/single_column', " +
                        "   skip_header_line_count = 1, " +
                        "   skip_footer_line_count = 1 " +
                        ")",
                warehouseDirectory));
        try {
            assertThat(env.executeTrino("SELECT * FROM hive.default.test_create_textfile_skip_header_footer")).containsExactlyInOrder(row("value"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_textfile_skip_header_footer");
        }
    }

    @Test
    void testInsertTextFileSkipHeaderFooter(HiveBasicEnvironment env)
    {
        // Test insert with skip header - should work
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_textfile_skip_header");
        env.executeHiveUpdate("" +
                "CREATE TABLE test_textfile_skip_header " +
                " (col1 int) " +
                "STORED AS TEXTFILE " +
                "TBLPROPERTIES ('skip.header.line.count'='1')");
        try {
            env.executeTrinoUpdate("INSERT INTO hive.default.test_textfile_skip_header VALUES (1)");
            assertThat(env.executeTrino("SELECT * FROM hive.default.test_textfile_skip_header")).containsOnly(row(1));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_textfile_skip_header");
        }

        // Test insert with skip footer - should fail
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_textfile_skip_footer");
        env.executeHiveUpdate("" +
                "CREATE TABLE test_textfile_skip_footer " +
                " (col1 int) " +
                "STORED AS TEXTFILE " +
                "TBLPROPERTIES ('skip.footer.line.count'='1')");
        try {
            assertThatThrownBy(() -> env.executeTrinoUpdate("INSERT INTO hive.default.test_textfile_skip_footer VALUES (1)"))
                    .hasMessageMatching(".* Inserting into Hive table with skip.footer.line.count property not supported");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_textfile_skip_footer");
        }

        // Test insert with skip header and footer - should fail (footer not supported)
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_textfile_skip_header_footer");
        env.executeHiveUpdate("" +
                "CREATE TABLE test_textfile_skip_header_footer " +
                " (col1 int) " +
                "STORED AS TEXTFILE " +
                "TBLPROPERTIES ('skip.header.line.count'='1', 'skip.footer.line.count'='1')");
        try {
            assertThatThrownBy(() -> env.executeTrinoUpdate("INSERT INTO hive.default.test_textfile_skip_header_footer VALUES (1)"))
                    .hasMessageMatching(".* Inserting into Hive table with skip.footer.line.count property not supported");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_textfile_skip_header_footer");
        }
    }

    @Test
    void testCreateTextFileTableAsSelectSkipHeaderFooter(HiveBasicEnvironment env)
    {
        // Test CTAS with skip header - should work
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_textfile_skip_header");
        env.executeTrinoUpdate(
                "CREATE TABLE hive.default.test_create_textfile_skip_header " +
                        "WITH ( " +
                        "   format = 'TEXTFILE', " +
                        "   skip_header_line_count = 1 " +
                        ") " +
                        "AS SELECT 1 AS col_header1, 2 AS col_header2");
        try {
            env.executeTrinoUpdate("INSERT INTO hive.default.test_create_textfile_skip_header VALUES (3, 4)");
            assertThat(env.executeTrino("SELECT * FROM hive.default.test_create_textfile_skip_header")).containsOnly(row(1, 2), row(3, 4));
            assertThat(env.executeHive("SELECT * FROM test_create_textfile_skip_header")).containsOnly(row(1, 2), row(3, 4));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_textfile_skip_header");
        }

        // Test CTAS with skip footer - should fail
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_textfile_skip_footer");
        try {
            assertThatThrownBy(() -> env.executeTrinoUpdate(
                    "CREATE TABLE hive.default.test_create_textfile_skip_footer " +
                            "WITH ( " +
                            "   format = 'TEXTFILE', " +
                            "   skip_footer_line_count = 1 " +
                            ") " +
                            "AS SELECT 1 AS col_header"))
                    .hasMessageMatching(".* Creating Hive table with data with value of skip.footer.line.count property greater than 0 is not supported");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_textfile_skip_footer");
        }
    }
}
