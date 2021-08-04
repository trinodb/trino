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
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.io.InputStream;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTextFileHiveTable
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @BeforeTestWithContext
    public void setup()
            throws Exception
    {
        hdfsClient.createDirectory(warehouseDirectory + "/TestTextFileHiveTable/single_column");
        try (InputStream inputStream = Resources.asByteSource(Resources.getResource("textfile/single_column.textfile")).openStream()) {
            hdfsClient.saveFile(warehouseDirectory + "/TestTextFileHiveTable/single_column/single_column.textfile", inputStream);
        }
    }

    @AfterTestWithContext
    public void cleanup()
    {
        hdfsClient.delete(warehouseDirectory + "/TestTextFileHiveTable");
    }

    @Test
    public void testCreateTextFileSkipHeaderFooter()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_create_textfile_skip_header");
        onTrino().executeQuery(format(
                "CREATE TABLE test_create_textfile_skip_header" +
                        " (name varchar) " +
                        "WITH ( " +
                        "   format = 'TEXTFILE', " +
                        "   external_location = 'hdfs://hadoop-master:9000%s/TestTextFileHiveTable/single_column', " +
                        "   skip_header_line_count = 1 " +
                        ")",
                warehouseDirectory));
        assertThat(query("SELECT * FROM test_create_textfile_skip_header")).containsOnly(row("value"), row("footer"));
        onHive().executeQuery("DROP TABLE test_create_textfile_skip_header");

        onHive().executeQuery("DROP TABLE IF EXISTS test_create_textfile_skip_footer");
        onTrino().executeQuery(format(
                "CREATE TABLE test_create_textfile_skip_footer" +
                        " (name varchar) " +
                        "WITH ( " +
                        "   format = 'TEXTFILE', " +
                        "   external_location = 'hdfs://hadoop-master:9000%s/TestTextFileHiveTable/single_column', " +
                        "   skip_footer_line_count = 1 " +
                        ")",
                warehouseDirectory));
        assertThat(query("SELECT * FROM test_create_textfile_skip_footer")).containsOnly(row("header"), row("value"));
        onHive().executeQuery("DROP TABLE test_create_textfile_skip_footer");

        onHive().executeQuery("DROP TABLE IF EXISTS test_create_textfile_skip_header_footer");
        onTrino().executeQuery(format(
                "CREATE TABLE test_create_textfile_skip_header_footer" +
                        " (name varchar) " +
                        "WITH ( " +
                        "   format = 'TEXTFILE', " +
                        "   external_location = 'hdfs://hadoop-master:9000%s/TestTextFileHiveTable/single_column', " +
                        "   skip_header_line_count = 1, " +
                        "   skip_footer_line_count = 1 " +
                        ")",
                warehouseDirectory));
        assertThat(query("SELECT * FROM test_create_textfile_skip_header_footer")).containsExactlyInOrder(row("value"));
        onHive().executeQuery("DROP TABLE test_create_textfile_skip_header_footer");
    }

    @Test
    public void testInsertTextFileSkipHeaderFooter()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_textfile_skip_header");
        onHive().executeQuery("" +
                "CREATE TABLE test_textfile_skip_header " +
                " (col1 int) " +
                "STORED AS TEXTFILE " +
                "TBLPROPERTIES ('skip.header.line.count'='1')");
        onTrino().executeQuery("INSERT INTO test_textfile_skip_header VALUES (1)");
        assertThat(query("SELECT * FROM test_textfile_skip_header")).containsOnly(row(1));
        onHive().executeQuery("DROP TABLE test_textfile_skip_header");

        onHive().executeQuery("DROP TABLE IF EXISTS test_textfile_skip_footer");
        onHive().executeQuery("" +
                "CREATE TABLE test_textfile_skip_footer " +
                " (col1 int) " +
                "STORED AS TEXTFILE " +
                "TBLPROPERTIES ('skip.footer.line.count'='1')");
        assertThatThrownBy(() -> onTrino().executeQuery("INSERT INTO test_textfile_skip_footer VALUES (1)"))
                .hasMessageMatching(".* Inserting into Hive table with skip.footer.line.count property not supported");
        onHive().executeQuery("DROP TABLE test_textfile_skip_footer");

        onHive().executeQuery("DROP TABLE IF EXISTS test_textfile_skip_header_footer");
        onHive().executeQuery("" +
                "CREATE TABLE test_textfile_skip_header_footer " +
                " (col1 int) " +
                "STORED AS TEXTFILE " +
                "TBLPROPERTIES ('skip.header.line.count'='1', 'skip.footer.line.count'='1')");
        assertThatThrownBy(() -> onTrino().executeQuery("INSERT INTO test_textfile_skip_header_footer VALUES (1)"))
                .hasMessageMatching(".* Inserting into Hive table with skip.footer.line.count property not supported");
        onHive().executeQuery("DROP TABLE test_textfile_skip_header_footer");
    }

    @Test
    public void testCreateTextFileTableAsSelectSkipHeaderFooter()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_create_textfile_skip_header");
        onTrino().executeQuery(
                "CREATE TABLE test_create_textfile_skip_header " +
                        "WITH ( " +
                        "   format = 'TEXTFILE', " +
                        "   skip_header_line_count = 1 " +
                        ") " +
                        "AS SELECT 1  AS col_header1, 2 as col_header2;");
        onTrino().executeQuery("INSERT INTO test_create_textfile_skip_header VALUES (3, 4)");
        assertThat(query("SELECT * FROM test_create_textfile_skip_header")).containsOnly(row(1, 2), row(3, 4));
        assertThat(onHive().executeQuery("SELECT * FROM test_create_textfile_skip_header")).containsOnly(row(1, 2), row(3, 4));
        onHive().executeQuery("DROP TABLE test_create_textfile_skip_header");

        onHive().executeQuery("DROP TABLE IF EXISTS test_create_textfile_skip_footer");
        assertThatThrownBy(() -> onTrino().executeQuery(
                "CREATE TABLE test_create_textfile_skip_footer " +
                        "WITH ( " +
                        "   format = 'TEXTFILE', " +
                        "   skip_footer_line_count = 1 " +
                        ") " +
                        "AS SELECT 1  AS col_header;")
        ).hasMessageMatching(".* Creating Hive table with data with value of skip.footer.line.count property greater than 0 is not supported");
        onHive().executeQuery("DROP TABLE test_create_textfile_skip_footer");
    }
}
