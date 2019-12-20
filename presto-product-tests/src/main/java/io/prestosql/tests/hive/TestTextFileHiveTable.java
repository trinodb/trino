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
package io.prestosql.tests.hive;

import com.google.common.io.Resources;
import com.google.inject.Inject;
import io.prestosql.tempto.AfterTestWithContext;
import io.prestosql.tempto.BeforeTestWithContext;
import io.prestosql.tempto.ProductTest;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.io.InputStream;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTextFileHiveTable
        extends ProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @BeforeTestWithContext
    public void setup()
            throws Exception
    {
        hdfsClient.createDirectory("/user/hive/warehouse/TestTextFileHiveTable/single_column");
        try (InputStream inputStream = Resources.asByteSource(Resources.getResource("textfile/single_column.textfile")).openStream()) {
            hdfsClient.saveFile("/user/hive/warehouse/TestTextFileHiveTable/single_column/single_column.textfile", inputStream);
        }
    }

    @AfterTestWithContext
    public void cleanup()
    {
        hdfsClient.delete("/user/hive/warehouse/TestTextFileHiveTable");
    }

    @Test
    public void testCreateTextFileSkipHeaderFooter()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_create_textfile_skip_header");
        onPresto().executeQuery("" +
                                "CREATE TABLE test_create_textfile_skip_header" +
                                " (name varchar) " +
                                "WITH ( " +
                                "   format = 'TEXTFILE', " +
                                "   external_location = 'hdfs://hadoop-master:9000/user/hive/warehouse/TestTextFileHiveTable/single_column', " +
                                "   skip_header_line_count = 1 " +
                                ")");
        assertThat(query("SELECT * FROM test_create_textfile_skip_header")).containsOnly(row("value"), row("footer"));
        onHive().executeQuery("DROP TABLE test_create_textfile_skip_header");

        onHive().executeQuery("DROP TABLE IF EXISTS test_create_textfile_skip_footer");
        onPresto().executeQuery("" +
                                "CREATE TABLE test_create_textfile_skip_footer" +
                                " (name varchar) " +
                                "WITH ( " +
                                "   format = 'TEXTFILE', " +
                                "   external_location = 'hdfs://hadoop-master:9000/user/hive/warehouse/TestTextFileHiveTable/single_column', " +
                                "   skip_footer_line_count = 1 " +
                                ")");
        assertThat(query("SELECT * FROM test_create_textfile_skip_footer")).containsOnly(row("header"), row("value"));
        onHive().executeQuery("DROP TABLE test_create_textfile_skip_footer");

        onHive().executeQuery("DROP TABLE IF EXISTS test_create_textfile_skip_header_footer");
        onPresto().executeQuery("" +
                                "CREATE TABLE test_create_textfile_skip_header_footer" +
                                " (name varchar) " +
                                "WITH ( " +
                                "   format = 'TEXTFILE', " +
                                "   external_location = 'hdfs://hadoop-master:9000/user/hive/warehouse/TestTextFileHiveTable/single_column', " +
                                "   skip_header_line_count = 1, " +
                                "   skip_footer_line_count = 1 " +
                                ")");
        assertThat(query("SELECT * FROM test_create_textfile_skip_header_footer")).containsExactly(row("value"));
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
        assertThatThrownBy(() -> onPresto().executeQuery("INSERT INTO test_textfile_skip_header VALUES (1)"))
                .hasMessageMatching(".* Inserting into Hive table with skip.header.line.count property not supported");
        onHive().executeQuery("DROP TABLE test_textfile_skip_header");

        onHive().executeQuery("DROP TABLE IF EXISTS test_textfile_skip_footer");
        onHive().executeQuery("" +
                "CREATE TABLE test_textfile_skip_footer " +
                " (col1 int) " +
                "STORED AS TEXTFILE " +
                "TBLPROPERTIES ('skip.footer.line.count'='1')");
        assertThatThrownBy(() -> onPresto().executeQuery("INSERT INTO test_textfile_skip_footer VALUES (1)"))
                .hasMessageMatching(".* Inserting into Hive table with skip.footer.line.count property not supported");
        onHive().executeQuery("DROP TABLE test_textfile_skip_footer");

        onHive().executeQuery("DROP TABLE IF EXISTS test_textfile_skip_header_footer");
        onHive().executeQuery("" +
                "CREATE TABLE test_textfile_skip_header_footer " +
                " (col1 int) " +
                "STORED AS TEXTFILE " +
                "TBLPROPERTIES ('skip.header.line.count'='1', 'skip.footer.line.count'='1')");
        assertThatThrownBy(() -> onPresto().executeQuery("INSERT INTO test_textfile_skip_header_footer VALUES (1)"))
                .hasMessageMatching(".* Inserting into Hive table with skip.header.line.count property not supported");
        onHive().executeQuery("DROP TABLE test_textfile_skip_header_footer");
    }
}
