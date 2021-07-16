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
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.io.InputStream;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;

public class TestWebHdfsHiveTable
{
    @Inject
    private HdfsClient hdfsClient;

    @BeforeTestWithContext
    public void setup()
            throws Exception
    {
        hdfsClient.createDirectory("/user/hive/warehouse/TestWebHdfsHiveTable/single_column");
        try (InputStream inputStream = Resources.asByteSource(Resources.getResource("textfile/single_column.textfile")).openStream()) {
            hdfsClient.saveFile("/user/hive/warehouse/TestWebHdfsHiveTable/single_column/single_column.textfile", inputStream);
        }
    }

    @AfterTestWithContext
    public void cleanup()
    {
        hdfsClient.delete("/user/hive/warehouse/TestWebHdfsHiveTable");
    }

    @Test
    public void testSelectWebHdfsTable()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_create_textfile_webhdfs");
        onPresto().executeQuery("" +
                "CREATE TABLE test_create_textfile_webhdfs" +
                " (name varchar) " +
                "WITH ( " +
                "   format = 'TEXTFILE', " +
                "   external_location = 'webhdfs://hadoop-master:50070/user/hive/warehouse/TestWebHdfsHiveTable/single_column', " +
                "   skip_header_line_count = 1 " +
                ")");
        assertThat(query("SELECT * FROM test_create_textfile_webhdfs")).containsOnly(row("value"), row("footer"));
        onHive().executeQuery("DROP TABLE test_create_textfile_webhdfs");
    }
}
