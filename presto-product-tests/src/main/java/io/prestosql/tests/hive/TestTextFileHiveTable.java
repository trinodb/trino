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

import io.prestosql.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTextFileHiveTable
        extends ProductTest
{
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
