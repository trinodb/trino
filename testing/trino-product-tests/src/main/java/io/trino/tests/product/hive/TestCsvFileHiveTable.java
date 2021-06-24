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

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCsvFileHiveTable
        extends ProductTest
{
    @Test
    public void testCreateCsvFileTableAsSelectSkipHeaderFooter()
    {
        onHive().executeQuery("DROP TABLE IF EXISTS test_create_csv_skip_header");
        assertThatThrownBy(() -> onTrino().executeQuery(
                "CREATE TABLE test_create_csv_skip_header " +
                        "WITH ( " +
                        "   format = 'CSV', " +
                        "   skip_header_line_count = 1 " +
                        ") " +
                        "AS SELECT CAST(1 AS VARCHAR)  AS col_header;")
        ).hasMessageMatching(".* Creating Hive table with data with value of skip.header.line.count property greater than 0 is not supported");
        onHive().executeQuery("DROP TABLE test_create_csv_skip_header");

        onHive().executeQuery("DROP TABLE IF EXISTS test_create_csv_skip_footer");
        assertThatThrownBy(() -> onTrino().executeQuery(
                "CREATE TABLE test_create_csv_skip_footer " +
                        "WITH ( " +
                        "   format = 'CSV', " +
                        "   skip_footer_line_count = 1 " +
                        ") " +
                        "AS SELECT CAST(1 AS VARCHAR)  AS col_header;")
        ).hasMessageMatching(".* Creating Hive table with data with value of skip.footer.line.count property greater than 0 is not supported");
        onHive().executeQuery("DROP TABLE test_create_csv_skip_footer");
    }
}
