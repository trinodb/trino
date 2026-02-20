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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for CSV file Hive table with skip header/footer functionality.
 * <p>
 * Ported from the Tempto-based TestCsvFileHiveTable.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestCsvFileHiveTable
{
    @Test
    void testCreateCsvFileTableAsSelectSkipHeaderFooter(HiveBasicEnvironment env)
    {
        // Test CTAS with skip header - should work
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_csv_skip_header");
        env.executeTrinoUpdate(
                "CREATE TABLE hive.default.test_create_csv_skip_header " +
                        "WITH ( " +
                        "   format = 'CSV', " +
                        "   skip_header_line_count = 1 " +
                        ") " +
                        "AS SELECT CAST(1 AS VARCHAR) AS col_name1, CAST(2 AS VARCHAR) AS col_name2");
        try {
            env.executeTrinoUpdate("INSERT INTO hive.default.test_create_csv_skip_header VALUES ('3', '4')");
            assertThat(env.executeTrino("SELECT * FROM hive.default.test_create_csv_skip_header")).containsOnly(row("1", "2"), row("3", "4"));
            assertThat(env.executeHive("SELECT * FROM test_create_csv_skip_header")).containsOnly(row("1", "2"), row("3", "4"));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_csv_skip_header");
        }

        // Test CTAS with skip footer - should fail
        env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_csv_skip_footer");
        try {
            assertThatThrownBy(() -> env.executeTrinoUpdate(
                    "CREATE TABLE hive.default.test_create_csv_skip_footer " +
                            "WITH ( " +
                            "   format = 'CSV', " +
                            "   skip_footer_line_count = 1 " +
                            ") " +
                            "AS SELECT CAST(1 AS VARCHAR) AS col_header"))
                    .hasMessageMatching(".* Creating Hive table with data with value of skip.footer.line.count property greater than 0 is not supported");
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS test_create_csv_skip_footer");
        }
    }
}
