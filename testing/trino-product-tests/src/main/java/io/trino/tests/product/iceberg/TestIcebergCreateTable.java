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
package io.trino.tests.product.iceberg;

import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestIcebergCreateTable
        extends ProductTest
{
    @BeforeTestWithContext
    public void setUp()
    {
        // Use IF NOT EXISTS because the schema can be left behind after previous test, as the tests are @Flaky
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS iceberg.iceberg");
    }

    @AfterTestWithContext
    public void cleanUp()
    {
        onTrino().executeQuery("DROP SCHEMA iceberg.iceberg");
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    @Flaky(issue = "https://github.com/trinodb/trino/issues/4864", match = "Failed to read footer of file")
    public void testCreateTable()
    {
        String tableName = "iceberg.iceberg.test_create_table_" + randomTableSuffix();
        onTrino().executeQuery("CREATE TABLE " + tableName + "(a bigint, b varchar)");
        try {
            onTrino().executeQuery("INSERT INTO " + tableName + "(a, b) VALUES " +
                    "(NULL, NULL), " +
                    "(-42, 'abc'), " +
                    "(9223372036854775807, 'abcdefghijklmnopqrstuvwxyz')");
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName))
                    .containsOnly(
                            row(null, null),
                            row(-42, "abc"),
                            row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz"));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    @Flaky(issue = "https://github.com/trinodb/trino/issues/4864", match = "Failed to read footer of file")
    public void testCreateTableAsSelect()
    {
        String tableName = "iceberg.iceberg.test_create_table_as_select_" + randomTableSuffix();
        onTrino().executeQuery("" +
                "CREATE TABLE " + tableName + " AS " +
                "SELECT * FROM (VALUES " +
                "  (NULL, NULL), " +
                "  (-42, 'abc'), " +
                "  (9223372036854775807, 'abcdefghijklmnopqrstuvwxyz')" +
                ") t(a, b)");
        try {
            assertThat(onTrino().executeQuery("SELECT * FROM " + tableName))
                    .containsOnly(
                            row(null, null),
                            row(-42, "abc"),
                            row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz"));
        }
        finally {
            onTrino().executeQuery("DROP TABLE " + tableName);
        }
    }
}
