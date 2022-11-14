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

import io.trino.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HMS_ONLY;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergRenameTable
        extends ProductTest
{
    @Test(groups = {ICEBERG, STORAGE_FORMATS, HMS_ONLY})
    public void testRenameTable()
    {
        String tableName = "iceberg.default.test_rename_table_" + randomNameSuffix();
        String newName = "iceberg.default.test_rename_table_new_" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE " + tableName + "(a bigint, b varchar)");
        try {
            onTrino().executeQuery("INSERT INTO " + tableName + "(a, b) VALUES " +
                    "(NULL, NULL), " +
                    "(-42, 'abc'), " +
                    "(9223372036854775807, 'abcdefghijklmnopqrstuvwxyz')");
            onTrino().executeQuery("ALTER TABLE " + tableName + " RENAME TO " + newName);
            assertThatThrownBy(() -> onTrino().executeQuery("SELECT * FROM " + tableName))
                    .hasMessageContaining("Table '" + tableName + "' does not exist");
            assertThat(onTrino().executeQuery("SELECT * FROM " + newName))
                    .containsOnly(
                            row(null, null),
                            row(-42, "abc"),
                            row(9223372036854775807L, "abcdefghijklmnopqrstuvwxyz"));
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS " + tableName);
            onTrino().executeQuery("DROP TABLE IF EXISTS " + newName);
        }
    }
}
