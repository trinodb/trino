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

import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HMS_ONLY;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestIcebergHiveTablesCompatibility
        extends ProductTest
{
    @Test(groups = {ICEBERG, STORAGE_FORMATS, HMS_ONLY})
    public void testIcebergSelectFromHiveTable()
    {
        String tableName = "test_iceberg_select_from_hive_" + randomTableSuffix();
        onTrino().executeQuery("CREATE TABLE hive.default." + tableName + "(a bigint)");

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM iceberg.default." + tableName))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q Not an Iceberg table: default." + tableName);

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM iceberg.default.\"" + tableName + "$files\""))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE hive.default." + tableName);
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS, HMS_ONLY})
    public void testHiveSelectFromIcebergTable()
    {
        String tableName = "test_hive_select_from_iceberg_" + randomTableSuffix();
        onTrino().executeQuery("CREATE TABLE iceberg.default." + tableName + "(a bigint)");

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hive.default." + tableName))
                // TODO (https://github.com/trinodb/trino/issues/8693) throw specific exception message
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q Unable to create input format org.apache.hadoop.mapred.FileInputFormat");

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hive.default.\"" + tableName + "$partitions\""))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q line 1:15: Table 'hive.default." + tableName + "$partitions' does not exist");

        // TODO (https://github.com/trinodb/trino/issues/8693) should fail
        assertThat(onTrino().executeQuery("SELECT * FROM hive.default.\"" + tableName + "$properties\""))
                .hasRowsCount(1);

        onTrino().executeQuery("DROP TABLE iceberg.default." + tableName);
    }
}
