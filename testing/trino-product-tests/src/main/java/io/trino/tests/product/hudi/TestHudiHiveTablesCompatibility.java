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
package io.trino.tests.product.hudi;

import io.trino.tempto.ProductTest;
import io.trino.tests.product.iceberg.TestIcebergHiveTablesCompatibility;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HUDI;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests interactions between Hudi and Hive connectors, when one tries to read a table created by the other.
 *
 * @see TestHudiHiveViewsCompatibility
 * @see TestIcebergHiveTablesCompatibility
 */
public class TestHudiHiveTablesCompatibility
        extends ProductTest
{
    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testHudiSelectFromHiveTable()
    {
        String tableName = "test_hudi_select_from_hive_" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE hive.default." + tableName + "(a bigint)");

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hudi.default." + tableName))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q Not a Hudi table: default." + tableName);

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hudi.default.\"" + tableName + "$data\""))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q line 1:15: Table 'hudi.default." + tableName + "$data' does not exist");

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hudi.default.\"" + tableName + "$timeline\""))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q line 1:15: Table 'hudi.default." + tableName + "$timeline' does not exist");

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hudi.default.\"" + tableName + "$files\""))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q Invalid Hudi table name (unknown type 'files'): " + tableName + "$files");

        onTrino().executeQuery("DROP TABLE hive.default." + tableName);
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testHiveSelectFromHudiTable()
    {
        String tableName = "test_hive_select_from_hudi_" + randomNameSuffix();
        assertQueryFailure(() -> onTrino().executeQuery("CREATE TABLE hudi.default." + tableName + "(a bigint)"))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q This connector does not support creating tables");

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hive.default." + tableName))
                // TODO should be "Cannot query Hudi table" once CREATE TABLE is supported
                .hasMessageMatching(format("Query failed \\(#\\w+\\):\\Q line 1:15: Table 'hive.default.%s' does not exist", tableName));

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hive.default.\"" + tableName + "$partitions\""))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q line 1:15: Table 'hive.default." + tableName + "$partitions' does not exist");

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hive.default.\"" + tableName + "$properties\""))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q Table 'default." + tableName + "$properties' not found");
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testHudiCannotCreateTableNamesakeToHiveTable()
    {
        String tableName = "test_hudi_create_namesake_hive_table_" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE hive.default." + tableName + "(a bigint)");

        assertQueryFailure(() -> onTrino().executeQuery("CREATE TABLE hudi.default." + tableName + "(a bigint)"))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q line 1:1: Table 'hudi.default." + tableName + "' of unsupported type already exists");

        onTrino().executeQuery("DROP TABLE hive.default." + tableName);
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testHiveCannotCreateTableNamesakeToHudiTable()
    {
        String tableName = "test_hive_create_namesake_hudi_table_" + randomNameSuffix();
        assertQueryFailure(() -> onTrino().executeQuery("CREATE TABLE hudi.default." + tableName + "(a bigint)"))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q This connector does not support creating tables");
        // TODO implement test like TestIcebergHiveTablesCompatibility.testHiveCannotCreateTableNamesakeToIcebergTable when CREATE TABLE supported
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testHiveSelectTableColumns()
    {
        String hiveTableName = "test_hive_table_columns_table_" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE hive.default." + hiveTableName + "(a bigint)");

        String hudiTableName = "test_hudi_table_columns_table_" + randomNameSuffix();
        assertQueryFailure(() -> onTrino().executeQuery("CREATE TABLE hudi.default." + hudiTableName + "(a bigint)"))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q This connector does not support creating tables");

        assertThat(onTrino().executeQuery(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = 'default' AND table_name = '%s'", hiveTableName)))
                .containsOnly(row("hive", "default", hiveTableName, "a"));
        // Hive does not show any information about tables with unsupported format
        assertThat(onTrino().executeQuery(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = 'default' AND table_name = '%s'", hudiTableName)))
                .hasNoRows();

        onTrino().executeQuery("DROP TABLE hive.default." + hiveTableName);
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testHiveListsHudiTable()
    {
        String tableName = "test_hive_lists_hudi_table_" + randomNameSuffix();
        assertQueryFailure(() -> onTrino().executeQuery("CREATE TABLE hudi.default." + tableName + "(a bigint)"))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q This connector does not support creating tables");
        // TODO change doesNotContain to contains once CREATE TABLE supported
        assertThat(onTrino().executeQuery("SHOW TABLES FROM hive.default").column(1)).doesNotContain(tableName);
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testHudiListsHiveTable()
    {
        String tableName = "test_hudi_lists_hive_table_" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE hive.default." + tableName + "(a bigint)");
        assertThat(onTrino().executeQuery("SHOW TABLES FROM hudi.default")).contains(row(tableName));
        onTrino().executeQuery("DROP TABLE hive.default." + tableName);
    }

    @Test(groups = {HUDI, PROFILE_SPECIFIC_TESTS})
    public void testHudiSelectFromHiveView()
    {
        String tableName = "hudi_from_hive_table_" + randomNameSuffix();
        String viewName = "hudi_from_trino_hive_view_" + randomNameSuffix();
        onTrino().executeQuery("CREATE TABLE hive.default." + tableName + " AS SELECT 1 a");
        onTrino().executeQuery("CREATE VIEW hive.default." + viewName + " AS TABLE hive.default." + tableName);

        assertQueryFailure(() -> onTrino().executeQuery("SELECT * FROM hudi.default." + viewName))
                .hasMessageMatching("Query failed \\(#\\w+\\):\\Q Not a Hudi table: default." + viewName);

        onTrino().executeQuery("DROP VIEW hive.default." + viewName);
        onTrino().executeQuery("DROP TABLE hive.default." + tableName);
    }
}
