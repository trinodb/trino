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

import io.trino.tempto.assertions.QueryAssert.Row;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.HIVE_VIEWS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHivePropertiesTable
        extends HiveProductTest
{
    @Test
    public void testTrinoViewPropertiesTable()
            throws Exception
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS test_trino_view_properties_base");
        onTrino().executeQuery("DROP VIEW IF EXISTS test_trino_view_properties");
        onTrino().executeQuery("CREATE TABLE test_trino_view_properties_base (col INT)");
        onTrino().executeQuery("CREATE VIEW test_trino_view_properties AS SELECT * FROM test_trino_view_properties_base");

        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM \"test_trino_view_properties$properties\""))
                .containsExactlyInOrder(
                        row("comment", "varchar", "", ""),
                        row("presto_query_id", "varchar", "", ""),
                        row("presto_version", "varchar", "", ""),
                        row("presto_view", "varchar", "", ""),
                        row("transient_lastddltime", "varchar", "", ""),
                        row("trino_created_by", "varchar", "", ""));

        assertThat(onTrino().executeQuery("SELECT * FROM \"test_trino_view_properties$properties\""))
                .hasRowsCount(1)
                .containsExactlyInOrder(new Row(getTablePropertiesOnHive("test_trino_view_properties")));

        onTrino().executeQuery("DROP VIEW IF EXISTS test_trino_view_properties");
        onTrino().executeQuery("DROP TABLE IF EXISTS test_trino_view_properties_base");
    }

    @Test(groups = HIVE_VIEWS)
    public void testHiveViewPropertiesTable()
            throws Exception
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS test_hive_view_properties_base");
        onTrino().executeQuery("DROP VIEW IF EXISTS test_hive_view_properties");
        onTrino().executeQuery("CREATE TABLE test_hive_view_properties_base (col INT)");
        onHive().executeQuery("CREATE VIEW test_hive_view_properties AS SELECT * FROM test_hive_view_properties_base");

        // Use "contains" method because the table properties for Hive views aren't identical among testing environments
        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM \"test_hive_view_properties$properties\""))
                .contains(row("transient_lastddltime", "varchar", "", ""));

        assertThat(onTrino().executeQuery("SELECT * FROM \"test_hive_view_properties$properties\""))
                .hasRowsCount(1)
                .containsExactlyInOrder(new Row(getTablePropertiesOnHive("test_hive_view_properties")));

        onTrino().executeQuery("DROP VIEW IF EXISTS test_hive_view_properties");
        onTrino().executeQuery("DROP TABLE IF EXISTS test_hive_view_properties_base");
    }

    private static List<String> getTablePropertiesOnHive(String tableName)
            throws SQLException
    {
        return onHive().executeQuery("SHOW TBLPROPERTIES " + tableName).rows().stream()
                .map(row -> (String) row.get(1))
                .collect(toImmutableList());
    }
}
