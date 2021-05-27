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
import org.testng.annotations.Test;

import static com.google.common.collect.Iterators.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestIcebergHiveMetadataListing
        extends ProductTest
{
    private String storageTable;

    @BeforeTestWithContext
    public void setUp()
    {
        onTrino().executeQuery("CREATE TABLE iceberg.default.iceberg_table1 (_string VARCHAR, _integer INTEGER)");
        onTrino().executeQuery("CREATE MATERIALIZED VIEW iceberg.default.iceberg_materialized_view AS " +
                "SELECT * FROM iceberg.default.iceberg_table1");
        storageTable = getOnlyElement(onTrino().executeQuery("SHOW TABLES FROM iceberg.default")
                .column(1).stream()
                .map(String.class::cast)
                .filter(name -> name.startsWith("st_"))
                .iterator());

        onTrino().executeQuery("CREATE TABLE hive.default.hive_table (_double DOUBLE)");
        onTrino().executeQuery("CREATE VIEW hive.default.hive_view AS SELECT * FROM hive.default.hive_table");
    }

    @AfterTestWithContext
    public void cleanUp()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS hive.default.hive_table");
        onTrino().executeQuery("DROP VIEW IF EXISTS hive.default.hive_view");
        onTrino().executeQuery("DROP MATERIALIZED VIEW IF EXISTS iceberg.default.iceberg_materialized_view");
        onTrino().executeQuery("DROP TABLE IF EXISTS iceberg.default.iceberg_table1");
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    public void testTableListing()
    {
        assertThat(onTrino().executeQuery("SHOW TABLES FROM iceberg.default"))
                    .containsOnly(
                            row("iceberg_table1"),
                            row("iceberg_materialized_view"),
                            row(storageTable));
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS})
    public void testColumnListing()
    {
        assertThat(onTrino().executeQuery(
                "SELECT table_name, column_name FROM iceberg.information_schema.columns " +
                        "WHERE table_catalog = 'iceberg' AND table_schema = 'default'"))
                .containsOnly(
                        row("iceberg_table1", "_string"),
                        row("iceberg_table1", "_integer"),
                        row("iceberg_materialized_view", "_string"),
                        row("iceberg_materialized_view", "_integer"),
                        row(storageTable, "_string"),
                        row(storageTable, "_integer"));
    }
}
