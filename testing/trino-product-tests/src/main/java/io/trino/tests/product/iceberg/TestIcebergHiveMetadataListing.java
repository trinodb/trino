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

import com.google.common.collect.ImmutableList;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.Iterators.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HMS_ONLY;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.stream.Collectors.toList;

public class TestIcebergHiveMetadataListing
        extends ProductTest
{
    private String storageTable;
    private List<QueryAssert.Row> preexistingTables;
    private List<QueryAssert.Row> preexistingColumns;

    @BeforeTestWithContext
    public void setUp()
    {
        cleanUp();

        preexistingTables = onTrino().executeQuery("SHOW TABLES FROM iceberg.default").rows().stream()
                .map(list -> row(list.toArray()))
                .collect(toList());

        preexistingColumns = onTrino().executeQuery("SELECT table_name, column_name FROM iceberg.information_schema.columns " +
                "WHERE table_catalog = 'iceberg' AND table_schema = 'default'").rows().stream()
                .map(list -> row(list.toArray()))
                .collect(toList());

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
        onTrino().executeQuery("CREATE VIEW iceberg.default.iceberg_view AS SELECT * FROM iceberg.default.iceberg_table1");
    }

    @AfterTestWithContext
    public void cleanUp()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS hive.default.hive_table");
        onTrino().executeQuery("DROP VIEW IF EXISTS hive.default.hive_view");
        onTrino().executeQuery("DROP VIEW IF EXISTS iceberg.default.iceberg_view");
        onTrino().executeQuery("DROP MATERIALIZED VIEW IF EXISTS iceberg.default.iceberg_materialized_view");
        onTrino().executeQuery("DROP TABLE IF EXISTS iceberg.default.iceberg_table1");
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS, HMS_ONLY})
    public void testTableListing()
    {
        assertThat(onTrino().executeQuery("SHOW TABLES FROM iceberg.default"))
                    .containsOnly(ImmutableList.<QueryAssert.Row>builder()
                            .addAll(preexistingTables)
                            .add(row("iceberg_table1"))
                            .add(row("iceberg_materialized_view"))
                            .add(row(storageTable))
                            .add(row("iceberg_view"))
                            // Iceberg connector supports Trino views created via Hive connector
                            .add(row("hive_view"))
                            .build());
    }

    @Test(groups = {ICEBERG, STORAGE_FORMATS, HMS_ONLY})
    public void testColumnListing()
    {
        assertThat(onTrino().executeQuery(
                "SELECT table_name, column_name FROM iceberg.information_schema.columns " +
                        "WHERE table_catalog = 'iceberg' AND table_schema = 'default'"))
                .containsOnly(ImmutableList.<QueryAssert.Row>builder()
                        .addAll(preexistingColumns)
                        .add(row("iceberg_table1", "_string"))
                        .add(row("iceberg_table1", "_integer"))
                        .add(row("iceberg_materialized_view", "_string"))
                        .add(row("iceberg_materialized_view", "_integer"))
                        .add(row(storageTable, "_string"))
                        .add(row(storageTable, "_integer"))
                        .add(row("iceberg_view", "_string"))
                        .add(row("iceberg_view", "_integer"))
                        .add(row("hive_view", "_double"))
                        .build());
    }
}
