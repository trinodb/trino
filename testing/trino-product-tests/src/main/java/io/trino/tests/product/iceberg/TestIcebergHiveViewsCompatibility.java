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
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tests.product.deltalake.TestHiveAndDeltaLakeCompatibility;
import io.trino.tests.product.hudi.TestHudiHiveViewsCompatibility;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.TestGroups.HMS_ONLY;
import static io.trino.tests.product.TestGroups.ICEBERG;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests interactions between Iceberg and Hive connectors, when one tries to read a view created by the other.
 *
 * @see TestIcebergHiveTablesCompatibility
 * @see TestHiveAndDeltaLakeCompatibility
 * @see TestHudiHiveViewsCompatibility
 */
public class TestIcebergHiveViewsCompatibility
        extends ProductTest
{
    @Test(groups = {ICEBERG, STORAGE_FORMATS, HMS_ONLY})
    public void testIcebergHiveViewsCompatibility()
    {
        try {
            // ensure clean env
            cleanup();

            List<QueryAssert.Row> hivePreexistingTables = onTrino().executeQuery("SHOW TABLES FROM hive.default").rows().stream()
                    .map(list -> row(list.toArray()))
                    .collect(toList());
            List<QueryAssert.Row> icebergPreexistingTables = onTrino().executeQuery("SHOW TABLES FROM iceberg.default").rows().stream()
                    .map(list -> row(list.toArray()))
                    .collect(toList());

            onTrino().executeQuery("CREATE TABLE hive.default.hive_table AS SELECT 1 bee");
            onTrino().executeQuery("CREATE TABLE iceberg.default.iceberg_table AS SELECT 2 snow");

            onTrino().executeQuery("USE hive.default"); // for sake of unqualified table references
            onTrino().executeQuery("CREATE VIEW hive.default.hive_view_qualified_hive AS SELECT * FROM hive.default.hive_table");
            onTrino().executeQuery("CREATE VIEW hive.default.hive_view_unqualified_hive AS SELECT * FROM hive_table");
            onTrino().executeQuery("CREATE VIEW hive.default.hive_view_qualified_iceberg AS SELECT * FROM iceberg.default.iceberg_table");
            assertQueryFailure(() -> onTrino().executeQuery("CREATE VIEW hive.default.hive_view_unqualified_iceberg AS SELECT * FROM iceberg_table"))
                    .hasMessageMatching("Query failed \\(#\\w+\\):\\Q Cannot query Iceberg table 'default.iceberg_table'");

            onTrino().executeQuery("USE iceberg.default"); // for sake of unqualified table references
            onTrino().executeQuery("CREATE VIEW iceberg.default.iceberg_view_qualified_hive AS SELECT * FROM hive.default.hive_table");
            assertThatThrownBy(() -> onTrino().executeQuery("CREATE VIEW iceberg.default.iceberg_view_unqualified_hive AS SELECT * FROM hive_table"))
                    .hasMessageContaining("Not an Iceberg table: default.hive_table");
            onTrino().executeQuery("CREATE VIEW iceberg.default.iceberg_view_qualified_iceberg AS SELECT * FROM iceberg.default.iceberg_table");
            onTrino().executeQuery("CREATE VIEW iceberg.default.iceberg_view_unqualified_iceberg AS SELECT * FROM iceberg_table");

            // select some random catalog so we are not biased towards iceberg.default during assertions
            onTrino().executeQuery("USE tpch.tiny");

            // both hive and iceberg catalogs should list all the tables and views.
            ImmutableList<QueryAssert.Row> newlyCreated = ImmutableList.<QueryAssert.Row>builder()
                    .add(row("hive_table"))
                    .add(row("iceberg_table"))
                    .add(row("hive_view_qualified_hive"))
                    .add(row("hive_view_unqualified_hive"))
                    .add(row("hive_view_qualified_iceberg"))
                    .add(row("iceberg_view_qualified_hive"))
                    .add(row("iceberg_view_qualified_iceberg"))
                    .add(row("iceberg_view_unqualified_iceberg"))
                    .build();

            assertThat(onTrino().executeQuery("SHOW TABLES FROM hive.default"))
                    .containsOnly(ImmutableList.<QueryAssert.Row>builder()
                            .addAll(hivePreexistingTables)
                            .addAll(newlyCreated)
                            .build());

            assertThat(onTrino().executeQuery("SHOW TABLES FROM iceberg.default"))
                    .containsOnly(ImmutableList.<QueryAssert.Row>builder()
                            .addAll(icebergPreexistingTables)
                            .addAll(newlyCreated)
                            .build());

            // try to access all views via hive catalog
            assertThat(onTrino().executeQuery("SELECT * FROM hive.default.hive_view_qualified_hive")).containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT * FROM hive.default.hive_view_unqualified_hive")).containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT * FROM hive.default.hive_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(onTrino().executeQuery("SELECT * FROM hive.default.iceberg_view_qualified_hive")).containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT * FROM hive.default.iceberg_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(onTrino().executeQuery("SELECT * FROM hive.default.iceberg_view_unqualified_iceberg")).containsOnly(row(2));

            // try to access all views via iceberg catalog
            assertThat(onTrino().executeQuery("SELECT * FROM iceberg.default.hive_view_qualified_hive")).containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT * FROM iceberg.default.hive_view_unqualified_hive")).containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT * FROM iceberg.default.hive_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(onTrino().executeQuery("SELECT * FROM iceberg.default.iceberg_view_qualified_hive")).containsOnly(row(1));
            assertThat(onTrino().executeQuery("SELECT * FROM iceberg.default.iceberg_view_qualified_iceberg")).containsOnly(row(2));
            assertThat(onTrino().executeQuery("SELECT * FROM iceberg.default.iceberg_view_unqualified_iceberg")).containsOnly(row(2));
        }
        finally {
            cleanup();
        }
    }

    private void cleanup()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS hive.default.hive_table");
        onTrino().executeQuery("DROP TABLE IF EXISTS iceberg.default.iceberg_table");

        onTrino().executeQuery("DROP VIEW IF EXISTS hive.default.hive_view_qualified_hive");
        onTrino().executeQuery("DROP VIEW IF EXISTS hive.default.hive_view_unqualified_hive");
        onTrino().executeQuery("DROP VIEW IF EXISTS hive.default.hive_view_qualified_iceberg");
        onTrino().executeQuery("DROP VIEW IF EXISTS hive.default.hive_view_unqualified_iceberg");

        onTrino().executeQuery("DROP VIEW IF EXISTS iceberg.default.iceberg_view_qualified_hive");
        onTrino().executeQuery("DROP VIEW IF EXISTS iceberg.default.iceberg_view_unqualified_hive");
        onTrino().executeQuery("DROP VIEW IF EXISTS iceberg.default.iceberg_view_qualified_iceberg");
        onTrino().executeQuery("DROP VIEW IF EXISTS iceberg.default.iceberg_view_unqualified_iceberg");
    }
}
