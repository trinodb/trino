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

import io.trino.tempto.AfterMethodWithContext;
import io.trino.tempto.BeforeMethodWithContext;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveMaterializedView
        extends HiveProductTest
{
    private boolean isTestEnabled()
    {
        // MATERIALIZED VIEW is supported since Hive 3
        return getHiveVersionMajor() >= 3;
    }

    @BeforeMethodWithContext
    public void setUp()
    {
        if (!isTestEnabled()) {
            return;
        }

        onHive().executeQuery("" +
                "CREATE TABLE test_materialized_view_table(x string) " +
                "STORED AS ORC " +
                "TBLPROPERTIES('transactional'='true')");
        onHive().executeQuery("INSERT INTO test_materialized_view_table VALUES ('a'), ('a'), ('b')");
    }

    @AfterMethodWithContext
    public void tearDown()
    {
        if (!isTestEnabled()) {
            return;
        }

        onHive().executeQuery("DROP TABLE IF EXISTS test_materialized_view_table");
    }

    @Test(groups = STORAGE_FORMATS)
    public void testMaterializedView()
    {
        testMaterializedView(false);
    }

    @Test(groups = STORAGE_FORMATS)
    public void testPartitionedMaterializedView()
    {
        testMaterializedView(true);
    }

    private void testMaterializedView(boolean partitioned)
    {
        if (!isTestEnabled()) {
            return;
        }

        onHive().executeQuery("DROP MATERIALIZED VIEW test_materialized_view_view");
        onHive().executeQuery("" +
                "CREATE MATERIALIZED VIEW test_materialized_view_view " +
                (partitioned ? "PARTITIONED ON (x) " : "") +
                "STORED AS ORC " +
                "AS SELECT x, count(*) c FROM test_materialized_view_table GROUP BY x");

        // metadata
        assertThat(onTrino().executeQuery("SHOW TABLES"))
                .contains(
                        row("test_materialized_view_table"),
                        row("test_materialized_view_view"));

        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM test_materialized_view_view"))
                .containsOnly(
                        row("c", "bigint", "", ""),
                        row("x", "varchar", partitioned ? "partition key" : "", ""));

        // read
        assertThat(onTrino().executeQuery("SELECT x, c FROM test_materialized_view_view"))
                .containsOnly(row("a", 2), row("b", 1));
        assertThat(onTrino().executeQuery("SELECT x, c FROM test_materialized_view_view WHERE x = 'a'"))
                .containsOnly(row("a", 2));

        // write
        assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO test_materialized_view_view(x, c) VALUES ('x', 42)"))
                .hasMessageContaining("Cannot write to Hive materialized view");

        onHive().executeQuery("DROP MATERIALIZED VIEW test_materialized_view_view");
    }
}
