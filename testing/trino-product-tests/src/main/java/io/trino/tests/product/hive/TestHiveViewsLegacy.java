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

import com.google.common.collect.ImmutableList;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.Requires;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableNationTable;
import io.trino.tempto.fulfillment.table.hive.tpch.ImmutableTpchTablesRequirements.ImmutableOrdersTable;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HIVE_VIEWS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Requires({
        ImmutableNationTable.class,
        ImmutableOrdersTable.class,
})
public class TestHiveViewsLegacy
        extends AbstractTestHiveViews
{
    @BeforeTestWithContext
    public void setup()
    {
        setSessionProperty("hive.hive_views_legacy_translation", "true");
    }

    @Override
    @Test(groups = HIVE_VIEWS)
    public void testShowCreateView()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_show_view");

        onHive().executeQuery("CREATE VIEW hive_show_view AS SELECT * FROM nation");

        // view SQL depends on Hive distribution
        assertThat(onTrino().executeQuery("SHOW CREATE VIEW hive_show_view")).hasRowsCount(1);
    }

    @Override
    @Test(groups = HIVE_VIEWS)
    public void testHiveViewInInformationSchema()
    {
        onHive().executeQuery("DROP SCHEMA IF EXISTS test_schema CASCADE");

        onHive().executeQuery("CREATE SCHEMA test_schema");
        onHive().executeQuery("CREATE VIEW test_schema.hive_test_view AS SELECT * FROM nation");
        onHive().executeQuery("CREATE TABLE test_schema.hive_table(a string)");
        onTrino().executeQuery("CREATE TABLE test_schema.trino_table(a int)");
        onTrino().executeQuery("CREATE VIEW test_schema.trino_test_view AS SELECT * FROM nation");

        boolean hiveWithTableNamesByType = getHiveVersionMajor() >= 3 ||
                (getHiveVersionMajor() == 2 && getHiveVersionMinor() >= 3);
        assertThat(onTrino().executeQuery("SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema'")).containsOnly(
                row("hive", "test_schema", "trino_table", "BASE TABLE"),
                row("hive", "test_schema", "hive_table", "BASE TABLE"),
                row("hive", "test_schema", "hive_test_view", hiveWithTableNamesByType ? "VIEW" : "BASE TABLE"),
                row("hive", "test_schema", "trino_test_view", "VIEW"));

        assertThat(onTrino().executeQuery("SELECT view_definition FROM information_schema.views WHERE table_schema = 'test_schema' and table_name = 'hive_test_view'")).containsOnly(
                row("SELECT \"nation\".\"n_nationkey\", \"nation\".\"n_name\", \"nation\".\"n_regionkey\", \"nation\".\"n_comment\" FROM \"default\".\"nation\""));

        assertThat(onTrino().executeQuery("DESCRIBE test_schema.hive_test_view"))
                .contains(row("n_nationkey", "bigint", "", ""));
    }

    @Override
    @Test(groups = HIVE_VIEWS)
    public void testHiveViewWithParametrizedTypes()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_view_parametrized");
        onHive().executeQuery("DROP TABLE IF EXISTS hive_table_parametrized");

        onHive().executeQuery("CREATE TABLE hive_table_parametrized(a decimal(20,4), b bigint, c varchar(20))");
        onHive().executeQuery("CREATE VIEW hive_view_parametrized AS SELECT * FROM hive_table_parametrized");
        onHive().executeQuery("INSERT INTO TABLE hive_table_parametrized VALUES (1.2345, 42, 'bar')");

        assertThat(onTrino().executeQuery("SELECT * FROM hive.default.hive_view_parametrized")).containsOnly(
                row(new BigDecimal("1.2345"), 42, "bar"));

        assertThat(onTrino().executeQuery("SELECT data_type FROM information_schema.columns WHERE table_name = 'hive_view_parametrized'")).containsOnly(
                row("decimal(20,4)"),
                row("bigint"),
                row("varchar(20)"));
    }

    @Override
    @Test
    public void testArrayIndexingInView()
    {
        assertThatThrownBy(super::testArrayIndexingInView)
                // Hive uses 0-based array indices so Trino returns incorrect results for such view
                .hasMessageContaining("Could not find rows:\n" +
                        "[hive]\n" +
                        "\n" +
                        "actual rows:\n" +
                        "[presto]");
    }

    @Override
    public void testArrayConstructionInView()
    {
        assertThatThrownBy(super::testArrayConstructionInView)
                .hasMessageContaining("Function 'array' not registered");
    }

    @Override
    public void testMapConstructionInView()
    {
        assertThatThrownBy(super::testMapConstructionInView)
                .hasMessageContaining("Unexpected parameters (varchar(15), varchar(15)) for function map");
    }

    @Override
    public void testPmodFunction()
    {
        assertThatThrownBy(super::testPmodFunction)
                .hasMessageContaining("Function 'pmod' not registered");
    }

    // When doing legacy Hive View translation, Trino columns type signature corresponds one to one with the Hive view columns type
    @Override
    protected List<QueryAssert.Row> getExpectedHiveViewTextualColumnsTypes()
    {
        return ImmutableList.of(
                row("a_char_1", "char(1)"),
                row("a_char_255", "char(255)"),
                row("a_varchar_1", "varchar(1)"),
                row("a_varchar_65535", "varchar(65535)"),
                row("a_string", "varchar"));
    }

    @Override
    public void testNestedHiveViews()
    {
        assertThatThrownBy(super::testNestedHiveViews)
                .hasMessageContaining("View 'hive.default.nested_top_view' is stale or in invalid state: column [n_renamed] of type varchar projected from query view at position 0 cannot be coerced to column [n_renamed] of type varchar(25) stored in view definition");
    }

    @Override
    @Test
    public void testCurrentUser()
    {
        assertThatThrownBy(super::testCurrentUser)
                .hasMessageContaining("Failed parsing stored view 'hive.default.current_user_hive_view': line 1:20: mismatched input '('");
    }

    @Override
    protected QueryExecutor connectToTrino(String catalog)
    {
        QueryExecutor executor = super.connectToTrino(catalog);
        executor.executeQuery("SET SESSION hive.hive_views_legacy_translation = true");
        return executor;
    }
}
