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
package io.prestosql.tests.hive;

import io.prestosql.tempto.Requirement;
import io.prestosql.tempto.RequirementsProvider;
import io.prestosql.tempto.configuration.Configuration;
import org.testng.annotations.Test;

import java.math.BigDecimal;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.prestosql.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_VIEWS;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;

public class TestHiveViews
        extends HiveProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return immutableTable(NATION);
    }

    @Test(groups = HIVE_VIEWS)
    public void testSelectOnView()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_test_view");

        onHive().executeQuery("CREATE VIEW hive_test_view AS SELECT * FROM nation");

        assertThat(query("SELECT * FROM hive_test_view")).hasRowsCount(25);
    }

    @Test(groups = HIVE_VIEWS)
    public void testSelectOnViewFromDifferentSchema()
    {
        onHive().executeQuery("DROP SCHEMA IF EXISTS test_schema CASCADE");

        onHive().executeQuery("CREATE SCHEMA test_schema");
        onHive().executeQuery(
                "CREATE VIEW test_schema.hive_test_view_1 AS SELECT * FROM " +
                        // no schema is specified in purpose
                        "nation");

        assertThat(query("SELECT * FROM test_schema.hive_test_view_1")).hasRowsCount(25);
    }

    @Test(groups = HIVE_VIEWS)
    public void testViewWithUnsupportedCoercion()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS view_with_unsupported_coercion");

        onHive().executeQuery("CREATE VIEW view_with_unsupported_coercion AS SELECT length(n_comment) FROM nation");

        assertThat(() -> query("SELECT COUNT(*) FROM view_with_unsupported_coercion"))
                .failsWithMessage("View 'hive.default.view_with_unsupported_coercion' is stale; it must be re-created");
    }

    @Test(groups = HIVE_VIEWS)
    public void testWithUnsupportedFunction()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS view_with_repeat_function");

        onHive().executeQuery("CREATE VIEW view_with_repeat_function AS SELECT REPEAT(n_comment,2) FROM nation");

        assertThat(() -> query("SELECT COUNT(*) FROM view_with_repeat_function"))
                .failsWithMessage("View 'hive.default.view_with_repeat_function' is stale; it must be re-created");
    }

    @Test(groups = HIVE_VIEWS)
    public void testExistingView()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_duplicate_view");

        onHive().executeQuery("CREATE VIEW hive_duplicate_view AS SELECT * FROM nation");

        assertThat(() -> query("CREATE VIEW hive_duplicate_view AS SELECT * FROM nation"))
                .failsWithMessage("View already exists");
    }

    @Test(groups = HIVE_VIEWS)
    public void testShowCreateView()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_show_view");

        onHive().executeQuery("CREATE VIEW hive_show_view AS SELECT * FROM nation");

        // view SQL depends on Hive distribution
        assertThat(query("SHOW CREATE VIEW hive_show_view")).hasRowsCount(1);
    }

    @Test(groups = HIVE_VIEWS)
    public void testUnsupportedLateralViews()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_lateral_view");
        onHive().executeQuery("DROP TABLE IF EXISTS pageAds");

        onHive().executeQuery("CREATE TABLE pageAds(pageid string, adid_list array<int>)");
        onHive().executeQuery("CREATE VIEW hive_lateral_view as SELECT pageid, adid FROM pageAds LATERAL VIEW explode(adid_list) adTable AS adid");

        assertThat(() -> query("SELECT COUNT(*) FROM hive_lateral_view"))
                .failsWithMessage("Failed parsing stored view 'hive.default.hive_lateral_view': line 1:78: mismatched input 'VIEW'");
    }

    @Test(groups = HIVE_VIEWS)
    public void testIdentifierThatStartWithDigit()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS view_on_identifiers_starting_with_numbers");
        onHive().executeQuery("DROP TABLE IF EXISTS 7_table_with_number");

        onHive().executeQuery("CREATE TABLE 7_table_with_number(num string)");
        onHive().executeQuery("CREATE VIEW view_on_identifiers_starting_with_numbers AS SELECT * FROM 7_table_with_number");

        assertThat(query("SELECT COUNT(*) FROM view_on_identifiers_starting_with_numbers")).contains(row(0));
    }

    @Test(groups = HIVE_VIEWS)
    public void testHiveViewInInformationSchema()
    {
        onHive().executeQuery("DROP SCHEMA IF EXISTS test_schema CASCADE;");

        onHive().executeQuery("CREATE SCHEMA test_schema;");
        onHive().executeQuery("CREATE VIEW test_schema.hive_test_view AS SELECT * FROM nation");
        onHive().executeQuery("CREATE TABLE test_schema.hive_table(a string)");
        onPresto().executeQuery("CREATE TABLE test_schema.presto_table(a int)");
        onPresto().executeQuery("CREATE VIEW test_schema.presto_test_view AS SELECT * FROM nation");

        boolean hiveWithTableNamesByType = getHiveVersionMajor() >= 3 ||
                (getHiveVersionMajor() == 2 && getHiveVersionMinor() >= 3);
        assertThat(query("SELECT * FROM information_schema.tables WHERE table_schema = 'test_schema'")).containsOnly(
                row("hive", "test_schema", "presto_table", "BASE TABLE"),
                row("hive", "test_schema", "hive_table", "BASE TABLE"),
                row("hive", "test_schema", "hive_test_view", hiveWithTableNamesByType ? "VIEW" : "BASE TABLE"),
                row("hive", "test_schema", "presto_test_view", "VIEW"));

        assertThat(query("SELECT view_definition FROM information_schema.views WHERE table_schema = 'test_schema' and table_name = 'hive_test_view'")).containsOnly(
                row("SELECT \"nation\".\"n_nationkey\", \"nation\".\"n_name\", \"nation\".\"n_regionkey\", \"nation\".\"n_comment\" FROM \"default\".\"nation\""));

        assertThat(query("DESCRIBE test_schema.hive_test_view"))
                .contains(row("n_nationkey", "bigint", "", ""));
    }

    @Test(groups = HIVE_VIEWS)
    public void testHiveViewWithParametrizedTypes()
    {
        onHive().executeQuery("DROP VIEW IF EXISTS hive_view_parametrized");
        onHive().executeQuery("DROP TABLE IF EXISTS hive_table_parametrized");

        onHive().executeQuery("CREATE TABLE hive_table_parametrized(a decimal(20,4), b bigint, c varchar(20))");
        onHive().executeQuery("CREATE VIEW hive_view_parametrized AS SELECT * FROM hive_table_parametrized");
        onHive().executeQuery("INSERT INTO TABLE hive_table_parametrized VALUES (1.2345, 42, 'bar')");

        assertThat(query("SELECT * FROM hive.default.hive_view_parametrized")).containsOnly(
                row(new BigDecimal("1.2345"), 42, "bar"));

        assertThat(query("SELECT data_type FROM information_schema.columns WHERE table_name = 'hive_view_parametrized'")).containsOnly(
                row("decimal(20,4)"),
                row("bigint"),
                row("varchar(20)"));
    }
}
