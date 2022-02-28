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

import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.query.QueryExecutor.param;
import static io.trino.tests.product.TestGroups.HIVE_ICEBERG_REDIRECTIONS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.hive.util.TemporaryHiveTable.randomTableSuffix;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.sql.JDBCType.VARCHAR;

/**
 * Tests interactions between Iceberg and Hive connectors, when one tries to read a table created by the other
 * with redirects enabled.
 *
 * @see TestIcebergHiveTablesCompatibility
 */
public class TestIcebergRedirectionToHive
        extends ProductTest
{
    @BeforeTestWithContext
    public void createAdditionalSchema()
    {
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS iceberg.nondefaultschema");
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirect()
    {
        String tableName = "redirect_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("TABLE " + icebergTableName))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectWithNonDefaultSchema()
    {
        String tableName = "redirect_non_default_schema_" + randomTableSuffix();
        String hiveTableName = "hive.nondefaultschema." + tableName;
        String icebergTableName = "iceberg.nondefaultschema." + tableName;

        createHiveTable(hiveTableName, false);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("TABLE " + icebergTableName))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: nondefaultschema." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectToNonexistentCatalog()
    {
        String tableName = "redirect_to_nonexistent_hive_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("SET SESSION iceberg.hive_catalog_name = 'someweirdcatalog'"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Session property 'iceberg.hive_catalog_name' does not exist");

        assertQueryFailure(() -> onTrino().executeQuery("TABLE " + icebergTableName))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    // Note: this tests engine more than connectors. Still good scenario to test.
    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectWithDefaultSchemaInSession()
    {
        String tableName = "redirect_with_use_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        onTrino().executeQuery("USE iceberg.default");
        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("TABLE " + tableName)) // unqualified
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);
//        assertResultsEqual(
//                onTrino().executeQuery("TABLE " + tableName), // unqualified
//                onTrino().executeQuery("TABLE " + hiveTableName));

        onTrino().executeQuery("USE hive.default");
        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("TABLE " + icebergTableName))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);
//        assertResultsEqual(
//                onTrino().executeQuery("TABLE " + icebergTableName),
//                onTrino().executeQuery("TABLE " + tableName)); // unqualified

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectPartitionsToUnpartitioned()
    {
        String tableName = "hive_unpartitioned_table_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;

        createHiveTable(hiveTableName, false);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("TABLE iceberg.default.\"" + tableName + "$partitions\""))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectPartitionsToPartitioned()
    {
        String tableName = "hive_partitioned_table_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;

        createHiveTable(hiveTableName, true);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("TABLE iceberg.default.\"" + tableName + "$partitions\""))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS}, dataProvider = "schemaAndPartitioning")
    public void testInsert(String schema, boolean partitioned)
    {
        String tableName = "hive_insert_" + randomTableSuffix();
        String hiveTableName = "hive." + schema + "." + tableName;
        String icebergTableName = "iceberg." + schema + "." + tableName;

        createHiveTable(hiveTableName, partitioned, false);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("INSERT INTO " + icebergTableName + " VALUES (6, false, -17), (7, true, 1)"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: " + schema + "." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @DataProvider
    public static Object[][] schemaAndPartitioning()
    {
        return new Object[][] {
                {"default", false},
                {"default", true},
                // Note: this tests engine more than connectors. Still good scenario to test.
                {"nondefaultschema", false},
                {"nondefaultschema", true},
        };
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDelete()
    {
        String tableName = "hive_delete_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, true);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("DELETE FROM " + icebergTableName + " WHERE regionkey = 1"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testUpdate()
    {
        String tableName = "hive_update_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, true);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("UPDATE " + icebergTableName + " SET nationkey = nationkey + 100 WHERE regionkey = 1"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDropTable()
    {
        String tableName = "iceberg_drop_hive_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);
        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("DROP TABLE " + icebergTableName))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);
        // TODO should fail
        onTrino().executeQuery("TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDescribe()
    {
        String tableName = "hive_describe_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, true);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("DESCRIBE " + icebergTableName))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testShowCreateTable()
    {
        String tableName = "hive_show_create_table_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, true);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("SHOW CREATE TABLE " + icebergTableName))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testShowStats()
    {
        String tableName = "hive_show_create_table_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, true);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("SHOW STATS FOR " + icebergTableName))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableRename()
    {
        String tableName = "hive_rename_table_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + icebergTableName + " RENAME TO a_new_name"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableAddColumn()
    {
        String tableName = "hive_alter_table_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + icebergTableName + " ADD COLUMN some_new_column double"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableRenameColumn()
    {
        String tableName = "hive_rename_column_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + icebergTableName + " RENAME COLUMN nationkey TO nation_key"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableDropColumn()
    {
        String tableName = "hive_alter_table_drop_column_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + icebergTableName + " DROP COLUMN comment"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testCommentTable()
    {
        String tableName = "hive_comment_table_" + randomTableSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        assertTableComment("hive", "default", tableName).isNull();
        // TODO: support redirects from Iceberg to Hive
        assertThat(readTableComment("iceberg", "default", tableName)).hasNoRows();

        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery("COMMENT ON TABLE " + icebergTableName + " IS 'This is my table, there are many like it but this one is mine'"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: default." + tableName);

        assertTableComment("hive", "default", tableName).isNull();
        // TODO: support redirects from Iceberg to Hive
        assertThat(readTableComment("iceberg", "default", tableName)).hasNoRows();

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testInformationSchemaColumns()
    {
        // use dedicated schema so that we control the number and shape of tables
        String schemaName = "redirect_information_schema_" + randomTableSuffix();
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        String tableName = "redirect_information_schema_table_" + randomTableSuffix();
        String hiveTableName = "hive." + schemaName + "." + tableName;

        createHiveTable(hiveTableName, false);

        // via redirection with table filter
        // TODO: support redirects from Iceberg to Hive
        assertQueryFailure(() -> onTrino().executeQuery(
                format("SELECT * FROM iceberg.information_schema.columns WHERE table_schema = '%s' AND table_name='%s'", schemaName, tableName)))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Not an Iceberg table: " + schemaName + "." + tableName);

        // test via redirection with just schema filter
        assertThat(onTrino().executeQuery(
                format("SELECT * FROM iceberg.information_schema.columns WHERE table_schema = '%s'", schemaName)))
                .containsOnly(
                        // TODO report table's columns via redirect to Hive
//                        row("iceberg", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
//                        row("iceberg", schemaName, tableName, "name", 2, null, "YES", "varchar(25)"),
//                        row("iceberg", schemaName, tableName, "comment", 3, null, "YES", "varchar(152)"),
//                        row("iceberg", schemaName, tableName, "regionkey", 4, null, "YES", "bigint")
                        /**/);

        // sanity check that getting columns info without redirection produces matching result
        assertThat(onTrino().executeQuery(
                format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s' AND table_name='%s'", schemaName, tableName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                        row("hive", schemaName, tableName, "name", 2, null, "YES", "varchar(25)"),
                        row("hive", schemaName, tableName, "comment", 3, null, "YES", "varchar(152)"),
                        row("hive", schemaName, tableName, "regionkey", 4, null, "YES", "bigint"));

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
        onTrino().executeQuery("DROP SCHEMA hive." + schemaName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testSystemJdbcColumns()
    {
        // use dedicated schema so that we control the number and shape of tables
        String schemaName = "redirect_system_jdbc_columns_" + randomTableSuffix();
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        String tableName = "redirect_system_jdbc_columns_table_" + randomTableSuffix();
        String hiveTableName = "hive." + schemaName + "." + tableName;

        createHiveTable(hiveTableName, false);

        // via redirection with table filter
        assertThat(onTrino().executeQuery(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'iceberg' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        // TODO report table's columns via redirect to Hive
//                        row("iceberg", schemaName, tableName, "nationkey"),
//                        row("iceberg", schemaName, tableName, "name"),
//                        row("iceberg", schemaName, tableName, "comment"),
//                        row("iceberg", schemaName, tableName, "regionkey")
                        /**/);

        // test via redirection with just schema filter
        assertThat(onTrino().executeQuery(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'iceberg' AND table_schem = '%s'", schemaName)))
                .containsOnly(
                        // TODO report table's columns via redirect to Hive
//                        row("iceberg", schemaName, tableName, "nationkey"),
//                        row("iceberg", schemaName, tableName, "name"),
//                        row("iceberg", schemaName, tableName, "comment"),
//                        row("iceberg", schemaName, tableName, "regionkey")
                        /**/);

        // sanity check that getting columns info without redirection produces matching result
        assertThat(onTrino().executeQuery(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey"),
                        row("hive", schemaName, tableName, "name"),
                        row("hive", schemaName, tableName, "comment"),
                        row("hive", schemaName, tableName, "regionkey"));

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
        onTrino().executeQuery("DROP SCHEMA hive." + schemaName);
    }

    private static void createHiveTable(String tableName, boolean partitioned)
    {
        createHiveTable(tableName, partitioned, true);
    }

    private static void createHiveTable(String tableName, boolean partitioned, boolean withData)
    {
        onTrino().executeQuery(
                "CREATE TABLE " + tableName + " " +
                        (partitioned ? "WITH (partitioned_by = ARRAY['regionkey']) " : "") +
                        " AS " +
                        "SELECT nationkey, name, comment, regionkey FROM tpch.tiny.nation " +
                        (withData ? "WITH DATA" : "WITH NO DATA"));
    }

    private static AbstractStringAssert<?> assertTableComment(String catalog, String schema, String tableName)
    {
        QueryResult queryResult = readTableComment(catalog, schema, tableName);
        return Assertions.assertThat((String) getOnlyElement(getOnlyElement(queryResult.rows())));
    }

    private static QueryResult readTableComment(String catalog, String schema, String tableName)
    {
        return onTrino().executeQuery(
                "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = ? AND schema_name = ? AND table_name = ?",
                param(VARCHAR, catalog),
                param(VARCHAR, schema),
                param(VARCHAR, tableName));
    }

    private static void assertResultsEqual(QueryResult first, QueryResult second)
    {
        assertThat(first).containsOnly(second.rows().stream()
                .map(QueryAssert.Row::new)
                .collect(toImmutableList()));

        // just for symmetry
        assertThat(second).containsOnly(first.rows().stream()
                .map(QueryAssert.Row::new)
                .collect(toImmutableList()));
    }
}
