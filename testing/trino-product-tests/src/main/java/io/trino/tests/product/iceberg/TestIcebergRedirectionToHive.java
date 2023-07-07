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

import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import org.assertj.core.api.AbstractStringAssert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.query.QueryExecutor.param;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE_ICEBERG_REDIRECTIONS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.sql.JDBCType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests interactions between Iceberg and Hive connectors, when one tries to read a table created by the other
 * with redirects enabled.
 *
 * @see TestIcebergHiveTablesCompatibility
 */
public class TestIcebergRedirectionToHive
        extends ProductTest
{
    @BeforeMethodWithContext
    public void createAdditionalSchema()
    {
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS iceberg.nondefaultschema");
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirect()
    {
        String tableName = "redirect_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + hiveTableName),
                onTrino().executeQuery("TABLE " + icebergTableName));

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectWithNonDefaultSchema()
    {
        String tableName = "redirect_non_default_schema_" + randomNameSuffix();
        String hiveTableName = "hive.nondefaultschema." + tableName;
        String icebergTableName = "iceberg.nondefaultschema." + tableName;

        createHiveTable(hiveTableName, false);

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + hiveTableName),
                onTrino().executeQuery("TABLE " + icebergTableName));

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectToNonexistentCatalog()
    {
        String tableName = "redirect_to_nonexistent_hive_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        // sanity check
        assertResultsEqual(
                onTrino().executeQuery("TABLE " + hiveTableName),
                onTrino().executeQuery("TABLE " + icebergTableName));

        onTrino().executeQuery("SET SESSION iceberg.hive_catalog_name = 'someweirdcatalog'");

        assertQueryFailure(() -> onTrino().executeQuery("TABLE " + icebergTableName))
                .hasMessageMatching(".*Table 'iceberg.default.redirect_to_nonexistent_hive_.*' redirected to 'someweirdcatalog.default.redirect_to_nonexistent_hive_.*', but the target catalog 'someweirdcatalog' does not exist");

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    // Note: this tests engine more than connectors. Still good scenario to test.
    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectWithDefaultSchemaInSession()
    {
        String tableName = "redirect_with_use_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        onTrino().executeQuery("USE iceberg.default");
        assertResultsEqual(
                onTrino().executeQuery("TABLE " + tableName), // unqualified
                onTrino().executeQuery("TABLE " + hiveTableName));

        onTrino().executeQuery("USE hive.default");
        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergTableName),
                onTrino().executeQuery("TABLE " + tableName)); // unqualified

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectPartitionsToUnpartitioned()
    {
        String tableName = "hive_unpartitioned_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("TABLE iceberg.default.\"" + tableName + "$partitions\""))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Table '" + icebergTableName + "$partitions' redirected to '" + hiveTableName + "$partitions', but the target table '" + hiveTableName + "$partitions' does not exist");

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectInvalidSystemTable()
    {
        String tableName = "hive_invalid_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("TABLE iceberg.default.\"" + tableName + "$invalid\""))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): Table '" + icebergTableName + "$invalid' redirected to '" + hiveTableName + "$invalid', but the target table '" + hiveTableName + "$invalid' does not exist");

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectPartitionsToPartitioned()
    {
        String tableName = "hive_partitioned_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;

        createHiveTable(hiveTableName, true);

        assertThat(onTrino().executeQuery("TABLE iceberg.default.\"" + tableName + "$partitions\""))
                .containsOnly(
                        row(0),
                        row(1),
                        row(2),
                        row(3),
                        row(4));

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS}, dataProvider = "schemaAndPartitioning")
    public void testInsert(String schema, boolean partitioned)
    {
        String tableName = "hive_insert_" + randomNameSuffix();
        String hiveTableName = "hive." + schema + "." + tableName;
        String icebergTableName = "iceberg." + schema + "." + tableName;

        createHiveTable(hiveTableName, partitioned, false);

        onTrino().executeQuery("INSERT INTO " + icebergTableName + " VALUES (42, 'some name', 'some comment', 12)");

        assertThat(onTrino().executeQuery("TABLE " + hiveTableName))
                .containsOnly(row(42L, "some name", "some comment", 12L));
        assertThat(onTrino().executeQuery("TABLE " + icebergTableName))
                .containsOnly(row(42L, "some name", "some comment", 12L));

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
        String tableName = "hive_delete_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, true);

        onTrino().executeQuery("DELETE FROM " + icebergTableName + " WHERE regionkey = 1");

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergTableName),
                onTrino().executeQuery("SELECT nationkey, name, comment, regionkey FROM tpch.tiny.nation WHERE regionkey != 1"));

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testUpdate()
    {
        String tableName = "hive_update_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, true);

        assertQueryFailure(() -> onTrino().executeQuery("UPDATE " + icebergTableName + " SET nationkey = nationkey + 100 WHERE regionkey = 1"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): " + MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDropTable()
    {
        String tableName = "iceberg_drop_hive_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);
        onTrino().executeQuery("DROP TABLE " + icebergTableName);
        assertQueryFailure(() -> onTrino().executeQuery("TABLE " + hiveTableName))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table '" + hiveTableName + "' does not exist");
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDescribe()
    {
        String tableName = "hive_describe_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, true);

        assertResultsEqual(
                onTrino().executeQuery("DESCRIBE " + icebergTableName),
                onTrino().executeQuery("DESCRIBE " + hiveTableName));

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testShowCreateTable()
    {
        String tableName = "hive_show_create_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, true);

        assertThat(onTrino().executeQuery("SHOW CREATE TABLE " + icebergTableName))
                .containsOnly(row("CREATE TABLE " + hiveTableName + " (\n" +
                        "   nationkey bigint,\n" +
                        "   name varchar(25),\n" +
                        "   comment varchar(152),\n" +
                        "   regionkey bigint\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC',\n" +
                        "   partitioned_by = ARRAY['regionkey']\n" + // 'partitioned_by' comes from Hive
                        ")"));

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testShowStats()
    {
        String tableName = "hive_show_create_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, true);

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + icebergTableName))
                .containsOnly(
                        row("nationkey", null, 5d, 0d, null, "0", "24"),
                        row("name", 177d, 5d, 0d, null, null, null),
                        row("comment", 1857d, 5d, 0d, null, null, null),
                        row("regionkey", null, 5d, 0d, null, "0", "4"),
                        row(null, null, null, null, 25d, null, null));

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableRename()
    {
        String tableName = "iceberg_rename_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + icebergTableName + " RENAME TO iceberg.default." + tableName + "_new"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table rename across catalogs is not supported");

        String newTableNameWithoutCatalogWithoutSchema = tableName + "_new_without_catalog_without_schema";
        onTrino().executeQuery("ALTER TABLE " + icebergTableName + " RENAME TO " + newTableNameWithoutCatalogWithoutSchema);
        String newTableNameWithoutCatalogWithSchema = tableName + "_new_without_catalog_with_schema";
        onTrino().executeQuery("ALTER TABLE iceberg.default." + newTableNameWithoutCatalogWithoutSchema + " RENAME TO default." + newTableNameWithoutCatalogWithSchema);
        String newTableNameWithCatalogWithSchema = tableName + "_new_with_catalog_with_schema";
        onTrino().executeQuery("ALTER TABLE iceberg.default." + newTableNameWithoutCatalogWithSchema + " RENAME TO hive.default." + newTableNameWithCatalogWithSchema);

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + hiveTableName + "_new_with_catalog_with_schema"),
                onTrino().executeQuery("TABLE " + icebergTableName + "_new_with_catalog_with_schema"));

        onTrino().executeQuery("DROP TABLE " + hiveTableName + "_new_with_catalog_with_schema");
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableAddColumn()
    {
        String tableName = "hive_alter_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        onTrino().executeQuery("ALTER TABLE " + icebergTableName + " ADD COLUMN some_new_column double");

        assertThat(onTrino().executeQuery("DESCRIBE " + hiveTableName).column(1))
                .containsOnly("nationkey", "name", "regionkey", "comment", "some_new_column");

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + hiveTableName),
                onTrino().executeQuery("SELECT nationkey, name, comment, regionkey, NULL FROM tpch.tiny.nation"));
        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableRenameColumn()
    {
        String tableName = "hive_rename_column_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        onTrino().executeQuery("ALTER TABLE " + icebergTableName + " RENAME COLUMN nationkey TO nation_key");

        assertThat(onTrino().executeQuery("DESCRIBE " + icebergTableName).column(1))
                .containsOnly("nation_key", "name", "regionkey", "comment");

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + hiveTableName),
                onTrino().executeQuery("SELECT nationkey as nation_key, name, comment, regionkey FROM tpch.tiny.nation"));

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableDropColumn()
    {
        String tableName = "hive_alter_table_drop_column_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        onTrino().executeQuery("ALTER TABLE " + icebergTableName + " DROP COLUMN comment");

        assertThat(onTrino().executeQuery("DESCRIBE " + icebergTableName).column(1))
                .containsOnly("nationkey", "name", "regionkey");

        // After dropping the column from the Hive metastore, access ORC columns by name
        // in order to avoid mismatches between column indexes
        onTrino().executeQuery("SET SESSION hive.orc_use_column_names = true");
        assertResultsEqual(
                onTrino().executeQuery("TABLE " + hiveTableName),
                onTrino().executeQuery("SELECT nationkey, name, regionkey FROM tpch.tiny.nation"));
        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testCommentTable()
    {
        String tableName = "hive_comment_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        assertTableComment("hive", "default", tableName).isNull();
        assertTableComment("iceberg", "default", tableName).isNull();

        String tableComment = "This is my table, there are many like it but this one is mine";
        onTrino().executeQuery(format("COMMENT ON TABLE " + icebergTableName + " IS '%s'", tableComment));

        assertTableComment("hive", "default", tableName).isEqualTo(tableComment);
        assertTableComment("iceberg", "default", tableName).isEqualTo(tableComment);

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testShowGrants()
    {
        String tableName = "hive_show_grants_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, true);

        assertQueryFailure(() -> onTrino().executeQuery(format("SHOW GRANTS ON %s", icebergTableName)))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + icebergTableName + " is redirected to " + hiveTableName + " and SHOW GRANTS is not supported with table redirections");

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testInformationSchemaColumns()
    {
        // use dedicated schema so that we control the number and shape of tables
        String schemaName = "redirect_information_schema_" + randomNameSuffix();
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        String tableName = "redirect_information_schema_table_" + randomNameSuffix();
        String hiveTableName = "hive." + schemaName + "." + tableName;

        createHiveTable(hiveTableName, false);

        // via redirection with table filter
        assertThat(onTrino().executeQuery(
                format("SELECT * FROM iceberg.information_schema.columns WHERE table_schema = '%s' AND table_name='%s'", schemaName, tableName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                        row("iceberg", schemaName, tableName, "name", 2, null, "YES", "varchar(25)"),
                        row("iceberg", schemaName, tableName, "comment", 3, null, "YES", "varchar(152)"),
                        row("iceberg", schemaName, tableName, "regionkey", 4, null, "YES", "bigint"));

        // test via redirection with just schema filter
        assertThat(onTrino().executeQuery(
                format("SELECT * FROM iceberg.information_schema.columns WHERE table_schema = '%s'", schemaName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                        row("iceberg", schemaName, tableName, "name", 2, null, "YES", "varchar(25)"),
                        row("iceberg", schemaName, tableName, "comment", 3, null, "YES", "varchar(152)"),
                        row("iceberg", schemaName, tableName, "regionkey", 4, null, "YES", "bigint"));

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
        String schemaName = "redirect_system_jdbc_columns_" + randomNameSuffix();
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        String tableName = "redirect_system_jdbc_columns_table_" + randomNameSuffix();
        String hiveTableName = "hive." + schemaName + "." + tableName;

        createHiveTable(hiveTableName, false);

        // via redirection with table filter
        assertThat(onTrino().executeQuery(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'iceberg' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey"),
                        row("iceberg", schemaName, tableName, "name"),
                        row("iceberg", schemaName, tableName, "comment"),
                        row("iceberg", schemaName, tableName, "regionkey"));

        // test via redirection with just schema filter - consistent with the functionality of the command `SHOW TABLES` command on the Iceberg connector
        assertThat(onTrino().executeQuery(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'iceberg' AND table_schem = '%s'", schemaName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey"),
                        row("iceberg", schemaName, tableName, "name"),
                        row("iceberg", schemaName, tableName, "comment"),
                        row("iceberg", schemaName, tableName, "regionkey"));

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

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testGrant()
    {
        String tableName = "hive_grant" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("GRANT SELECT ON " + icebergTableName + " TO ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + icebergTableName + " is redirected to " + hiveTableName + " and GRANT is not supported with table redirections");

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRevoke()
    {
        String tableName = "hive_revoke" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("REVOKE SELECT ON " + icebergTableName + " FROM ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + icebergTableName + " is redirected to " + hiveTableName + " and REVOKE is not supported with table redirections");

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testSetTableAuthorization()
    {
        String tableName = "hive_revoke" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + icebergTableName + " SET AUTHORIZATION ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + icebergTableName + " is redirected to " + hiveTableName + " and SET TABLE AUTHORIZATION is not supported with table redirections");

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDeny()
    {
        String tableName = "hive_deny" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(hiveTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("DENY DELETE ON " + icebergTableName + " TO ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + icebergTableName + " is redirected to " + hiveTableName + " and DENY is not supported with table redirections");

        onTrino().executeQuery("DROP TABLE " + hiveTableName);
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
        return assertThat((String) readTableComment(catalog, schema, tableName).getOnlyValue());
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
