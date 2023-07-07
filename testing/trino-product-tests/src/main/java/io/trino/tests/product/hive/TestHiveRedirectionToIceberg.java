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

import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import org.assertj.core.api.AbstractStringAssert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.google.common.collect.ImmutableList.toImmutableList;
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

public class TestHiveRedirectionToIceberg
        extends ProductTest
{
    @BeforeMethodWithContext
    public void createAdditionalSchema()
    {
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirect()
    {
        String tableName = "redirect_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergTableName),
                onTrino().executeQuery("TABLE " + hiveTableName));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectWithNonDefaultSchema()
    {
        String tableName = "redirect_non_default_schema_" + randomNameSuffix();
        String hiveTableName = "hive.nondefaultschema." + tableName;
        String icebergTableName = "iceberg.nondefaultschema." + tableName;

        createIcebergTable(icebergTableName, false);

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergTableName),
                onTrino().executeQuery("TABLE " + hiveTableName));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectToNonexistentCatalog()
    {
        String tableName = "redirect_to_nonexistent_iceberg_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        // sanity check
        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergTableName),
                onTrino().executeQuery("TABLE " + hiveTableName));

        onTrino().executeQuery("SET SESSION hive.iceberg_catalog_name = 'someweirdcatalog'");

        assertQueryFailure(() -> onTrino().executeQuery("TABLE " + hiveTableName))
                .hasMessageMatching(".*Table 'hive.default.redirect_to_nonexistent_iceberg_.*' redirected to 'someweirdcatalog.default.redirect_to_nonexistent_iceberg_.*', but the target catalog 'someweirdcatalog' does not exist");

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    // Note: this tests engine more than connectors. Still good scenario to test.
    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectWithDefaultSchemaInSession()
    {
        String tableName = "redirect_with_use_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        onTrino().executeQuery("USE iceberg.default");
        assertResultsEqual(
                onTrino().executeQuery("TABLE " + tableName), // unqualified
                onTrino().executeQuery("TABLE " + hiveTableName));

        onTrino().executeQuery("USE hive.default");
        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergTableName),
                onTrino().executeQuery("TABLE " + tableName)); // unqualified

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectPartitionsToUnpartitioned()
    {
        String tableName = "iceberg_unpartitioned_table_" + randomNameSuffix();
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        assertThat(onTrino().executeQuery("" +
                "SELECT record_count, data.nationkey.min, data.nationkey.max, data.name.min, data.name.max " +
                "FROM hive.default.\"" + tableName + "$partitions\""))
                .containsOnly(row(25L, 0L, 24L, "ALGERIA", "VIETNAM"));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRedirectPartitionsToPartitioned()
    {
        String tableName = "iceberg_partitioned_table_" + randomNameSuffix();
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, true);

        assertThat(onTrino().executeQuery("" +
                "SELECT partition.regionkey, record_count, data.nationkey.min, data.nationkey.max, data.name.min, data.name.max " +
                "FROM hive.default.\"" + tableName + "$partitions\""))
                .containsOnly(
                        row(0L, 5L, 0L, 16L, "ALGERIA", "MOZAMBIQUE"),
                        row(1L, 5L, 1L, 24L, "ARGENTINA", "UNITED STATES"),
                        row(2L, 5L, 8L, 21L, "CHINA", "VIETNAM"),
                        row(3L, 5L, 6L, 23L, "FRANCE", "UNITED KINGDOM"),
                        row(4L, 5L, 4L, 20L, "EGYPT", "SAUDI ARABIA"));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS}, dataProvider = "schemaAndPartitioning")
    public void testInsert(String schema, boolean partitioned)
    {
        String tableName = "iceberg_insert_" + randomNameSuffix();
        String hiveTableName = "hive." + schema + "." + tableName;
        String icebergTableName = "iceberg." + schema + "." + tableName;

        createIcebergTable(icebergTableName, partitioned, false);

        onTrino().executeQuery("INSERT INTO " + hiveTableName + " VALUES (42, 'some name', 12, 'some comment')");

        assertThat(onTrino().executeQuery("TABLE " + hiveTableName))
                .containsOnly(row(42L, "some name", 12L, "some comment"));
        assertThat(onTrino().executeQuery("TABLE " + icebergTableName))
                .containsOnly(row(42L, "some name", 12L, "some comment"));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
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
        String tableName = "iceberg_insert_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, true);

        onTrino().executeQuery("DELETE FROM " + hiveTableName + " WHERE regionkey = 1");

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergTableName),
                onTrino().executeQuery("SELECT nationkey, name, regionkey, comment FROM tpch.tiny.nation WHERE regionkey != 1"));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testUpdate()
    {
        String tableName = "iceberg_insert_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, true);

        assertThat(onTrino().executeQuery("UPDATE " + hiveTableName + " SET nationkey = nationkey + 100 WHERE regionkey = 1")).updatedRowsCountIsEqualTo(5);
        assertResultsEqual(
                onTrino().executeQuery("SELECT comment, nationkey FROM " + hiveTableName),
                onTrino().executeQuery("SELECT comment, IF(regionkey = 1, nationkey + 100, nationkey) FROM tpch.tiny.nation"));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testMerge()
    {
        String sourceTableName = "iceberg_merge_source_" + randomNameSuffix();
        String targetTableName = "iceberg_merge_target_" + randomNameSuffix();
        String hiveSourceTableName = "hive.default." + sourceTableName;
        String hiveTargetTableName = "hive.default." + targetTableName;
        String icebergSourceTableName = "iceberg.default." + sourceTableName;
        String icebergTargetTableName = "iceberg.default." + targetTableName;

        createIcebergTable(icebergSourceTableName, true, true);
        createIcebergTable(icebergTargetTableName, true, false);

        assertThat(onTrino().executeQuery("" +
                "MERGE INTO " + hiveTargetTableName + " t USING " + hiveSourceTableName + " s ON t.nationkey = s.nationkey " +
                "WHEN NOT MATCHED " +
                "    THEN INSERT (nationkey, name, regionkey, comment) " +
                "            VALUES (s.nationkey, s.name, s.regionkey, s.comment)"))
                .updatedRowsCountIsEqualTo(25);
        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergSourceTableName),
                onTrino().executeQuery("TABLE " + icebergTargetTableName));

        onTrino().executeQuery("DROP TABLE " + icebergSourceTableName);
        onTrino().executeQuery("DROP TABLE " + icebergTargetTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDropTable()
    {
        String tableName = "hive_drop_iceberg_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);
        onTrino().executeQuery("DROP TABLE " + hiveTableName);
        assertQueryFailure(() -> onTrino().executeQuery("TABLE " + icebergTableName))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table '" + icebergTableName + "' does not exist");
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDescribe()
    {
        String tableName = "iceberg_describe_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, true);

        assertResultsEqual(
                onTrino().executeQuery("DESCRIBE " + icebergTableName),
                onTrino().executeQuery("DESCRIBE " + hiveTableName));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testShowCreateTable()
    {
        String tableName = "iceberg_show_create_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, true);

        assertThat((String) onTrino().executeQuery("SHOW CREATE TABLE " + hiveTableName).getOnlyValue())
                .matches("\\QCREATE TABLE " + icebergTableName + " (\n" +
                        "   nationkey bigint,\n" +
                        "   name varchar,\n" +
                        "   regionkey bigint,\n" +
                        "   comment varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'PARQUET',\n" +
                        "   format_version = 2,\n" +
                        format("   location = 'hdfs://hadoop-master:9000/user/hive/warehouse/%s-\\E.*\\Q',\n", tableName) +
                        "   partitioning = ARRAY['regionkey']\n" + // 'partitioning' comes from Iceberg
                        ")\\E");

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testShowStats()
    {
        String tableName = "iceberg_show_create_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, true);

        assertThat(onTrino().executeQuery("SHOW STATS FOR " + hiveTableName))
                .containsOnly(
                        row("nationkey", null, 25d, 0d, null, "0", "24"),
                        row("name", 1231d, 25d, 0d, null, null, null),
                        row("regionkey", null, 5d, 0d, null, "0", "4"),
                        row("comment", 3558d, 25d, 0d, null, null, null),
                        row(null, null, null, null, 25d, null, null));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableRename()
    {
        String tableName = "iceberg_rename_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + hiveTableName + " RENAME TO hive.default." + tableName + "_new"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table rename across catalogs is not supported");

        String newTableNameWithoutCatalogWithoutSchema = tableName + "_new_without_catalog_without_schema";
        onTrino().executeQuery("ALTER TABLE " + hiveTableName + " RENAME TO " + newTableNameWithoutCatalogWithoutSchema);
        String newTableNameWithoutCatalogWithSchema = tableName + "_new_without_catalog_with_schema";
        onTrino().executeQuery("ALTER TABLE hive.default." + newTableNameWithoutCatalogWithoutSchema + " RENAME TO default." + newTableNameWithoutCatalogWithSchema);
        String newTableNameWithCatalogWithSchema = tableName + "_new_with_catalog_with_schema";
        onTrino().executeQuery("ALTER TABLE hive.default." + newTableNameWithoutCatalogWithSchema + " RENAME TO iceberg.default." + newTableNameWithCatalogWithSchema);

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergTableName + "_new_with_catalog_with_schema"),
                onTrino().executeQuery("TABLE " + hiveTableName + "_new_with_catalog_with_schema"));

        onTrino().executeQuery("DROP TABLE " + icebergTableName + "_new_with_catalog_with_schema");
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableAddColumn()
    {
        String tableName = "iceberg_alter_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        onTrino().executeQuery("ALTER TABLE " + hiveTableName + " ADD COLUMN some_new_column double");

        assertThat(onTrino().executeQuery("DESCRIBE " + icebergTableName).column(1))
                .containsOnly("nationkey", "name", "regionkey", "comment", "some_new_column");

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergTableName),
                onTrino().executeQuery("SELECT * , NULL FROM tpch.tiny.nation"));
        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableDropColumn()
    {
        String tableName = "iceberg_alter_table_drop_column_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        onTrino().executeQuery("ALTER TABLE " + hiveTableName + " DROP COLUMN comment");

        assertThat(onTrino().executeQuery("DESCRIBE " + icebergTableName).column(1))
                .containsOnly("nationkey", "name", "regionkey");

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergTableName),
                onTrino().executeQuery("SELECT nationkey, name, regionkey FROM tpch.tiny.nation"));
        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testAlterTableRenameColumn()
    {
        String tableName = "iceberg_alter_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        onTrino().executeQuery("ALTER TABLE " + hiveTableName + " RENAME COLUMN nationkey TO nation_key");

        assertThat(onTrino().executeQuery("DESCRIBE " + icebergTableName).column(1))
                .containsOnly("nation_key", "name", "regionkey", "comment");

        assertResultsEqual(
                onTrino().executeQuery("TABLE " + icebergTableName),
                onTrino().executeQuery("SELECT nationkey as nation_key, name, regionkey, comment FROM tpch.tiny.nation"));
        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testCommentTable()
    {
        String tableName = "iceberg_comment_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        assertTableComment("hive", "default", tableName).isNull();
        assertTableComment("iceberg", "default", tableName).isNull();

        String tableComment = "This is my table, there are many like it but this one is mine";
        onTrino().executeQuery(format("COMMENT ON TABLE " + hiveTableName + " IS '%s'", tableComment));

        assertTableComment("hive", "default", tableName).isEqualTo(tableComment);
        assertTableComment("iceberg", "default", tableName).isEqualTo(tableComment);

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testCommentColumn()
    {
        String tableName = "iceberg_comment_column_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String columnName = "nationkey";
        createIcebergTable(icebergTableName, false);

        assertColumnComment("hive", "default", tableName, columnName).isNull();
        assertColumnComment("iceberg", "default", tableName, columnName).isNull();

        String columnComment = "Internal identifier for the nation";
        onTrino().executeQuery(format("COMMENT ON COLUMN %s.%s IS '%s'", hiveTableName, columnName, columnComment));

        assertColumnComment("hive", "default", tableName, columnName).isEqualTo(columnComment);
        assertColumnComment("iceberg", "default", tableName, columnName).isEqualTo(columnComment);

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testShowGrants()
    {
        String tableName = "iceberg_show_grants_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        createIcebergTable(icebergTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery(format("SHOW GRANTS ON %s", hiveTableName)))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + hiveTableName + " is redirected to " + icebergTableName + " and SHOW GRANTS is not supported with table redirections");

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testInformationSchemaColumns()
    {
        // use dedicated schema so that we control the number and shape of tables
        String schemaName = "redirect_information_schema_" + randomNameSuffix();
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        String tableName = "redirect_information_schema_table_" + randomNameSuffix();
        String icebergTableName = "iceberg." + schemaName + "." + tableName;

        createIcebergTable(icebergTableName, false);

        // via redirection with table filter
        assertThat(onTrino().executeQuery(
                format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                        row("hive", schemaName, tableName, "name", 2, null, "YES", "varchar"),
                        row("hive", schemaName, tableName, "regionkey", 3, null, "YES", "bigint"),
                        row("hive", schemaName, tableName, "comment", 4, null, "YES", "varchar"));

        // test via redirection with just schema filter
        assertThat(onTrino().executeQuery(
                format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s'", schemaName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                        row("hive", schemaName, tableName, "name", 2, null, "YES", "varchar"),
                        row("hive", schemaName, tableName, "regionkey", 3, null, "YES", "bigint"),
                        row("hive", schemaName, tableName, "comment", 4, null, "YES", "varchar"));

        // sanity check that getting columns info without redirection produces matching result
        assertThat(onTrino().executeQuery(
                format("SELECT * FROM iceberg.information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                        row("iceberg", schemaName, tableName, "name", 2, null, "YES", "varchar"),
                        row("iceberg", schemaName, tableName, "regionkey", 3, null, "YES", "bigint"),
                        row("iceberg", schemaName, tableName, "comment", 4, null, "YES", "varchar"));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
        onTrino().executeQuery("DROP SCHEMA hive." + schemaName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testSystemJdbcColumns()
    {
        // use dedicated schema so that we control the number and shape of tables
        String schemaName = "redirect_system_jdbc_columns_" + randomNameSuffix();
        onTrino().executeQuery("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        String tableName = "redirect_system_jdbc_columns_table_" + randomNameSuffix();
        String icebergTableName = "iceberg." + schemaName + "." + tableName;

        createIcebergTable(icebergTableName, false);

        // via redirection with table filter
        assertThat(onTrino().executeQuery(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey"),
                        row("hive", schemaName, tableName, "name"),
                        row("hive", schemaName, tableName, "regionkey"),
                        row("hive", schemaName, tableName, "comment"));

        // test via redirection with just schema filter
        // via redirection with table filter
        assertThat(onTrino().executeQuery(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = '%s'", schemaName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey"),
                        row("hive", schemaName, tableName, "name"),
                        row("hive", schemaName, tableName, "regionkey"),
                        row("hive", schemaName, tableName, "comment"));

        // sanity check that getting columns info without redirection produces matching result
        assertThat(onTrino().executeQuery(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'iceberg' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey"),
                        row("iceberg", schemaName, tableName, "name"),
                        row("iceberg", schemaName, tableName, "regionkey"),
                        row("iceberg", schemaName, tableName, "comment"));

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
        onTrino().executeQuery("DROP SCHEMA hive." + schemaName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testGrant()
    {
        String tableName = "iceberg_grant_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("GRANT SELECT ON " + hiveTableName + " TO ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + hiveTableName + " is redirected to " + icebergTableName + " and GRANT is not supported with table redirections");

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testRevoke()
    {
        String tableName = "iceberg_revoke_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("REVOKE SELECT ON " + hiveTableName + " FROM ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + hiveTableName + " is redirected to " + icebergTableName + " and REVOKE is not supported with table redirections");

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testSetTableAuthorization()
    {
        String tableName = "iceberg_set_table_authorization_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("ALTER TABLE " + hiveTableName + " SET AUTHORIZATION ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + hiveTableName + " is redirected to " + icebergTableName + " and SET TABLE AUTHORIZATION is not supported with table redirections");

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    @Test(groups = {HIVE_ICEBERG_REDIRECTIONS, PROFILE_SPECIFIC_TESTS})
    public void testDeny()
    {
        String tableName = "iceberg_deny_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(icebergTableName, false);

        assertQueryFailure(() -> onTrino().executeQuery("DENY DELETE ON " + hiveTableName + " TO ROLE PUBLIC"))
                .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): line 1:1: Table " + hiveTableName + " is redirected to " + icebergTableName + " and DENY is not supported with table redirections");

        onTrino().executeQuery("DROP TABLE " + icebergTableName);
    }

    private static void createIcebergTable(String tableName, boolean partitioned)
    {
        createIcebergTable(tableName, partitioned, true);
    }

    private static void createIcebergTable(String tableName, boolean partitioned, boolean withData)
    {
        onTrino().executeQuery(
                "CREATE TABLE " + tableName + " " +
                        (partitioned ? "WITH (partitioning = ARRAY['regionkey']) " : "") +
                        " AS " +
                        "SELECT * FROM tpch.tiny.nation " +
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

    private static AbstractStringAssert<?> assertColumnComment(String catalog, String schema, String tableName, String columnName)
    {
        return assertThat((String) readColumnComment(catalog, schema, tableName, columnName).getOnlyValue());
    }

    private static QueryResult readColumnComment(String catalog, String schema, String tableName, String columnName)
    {
        return onTrino().executeQuery(
                format("SELECT comment FROM %s.information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?", catalog),
                param(VARCHAR, schema),
                param(VARCHAR, tableName),
                param(VARCHAR, columnName));
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
