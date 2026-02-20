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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.assertj.core.api.AbstractStringAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUnit 5 port of TestHiveRedirectionToIceberg.
 * <p>
 * Tests Hive to Iceberg table redirection functionality.
 */
@ProductTest
@RequiresEnvironment(HiveIcebergRedirectionsEnvironment.class)
@TestGroup.HiveIcebergRedirections
class TestHiveRedirectionToIceberg
{
    @Test
    void testRedirect(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "redirect_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        assertResultsEqual(
                env.executeTrino("TABLE " + icebergTableName),
                env.executeTrino("TABLE " + hiveTableName));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testRedirectWithNonDefaultSchema(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "redirect_non_default_schema_" + randomNameSuffix();
        String hiveTableName = "hive.nondefaultschema." + tableName;
        String icebergTableName = "iceberg.nondefaultschema." + tableName;

        createIcebergTable(env, icebergTableName, false);

        assertResultsEqual(
                env.executeTrino("TABLE " + icebergTableName),
                env.executeTrino("TABLE " + hiveTableName));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testRedirectToNonexistentCatalog(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "redirect_to_nonexistent_iceberg_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        // sanity check
        assertResultsEqual(
                env.executeTrino("TABLE " + icebergTableName),
                env.executeTrino("TABLE " + hiveTableName));

        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.iceberg_catalog_name = 'someweirdcatalog'");

            assertThatThrownBy(() -> session.executeQuery("TABLE " + hiveTableName))
                    .hasMessageMatching(".*Table 'hive.default.redirect_to_nonexistent_iceberg_.*' redirected to 'someweirdcatalog.default.redirect_to_nonexistent_iceberg_.*', but the target catalog 'someweirdcatalog' does not exist");
        });

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    // Note: this tests engine more than connectors. Still good scenario to test.
    @Test
    void testRedirectWithDefaultSchemaInSession(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "redirect_with_use_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        env.executeTrinoInSession(session -> {
            session.executeUpdate("USE iceberg.default");
            assertResultsEqual(
                    session.executeQuery("TABLE " + tableName), // unqualified
                    session.executeQuery("TABLE " + hiveTableName));
        });

        env.executeTrinoInSession(session -> {
            session.executeUpdate("USE hive.default");
            assertResultsEqual(
                    session.executeQuery("TABLE " + icebergTableName),
                    session.executeQuery("TABLE " + tableName)); // unqualified
        });

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testRedirectPartitionsToUnpartitioned(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_unpartitioned_table_" + randomNameSuffix();
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        assertThat(env.executeTrino("" +
                "SELECT record_count, data.nationkey.min, data.nationkey.max, data.name.min, data.name.max " +
                "FROM hive.default.\"" + tableName + "$partitions\""))
                .containsOnly(row(25L, 0L, 24L, "ALGERIA", "VIETNAM"));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testRedirectPartitionsToPartitioned(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_partitioned_table_" + randomNameSuffix();
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, true);

        assertThat(env.executeTrino("" +
                "SELECT partition.regionkey, record_count, data.nationkey.min, data.nationkey.max, data.name.min, data.name.max " +
                "FROM hive.default.\"" + tableName + "$partitions\""))
                .containsOnly(
                        row(0L, 5L, 0L, 16L, "ALGERIA", "MOZAMBIQUE"),
                        row(1L, 5L, 1L, 24L, "ARGENTINA", "UNITED STATES"),
                        row(2L, 5L, 8L, 21L, "CHINA", "VIETNAM"),
                        row(3L, 5L, 6L, 23L, "FRANCE", "UNITED KINGDOM"),
                        row(4L, 5L, 4L, 20L, "EGYPT", "SAUDI ARABIA"));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    static Stream<Arguments> schemaAndPartitioning()
    {
        return Stream.of(
                Arguments.of("default", false),
                Arguments.of("default", true),
                // Note: this tests engine more than connectors. Still good scenario to test.
                Arguments.of("nondefaultschema", false),
                Arguments.of("nondefaultschema", true));
    }

    @ParameterizedTest
    @MethodSource("schemaAndPartitioning")
    void testInsert(String schema, boolean partitioned, HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_insert_" + randomNameSuffix();
        String hiveTableName = "hive." + schema + "." + tableName;
        String icebergTableName = "iceberg." + schema + "." + tableName;

        createIcebergTable(env, icebergTableName, partitioned, false);

        env.executeTrinoUpdate("INSERT INTO " + hiveTableName + " VALUES (42, 'some name', 12, 'some comment')");

        assertThat(env.executeTrino("TABLE " + hiveTableName))
                .containsOnly(row(42L, "some name", 12L, "some comment"));
        assertThat(env.executeTrino("TABLE " + icebergTableName))
                .containsOnly(row(42L, "some name", 12L, "some comment"));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testDelete(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_insert_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, true);

        env.executeTrinoUpdate("DELETE FROM " + hiveTableName + " WHERE regionkey = 1");

        assertResultsEqual(
                env.executeTrino("TABLE " + icebergTableName),
                env.executeTrino("SELECT nationkey, name, regionkey, comment FROM tpch.tiny.nation WHERE regionkey != 1"));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testUpdate(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_insert_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, true);

        int updatedRows = env.executeTrinoUpdate("UPDATE " + hiveTableName + " SET nationkey = nationkey + 100 WHERE regionkey = 1");
        assertThat(updatedRows).isEqualTo(5);

        assertResultsEqual(
                env.executeTrino("SELECT comment, nationkey FROM " + hiveTableName),
                env.executeTrino("SELECT comment, IF(regionkey = 1, nationkey + 100, nationkey) FROM tpch.tiny.nation"));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testMerge(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String sourceTableName = "iceberg_merge_source_" + randomNameSuffix();
        String targetTableName = "iceberg_merge_target_" + randomNameSuffix();
        String hiveSourceTableName = "hive.default." + sourceTableName;
        String hiveTargetTableName = "hive.default." + targetTableName;
        String icebergSourceTableName = "iceberg.default." + sourceTableName;
        String icebergTargetTableName = "iceberg.default." + targetTableName;

        createIcebergTable(env, icebergSourceTableName, true, true);
        createIcebergTable(env, icebergTargetTableName, true, false);

        int updatedRows = env.executeTrinoUpdate("" +
                "MERGE INTO " + hiveTargetTableName + " t USING " + hiveSourceTableName + " s ON t.nationkey = s.nationkey " +
                "WHEN NOT MATCHED " +
                "    THEN INSERT (nationkey, name, regionkey, comment) " +
                "            VALUES (s.nationkey, s.name, s.regionkey, s.comment)");
        assertThat(updatedRows).isEqualTo(25);

        assertResultsEqual(
                env.executeTrino("TABLE " + icebergSourceTableName),
                env.executeTrino("TABLE " + icebergTargetTableName));

        env.executeTrinoUpdate("DROP TABLE " + icebergSourceTableName);
        env.executeTrinoUpdate("DROP TABLE " + icebergTargetTableName);
    }

    @Test
    void testDropTable(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "hive_drop_iceberg_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);
        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
        assertThatThrownBy(() -> env.executeTrino("TABLE " + icebergTableName))
                .hasMessageMatching(".*line 1:1: Table '" + icebergTableName + "' does not exist.*");
    }

    @Test
    void testDescribe(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_describe_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, true);

        assertResultsEqual(
                env.executeTrino("DESCRIBE " + icebergTableName),
                env.executeTrino("DESCRIBE " + hiveTableName));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testShowCreateTable(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_show_create_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, true);

        assertThat((String) env.executeTrino("SHOW CREATE TABLE " + hiveTableName).getOnlyValue())
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

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testShowStats(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_show_create_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, true);

        assertThat(env.executeTrino("SHOW STATS FOR " + hiveTableName))
                .containsOnly(
                        row("nationkey", null, 25d, 0d, null, "0", "24"),
                        row("name", 1182d, 25d, 0d, null, null, null),
                        row("regionkey", null, 5d, 0d, null, "0", "4"),
                        row("comment", 3507d, 25d, 0d, null, null, null),
                        row(null, null, null, null, 25d, null, null));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testAlterTableRename(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_rename_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE " + hiveTableName + " RENAME TO hive.default." + tableName + "_new"))
                .hasMessageMatching(".*line 1:1: Table rename across catalogs is not supported.*");

        String newTableNameWithoutCatalogWithoutSchema = tableName + "_new_without_catalog_without_schema";
        env.executeTrinoUpdate("ALTER TABLE " + hiveTableName + " RENAME TO " + newTableNameWithoutCatalogWithoutSchema);
        String newTableNameWithoutCatalogWithSchema = tableName + "_new_without_catalog_with_schema";
        env.executeTrinoUpdate("ALTER TABLE hive.default." + newTableNameWithoutCatalogWithoutSchema + " RENAME TO default." + newTableNameWithoutCatalogWithSchema);
        String newTableNameWithCatalogWithSchema = tableName + "_new_with_catalog_with_schema";
        env.executeTrinoUpdate("ALTER TABLE hive.default." + newTableNameWithoutCatalogWithSchema + " RENAME TO iceberg.default." + newTableNameWithCatalogWithSchema);

        assertResultsEqual(
                env.executeTrino("TABLE " + icebergTableName + "_new_with_catalog_with_schema"),
                env.executeTrino("TABLE " + hiveTableName + "_new_with_catalog_with_schema"));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName + "_new_with_catalog_with_schema");
    }

    @Test
    void testAlterTableAddColumn(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_alter_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        env.executeTrinoUpdate("ALTER TABLE " + hiveTableName + " ADD COLUMN some_new_column double");

        assertThat(env.executeTrino("DESCRIBE " + icebergTableName).column(1))
                .containsOnly("nationkey", "name", "regionkey", "comment", "some_new_column");

        assertResultsEqual(
                env.executeTrino("TABLE " + icebergTableName),
                env.executeTrino("SELECT * , NULL FROM tpch.tiny.nation"));
        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testAlterTableDropColumn(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_alter_table_drop_column_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        env.executeTrinoUpdate("ALTER TABLE " + hiveTableName + " DROP COLUMN comment");

        assertThat(env.executeTrino("DESCRIBE " + icebergTableName).column(1))
                .containsOnly("nationkey", "name", "regionkey");

        assertResultsEqual(
                env.executeTrino("TABLE " + icebergTableName),
                env.executeTrino("SELECT nationkey, name, regionkey FROM tpch.tiny.nation"));
        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testAlterTableRenameColumn(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_alter_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        env.executeTrinoUpdate("ALTER TABLE " + hiveTableName + " RENAME COLUMN nationkey TO nation_key");

        assertThat(env.executeTrino("DESCRIBE " + icebergTableName).column(1))
                .containsOnly("nation_key", "name", "regionkey", "comment");

        assertResultsEqual(
                env.executeTrino("TABLE " + icebergTableName),
                env.executeTrino("SELECT nationkey as nation_key, name, regionkey, comment FROM tpch.tiny.nation"));
        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testCommentTable(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_comment_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        assertTableComment(env, "hive", "default", tableName).isNull();
        assertTableComment(env, "iceberg", "default", tableName).isNull();

        String tableComment = "This is my table, there are many like it but this one is mine";
        env.executeTrinoUpdate(format("COMMENT ON TABLE %s IS '%s'", hiveTableName, tableComment));

        assertTableComment(env, "hive", "default", tableName).isEqualTo(tableComment);
        assertTableComment(env, "iceberg", "default", tableName).isEqualTo(tableComment);

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testCommentColumn(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_comment_column_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        String columnName = "nationkey";
        createIcebergTable(env, icebergTableName, false);

        assertColumnComment(env, "hive", "default", tableName, columnName).isNull();
        assertColumnComment(env, "iceberg", "default", tableName, columnName).isNull();

        String columnComment = "Internal identifier for the nation";
        env.executeTrinoUpdate(format("COMMENT ON COLUMN %s.%s IS '%s'", hiveTableName, columnName, columnComment));

        assertColumnComment(env, "hive", "default", tableName, columnName).isEqualTo(columnComment);
        assertColumnComment(env, "iceberg", "default", tableName, columnName).isEqualTo(columnComment);

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testShowGrants(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_show_grants_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;
        createIcebergTable(env, icebergTableName, false);

        assertThatThrownBy(() -> env.executeTrino(format("SHOW GRANTS ON %s", hiveTableName)))
                .hasMessageMatching(".*line 1:1: Table " + hiveTableName + " is redirected to " + icebergTableName + " and SHOW GRANTS is not supported with table redirections.*");

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testInformationSchemaColumns(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        // use dedicated schema so that we control the number and shape of tables
        String schemaName = "redirect_information_schema_" + randomNameSuffix();
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        String tableName = "redirect_information_schema_table_" + randomNameSuffix();
        String icebergTableName = "iceberg." + schemaName + "." + tableName;

        createIcebergTable(env, icebergTableName, false);

        // via redirection with table filter
        assertThat(env.executeTrino(
                format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                        row("hive", schemaName, tableName, "name", 2, null, "YES", "varchar"),
                        row("hive", schemaName, tableName, "regionkey", 3, null, "YES", "bigint"),
                        row("hive", schemaName, tableName, "comment", 4, null, "YES", "varchar"));

        // test via redirection with just schema filter
        assertThat(env.executeTrino(
                format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s'", schemaName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                        row("hive", schemaName, tableName, "name", 2, null, "YES", "varchar"),
                        row("hive", schemaName, tableName, "regionkey", 3, null, "YES", "bigint"),
                        row("hive", schemaName, tableName, "comment", 4, null, "YES", "varchar"));

        // sanity check that getting columns info without redirection produces matching result
        assertThat(env.executeTrino(
                format("SELECT * FROM iceberg.information_schema.columns WHERE table_schema = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey", 1, null, "YES", "bigint"),
                        row("iceberg", schemaName, tableName, "name", 2, null, "YES", "varchar"),
                        row("iceberg", schemaName, tableName, "regionkey", 3, null, "YES", "bigint"),
                        row("iceberg", schemaName, tableName, "comment", 4, null, "YES", "varchar"));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
        env.executeTrinoUpdate("DROP SCHEMA hive." + schemaName);
    }

    @Test
    void testSystemJdbcColumns(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        // use dedicated schema so that we control the number and shape of tables
        String schemaName = "redirect_system_jdbc_columns_" + randomNameSuffix();
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        String tableName = "redirect_system_jdbc_columns_table_" + randomNameSuffix();
        String icebergTableName = "iceberg." + schemaName + "." + tableName;

        createIcebergTable(env, icebergTableName, false);

        // via redirection with table filter
        assertThat(env.executeTrino(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey"),
                        row("hive", schemaName, tableName, "name"),
                        row("hive", schemaName, tableName, "regionkey"),
                        row("hive", schemaName, tableName, "comment"));

        // test via redirection with just schema filter
        // via redirection with table filter
        assertThat(env.executeTrino(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = '%s'", schemaName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey"),
                        row("hive", schemaName, tableName, "name"),
                        row("hive", schemaName, tableName, "regionkey"),
                        row("hive", schemaName, tableName, "comment"));

        // sanity check that getting columns info without redirection produces matching result
        assertThat(env.executeTrino(
                format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'iceberg' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey"),
                        row("iceberg", schemaName, tableName, "name"),
                        row("iceberg", schemaName, tableName, "regionkey"),
                        row("iceberg", schemaName, tableName, "comment"));

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
        env.executeTrinoUpdate("DROP SCHEMA hive." + schemaName);
    }

    @Test
    void testGrant(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_grant_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        assertThatThrownBy(() -> env.executeTrinoUpdate("GRANT SELECT ON " + hiveTableName + " TO ROLE PUBLIC"))
                .hasMessageMatching(".*line 1:1: Table " + hiveTableName + " is redirected to " + icebergTableName + " and GRANT is not supported with table redirections.*");

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testRevoke(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_revoke_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        assertThatThrownBy(() -> env.executeTrinoUpdate("REVOKE SELECT ON " + hiveTableName + " FROM ROLE PUBLIC"))
                .hasMessageMatching(".*line 1:1: Table " + hiveTableName + " is redirected to " + icebergTableName + " and REVOKE is not supported with table redirections.*");

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testSetTableAuthorization(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_set_table_authorization_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE " + hiveTableName + " SET AUTHORIZATION ROLE PUBLIC"))
                .hasMessageMatching(".*line 1:1: Table " + hiveTableName + " is redirected to " + icebergTableName + " and SET TABLE AUTHORIZATION is not supported with table redirections.*");

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    @Test
    void testDeny(HiveIcebergRedirectionsEnvironment env)
    {
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive.nondefaultschema");

        String tableName = "iceberg_deny_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createIcebergTable(env, icebergTableName, false);

        assertThatThrownBy(() -> env.executeTrinoUpdate("DENY DELETE ON " + hiveTableName + " TO ROLE PUBLIC"))
                .hasMessageMatching(".*line 1:1: Table " + hiveTableName + " is redirected to " + icebergTableName + " and DENY is not supported with table redirections.*");

        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
    }

    // ==================== Helper Methods ====================

    private static void createIcebergTable(HiveIcebergRedirectionsEnvironment env, String tableName, boolean partitioned)
    {
        createIcebergTable(env, tableName, partitioned, true);
    }

    private static void createIcebergTable(HiveIcebergRedirectionsEnvironment env, String tableName, boolean partitioned, boolean withData)
    {
        env.executeTrinoUpdate(
                "CREATE TABLE " + tableName + " " +
                        (partitioned ? "WITH (partitioning = ARRAY['regionkey']) " : "") +
                        " AS " +
                        "SELECT * FROM tpch.tiny.nation " +
                        (withData ? "WITH DATA" : "WITH NO DATA"));
    }

    private static AbstractStringAssert<?> assertTableComment(HiveIcebergRedirectionsEnvironment env, String catalog, String schema, String tableName)
    {
        return assertThat((String) readTableComment(env, catalog, schema, tableName).getOnlyValue());
    }

    private static QueryResult readTableComment(HiveIcebergRedirectionsEnvironment env, String catalog, String schema, String tableName)
    {
        try (Connection conn = env.createTrinoConnection();
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT comment FROM system.metadata.table_comments WHERE catalog_name = ? AND schema_name = ? AND table_name = ?")) {
            stmt.setString(1, catalog);
            stmt.setString(2, schema);
            stmt.setString(3, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                return QueryResult.forResultSet(rs);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to read table comment", e);
        }
    }

    private static AbstractStringAssert<?> assertColumnComment(HiveIcebergRedirectionsEnvironment env, String catalog, String schema, String tableName, String columnName)
    {
        return assertThat((String) readColumnComment(env, catalog, schema, tableName, columnName).getOnlyValue());
    }

    private static QueryResult readColumnComment(HiveIcebergRedirectionsEnvironment env, String catalog, String schema, String tableName, String columnName)
    {
        try (Connection conn = env.createTrinoConnection();
                PreparedStatement stmt = conn.prepareStatement(
                        "SELECT comment FROM " + catalog + ".information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?")) {
            stmt.setString(1, schema);
            stmt.setString(2, tableName);
            stmt.setString(3, columnName);
            try (ResultSet rs = stmt.executeQuery()) {
                return QueryResult.forResultSet(rs);
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to read column comment", e);
        }
    }

    private static void assertResultsEqual(QueryResult first, QueryResult second)
    {
        List<Row> firstRows = first.rows().stream()
                .map(Row::fromList)
                .toList();
        List<Row> secondRows = second.rows().stream()
                .map(Row::fromList)
                .toList();

        assertThat(first).containsOnly(secondRows.toArray(new Row[0]));

        // just for symmetry
        assertThat(second).containsOnly(firstRows.toArray(new Row[0]));
    }
}
