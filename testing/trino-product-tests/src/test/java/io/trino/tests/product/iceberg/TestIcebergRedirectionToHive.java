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

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import io.trino.tests.product.hive.HiveIcebergRedirectionsEnvironment;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.trino.plugin.hive.HiveMetadata.MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests interactions between Iceberg and Hive connectors, when one tries to read a table created by the other
 * with redirects enabled.
 * <p>
 * Ported from the Tempto-based TestIcebergRedirectionToHive.
 */
@ProductTest
@RequiresEnvironment(HiveIcebergRedirectionsEnvironment.class)
@TestGroup.HiveIcebergRedirections
class TestIcebergRedirectionToHive
{
    @BeforeEach
    void createAdditionalSchema(HiveIcebergRedirectionsEnvironment env)
    {
        // Create schema via Iceberg catalog - it will be visible in Hive via shared metastore
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS iceberg.nondefaultschema");
    }

    @Test
    void testRedirect(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "redirect_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        assertResultsEqual(
                env.executeTrino("TABLE " + hiveTableName),
                env.executeTrino("TABLE " + icebergTableName));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testRedirectWithNonDefaultSchema(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "redirect_non_default_schema_" + randomNameSuffix();
        String hiveTableName = "hive.nondefaultschema." + tableName;
        String icebergTableName = "iceberg.nondefaultschema." + tableName;

        createHiveTable(env, hiveTableName, false);

        assertResultsEqual(
                env.executeTrino("TABLE " + hiveTableName),
                env.executeTrino("TABLE " + icebergTableName));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testRedirectToNonexistentCatalog(HiveIcebergRedirectionsEnvironment env)
            throws Exception
    {
        String tableName = "redirect_to_nonexistent_hive_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        // sanity check
        assertResultsEqual(
                env.executeTrino("TABLE " + hiveTableName),
                env.executeTrino("TABLE " + icebergTableName));

        // Session setting must persist for the query to use the wrong catalog
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION iceberg.hive_catalog_name = 'someweirdcatalog'");
            assertThatThrownBy(() -> session.executeQuery("TABLE " + icebergTableName))
                    .hasMessageMatching(".*Table 'iceberg.default.redirect_to_nonexistent_hive_.*' redirected to 'someweirdcatalog.default.redirect_to_nonexistent_hive_.*', but the target catalog 'someweirdcatalog' does not exist");
        });

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testRedirectWithDefaultSchemaInSession(HiveIcebergRedirectionsEnvironment env)
            throws Exception
    {
        String tableName = "redirect_with_use_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        // Test USE iceberg.default with unqualified table name
        env.executeTrinoInSession(session -> {
            session.executeUpdate("USE iceberg.default");
            QueryResult unqualified = session.executeQuery("TABLE " + tableName);
            QueryResult qualified = session.executeQuery("TABLE " + hiveTableName);
            assertResultsEqual(unqualified, qualified);
        });

        // Test USE hive.default with unqualified table name
        env.executeTrinoInSession(session -> {
            session.executeUpdate("USE hive.default");
            QueryResult fromIceberg = session.executeQuery("TABLE " + icebergTableName);
            QueryResult unqualified = session.executeQuery("TABLE " + tableName);
            assertResultsEqual(fromIceberg, unqualified);
        });

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testRedirectPartitionsToUnpartitioned(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_unpartitioned_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        assertThatThrownBy(() -> env.executeTrino("TABLE iceberg.default.\"" + tableName + "$partitions\""))
                .cause().hasMessageMatching(".*Table 'iceberg.default.\"" + tableName + "\\$partitions\"' redirected to 'hive.default.\"" + tableName + "\\$partitions\"', " +
                        "but the target table 'hive.default.\"" + tableName + "\\$partitions\"' does not exist");

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testRedirectInvalidSystemTable(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_invalid_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        assertThatThrownBy(() -> env.executeTrino("TABLE iceberg.default.\"" + tableName + "$invalid\""))
                .cause().hasMessageMatching(".*Table 'iceberg.default.\"" + tableName + "\\$invalid\"' redirected to 'hive.default.\"" + tableName + "\\$invalid\"', " +
                        "but the target table 'hive.default.\"" + tableName + "\\$invalid\"' does not exist");

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testRedirectPartitionsToPartitioned(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_partitioned_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;

        createHiveTable(env, hiveTableName, true);

        assertThat(env.executeTrino("TABLE iceberg.default.\"" + tableName + "$partitions\""))
                .containsOnly(
                        row(0L),
                        row(1L),
                        row(2L),
                        row(3L),
                        row(4L));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    static Stream<Arguments> schemaAndPartitioning()
    {
        return Stream.of(
                Arguments.of("default", false),
                Arguments.of("default", true),
                Arguments.of("nondefaultschema", false),
                Arguments.of("nondefaultschema", true));
    }

    @ParameterizedTest
    @MethodSource("schemaAndPartitioning")
    void testInsert(String schema, boolean partitioned, HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_insert_" + randomNameSuffix();
        String hiveTableName = "hive." + schema + "." + tableName;
        String icebergTableName = "iceberg." + schema + "." + tableName;

        createHiveTable(env, hiveTableName, partitioned, false);

        env.executeTrinoUpdate("INSERT INTO " + icebergTableName + " VALUES (42, 'some name', 'some comment', 12)");

        assertThat(env.executeTrino("TABLE " + hiveTableName))
                .containsOnly(row(42L, "some name", "some comment", 12L));
        assertThat(env.executeTrino("TABLE " + icebergTableName))
                .containsOnly(row(42L, "some name", "some comment", 12L));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testDelete(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_delete_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, true);

        env.executeTrinoUpdate("DELETE FROM " + icebergTableName + " WHERE regionkey = 1");

        assertResultsEqual(
                env.executeTrino("TABLE " + icebergTableName),
                env.executeTrino("SELECT nationkey, name, comment, regionkey FROM tpch.tiny.nation WHERE regionkey != 1"));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testUpdate(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_update_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, true);

        assertThatThrownBy(() -> env.executeTrinoUpdate("UPDATE " + icebergTableName + " SET nationkey = nationkey + 100 WHERE regionkey = 1"))
                .cause().hasMessageContaining(MODIFYING_NON_TRANSACTIONAL_TABLE_MESSAGE);

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testCreateOrReplaceTable(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "iceberg_create_or_replace_hive_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        assertThatThrownBy(() -> env.executeTrinoUpdate("CREATE OR REPLACE TABLE " + icebergTableName + " (d integer)"))
                .cause().hasMessageMatching(".*Table '" + icebergTableName + "' of unsupported type already exists");
    }

    @Test
    void testDropTable(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "iceberg_drop_hive_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);
        env.executeTrinoUpdate("DROP TABLE " + icebergTableName);
        assertThatThrownBy(() -> env.executeTrino("TABLE " + hiveTableName))
                .cause().hasMessageMatching(".*Table '" + hiveTableName + "' does not exist");
    }

    @Test
    void testDescribe(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_describe_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, true);

        assertResultsEqual(
                env.executeTrino("DESCRIBE " + icebergTableName),
                env.executeTrino("DESCRIBE " + hiveTableName));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testShowCreateTable(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_show_create_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, true);

        assertThat(env.executeTrino("SHOW CREATE TABLE " + icebergTableName))
                .containsOnly(row("CREATE TABLE " + hiveTableName + " (\n" +
                        "   nationkey bigint,\n" +
                        "   name varchar(25),\n" +
                        "   comment varchar(152),\n" +
                        "   regionkey bigint\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   format = 'ORC',\n" +
                        "   partitioned_by = ARRAY['regionkey']\n" +
                        ")"));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testShowStats(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_show_stats_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, true);

        assertThat(env.executeTrino("SHOW STATS FOR " + icebergTableName))
                .containsOnly(
                        row("nationkey", null, 5d, 0d, null, "0", "24"),
                        row("name", 177d, 5d, 0d, null, null, null),
                        row("comment", 1857d, 5d, 0d, null, null, null),
                        row("regionkey", null, 5d, 0d, null, "0", "4"),
                        row(null, null, null, null, 25d, null, null));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testAlterTableRename(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "iceberg_rename_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE " + icebergTableName + " RENAME TO iceberg.default." + tableName + "_new"))
                .cause().hasMessageContaining("Table rename across catalogs is not supported");

        String newTableNameWithoutCatalogWithoutSchema = tableName + "_new_without_catalog_without_schema";
        env.executeTrinoUpdate("ALTER TABLE " + icebergTableName + " RENAME TO " + newTableNameWithoutCatalogWithoutSchema);
        String newTableNameWithoutCatalogWithSchema = tableName + "_new_without_catalog_with_schema";
        env.executeTrinoUpdate("ALTER TABLE iceberg.default." + newTableNameWithoutCatalogWithoutSchema + " RENAME TO default." + newTableNameWithoutCatalogWithSchema);
        String newTableNameWithCatalogWithSchema = tableName + "_new_with_catalog_with_schema";
        env.executeTrinoUpdate("ALTER TABLE iceberg.default." + newTableNameWithoutCatalogWithSchema + " RENAME TO hive.default." + newTableNameWithCatalogWithSchema);

        assertResultsEqual(
                env.executeTrino("TABLE " + hiveTableName + "_new_with_catalog_with_schema"),
                env.executeTrino("TABLE " + icebergTableName + "_new_with_catalog_with_schema"));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName + "_new_with_catalog_with_schema");
    }

    @Test
    void testAlterTableAddColumn(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_alter_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        env.executeTrinoUpdate("ALTER TABLE " + icebergTableName + " ADD COLUMN some_new_column double");

        assertThat(env.executeTrino("DESCRIBE " + hiveTableName).column(1))
                .containsOnly("nationkey", "name", "regionkey", "comment", "some_new_column");

        assertResultsEqual(
                env.executeTrino("TABLE " + hiveTableName),
                env.executeTrino("SELECT nationkey, name, comment, regionkey, NULL FROM tpch.tiny.nation"));
        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testAlterTableRenameColumn(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_rename_column_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        env.executeTrinoUpdate("ALTER TABLE " + icebergTableName + " RENAME COLUMN nationkey TO nation_key");

        assertThat(env.executeTrino("DESCRIBE " + icebergTableName).column(1))
                .containsOnly("nation_key", "name", "regionkey", "comment");

        assertResultsEqual(
                env.executeTrino("TABLE " + hiveTableName),
                env.executeTrino("SELECT nationkey as nation_key, name, comment, regionkey FROM tpch.tiny.nation"));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testAlterTableDropColumn(HiveIcebergRedirectionsEnvironment env)
            throws Exception
    {
        String tableName = "hive_alter_table_drop_column_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        env.executeTrinoUpdate("ALTER TABLE " + icebergTableName + " DROP COLUMN comment");

        assertThat(env.executeTrino("DESCRIBE " + icebergTableName).column(1))
                .containsOnly("nationkey", "name", "regionkey");

        // After dropping the column from the Hive metastore, access ORC columns by name
        // Session setting must persist for the query
        env.executeTrinoInSession(session -> {
            session.executeUpdate("SET SESSION hive.orc_use_column_names = true");
            QueryResult hiveResult = session.executeQuery("TABLE " + hiveTableName);
            QueryResult expected = session.executeQuery("SELECT nationkey, name, regionkey FROM tpch.tiny.nation");
            assertResultsEqual(hiveResult, expected);
        });
        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testCommentTable(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_comment_table_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        assertTableComment(env, "hive", "default", tableName, null);
        assertTableComment(env, "iceberg", "default", tableName, null);

        String tableComment = "This is my table, there are many like it but this one is mine";
        env.executeTrinoUpdate(String.format("COMMENT ON TABLE %s IS '%s'", icebergTableName, tableComment));

        assertTableComment(env, "hive", "default", tableName, tableComment);
        assertTableComment(env, "iceberg", "default", tableName, tableComment);

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testShowGrants(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_show_grants_" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, true);

        assertThatThrownBy(() -> env.executeTrino(String.format("SHOW GRANTS ON %s", icebergTableName)))
                .cause().hasMessageMatching(".*Table " + icebergTableName + " is redirected to " + hiveTableName + " and SHOW GRANTS is not supported with table redirections");

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testInformationSchemaColumns(HiveIcebergRedirectionsEnvironment env)
    {
        String schemaName = "redirect_information_schema_" + randomNameSuffix();
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        String tableName = "redirect_information_schema_table_" + randomNameSuffix();
        String hiveTableName = "hive." + schemaName + "." + tableName;

        createHiveTable(env, hiveTableName, false);

        // via redirection with table filter
        assertThat(env.executeTrino(
                String.format("SELECT * FROM iceberg.information_schema.columns WHERE table_schema = '%s' AND table_name='%s'", schemaName, tableName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey", 1L, null, "YES", "bigint"),
                        row("iceberg", schemaName, tableName, "name", 2L, null, "YES", "varchar(25)"),
                        row("iceberg", schemaName, tableName, "comment", 3L, null, "YES", "varchar(152)"),
                        row("iceberg", schemaName, tableName, "regionkey", 4L, null, "YES", "bigint"));

        // test via redirection with just schema filter
        assertThat(env.executeTrino(
                String.format("SELECT * FROM iceberg.information_schema.columns WHERE table_schema = '%s'", schemaName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey", 1L, null, "YES", "bigint"),
                        row("iceberg", schemaName, tableName, "name", 2L, null, "YES", "varchar(25)"),
                        row("iceberg", schemaName, tableName, "comment", 3L, null, "YES", "varchar(152)"),
                        row("iceberg", schemaName, tableName, "regionkey", 4L, null, "YES", "bigint"));

        // sanity check that getting columns info without redirection produces matching result
        assertThat(env.executeTrino(
                String.format("SELECT * FROM hive.information_schema.columns WHERE table_schema = '%s' AND table_name='%s'", schemaName, tableName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey", 1L, null, "YES", "bigint"),
                        row("hive", schemaName, tableName, "name", 2L, null, "YES", "varchar(25)"),
                        row("hive", schemaName, tableName, "comment", 3L, null, "YES", "varchar(152)"),
                        row("hive", schemaName, tableName, "regionkey", 4L, null, "YES", "bigint"));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
        env.executeTrinoUpdate("DROP SCHEMA hive." + schemaName);
    }

    @Test
    void testSystemJdbcColumns(HiveIcebergRedirectionsEnvironment env)
    {
        String schemaName = "redirect_system_jdbc_columns_" + randomNameSuffix();
        env.executeTrinoUpdate("CREATE SCHEMA IF NOT EXISTS hive." + schemaName);

        String tableName = "redirect_system_jdbc_columns_table_" + randomNameSuffix();
        String hiveTableName = "hive." + schemaName + "." + tableName;

        createHiveTable(env, hiveTableName, false);

        // via redirection with table filter
        assertThat(env.executeTrino(
                String.format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'iceberg' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey"),
                        row("iceberg", schemaName, tableName, "name"),
                        row("iceberg", schemaName, tableName, "comment"),
                        row("iceberg", schemaName, tableName, "regionkey"));

        // test via redirection with just schema filter
        assertThat(env.executeTrino(
                String.format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'iceberg' AND table_schem = '%s'", schemaName)))
                .containsOnly(
                        row("iceberg", schemaName, tableName, "nationkey"),
                        row("iceberg", schemaName, tableName, "name"),
                        row("iceberg", schemaName, tableName, "comment"),
                        row("iceberg", schemaName, tableName, "regionkey"));

        // sanity check that getting columns info without redirection produces matching result
        assertThat(env.executeTrino(
                String.format("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = '%s' AND table_name = '%s'", schemaName, tableName)))
                .containsOnly(
                        row("hive", schemaName, tableName, "nationkey"),
                        row("hive", schemaName, tableName, "name"),
                        row("hive", schemaName, tableName, "comment"),
                        row("hive", schemaName, tableName, "regionkey"));

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
        env.executeTrinoUpdate("DROP SCHEMA hive." + schemaName);
    }

    @Test
    void testGrant(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_grant" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        assertThatThrownBy(() -> env.executeTrinoUpdate("GRANT SELECT ON " + icebergTableName + " TO ROLE PUBLIC"))
                .cause().hasMessageMatching(".*Table " + icebergTableName + " is redirected to " + hiveTableName + " and GRANT is not supported with table redirections");

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testRevoke(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_revoke" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        assertThatThrownBy(() -> env.executeTrinoUpdate("REVOKE SELECT ON " + icebergTableName + " FROM ROLE PUBLIC"))
                .cause().hasMessageMatching(".*Table " + icebergTableName + " is redirected to " + hiveTableName + " and REVOKE is not supported with table redirections");

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testSetTableAuthorization(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_set_authorization" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        assertThatThrownBy(() -> env.executeTrinoUpdate("ALTER TABLE " + icebergTableName + " SET AUTHORIZATION ROLE PUBLIC"))
                .cause().hasMessageMatching(".*Table " + icebergTableName + " is redirected to " + hiveTableName + " and SET TABLE AUTHORIZATION is not supported with table redirections");

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    @Test
    void testDeny(HiveIcebergRedirectionsEnvironment env)
    {
        String tableName = "hive_deny" + randomNameSuffix();
        String hiveTableName = "hive.default." + tableName;
        String icebergTableName = "iceberg.default." + tableName;

        createHiveTable(env, hiveTableName, false);

        assertThatThrownBy(() -> env.executeTrinoUpdate("DENY DELETE ON " + icebergTableName + " TO ROLE PUBLIC"))
                .cause().hasMessageMatching(".*Table " + icebergTableName + " is redirected to " + hiveTableName + " and DENY is not supported with table redirections");

        env.executeTrinoUpdate("DROP TABLE " + hiveTableName);
    }

    private static void createHiveTable(HiveIcebergRedirectionsEnvironment env, String tableName, boolean partitioned)
    {
        createHiveTable(env, tableName, partitioned, true);
    }

    private static void createHiveTable(HiveIcebergRedirectionsEnvironment env, String tableName, boolean partitioned, boolean withData)
    {
        env.executeTrinoUpdate(
                "CREATE TABLE " + tableName + " " +
                        (partitioned ? "WITH (partitioned_by = ARRAY['regionkey']) " : "") +
                        " AS " +
                        "SELECT nationkey, name, comment, regionkey FROM tpch.tiny.nation " +
                        (withData ? "WITH DATA" : "WITH NO DATA"));
    }

    private static void assertTableComment(HiveIcebergRedirectionsEnvironment env, String catalog, String schema, String tableName, String expectedComment)
    {
        QueryResult result = env.executeTrino(
                String.format("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = '%s' AND schema_name = '%s' AND table_name = '%s'",
                        catalog, schema, tableName));
        if (expectedComment == null) {
            assertThat(result).hasRowCount(1);
            assertThat(result.getOnlyValue()).isNull();
        }
        else {
            assertThat(result).containsOnly(row(expectedComment));
        }
    }

    private static void assertResultsEqual(QueryResult first, QueryResult second)
    {
        assertThat(first).hasSameRowsAs(second);
    }
}
