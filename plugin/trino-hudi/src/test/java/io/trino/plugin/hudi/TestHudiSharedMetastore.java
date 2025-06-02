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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.TestingHivePlugin;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.tpch.TpchTable.NATION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestHudiSharedMetastore
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session hudiSession = testSessionBuilder()
                .setCatalog("hudi")
                .setSchema("default")
                .build();
        Session hiveSession = testSessionBuilder()
                .setCatalog("hive")
                .setSchema("default")
                .build();

        QueryRunner queryRunner = DistributedQueryRunner.builder(hudiSession).build();

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("hudi_data");
        dataDirectory.toFile().deleteOnExit();

        queryRunner.installPlugin(new HudiPlugin());
        queryRunner.createCatalog(
                "hudi",
                "hudi",
                ImmutableMap.of(
                        "hive.metastore", "file",
                        "hive.metastore.catalog.dir", dataDirectory.toString(),
                        "fs.hadoop.enabled", "true"));

        queryRunner.installPlugin(new TestingHivePlugin(dataDirectory));
        queryRunner.createCatalog("hive", "hive");

        queryRunner.execute("CREATE SCHEMA hive.default");

        TpchHudiTablesInitializer tpchHudiTablesInitializer = new TpchHudiTablesInitializer(List.of(NATION));
        tpchHudiTablesInitializer.initializeTables(queryRunner, Location.of(dataDirectory.toString()), "default");

        copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, hiveSession, ImmutableList.of(TpchTable.REGION));

        return queryRunner;
    }

    @Test
    void testHudiSelectFromHiveTable()
    {
        String tableName = "test_hudi_select_from_hive_" + randomNameSuffix();
        assertUpdate("CREATE TABLE hive.default." + tableName + "(a bigint)");

        assertQueryFails("SELECT * FROM hudi.default." + tableName, "Not a Hudi table: default." + tableName);
        assertQueryFails("SELECT * FROM hudi.default.\"" + tableName + "$data\"", ".* Table .* does not exist");
        assertQueryFails("SELECT * FROM hudi.default.\"" + tableName + "$timeline\"", ".* Table .* does not exist");
        assertQueryFails("SELECT * FROM hudi.default.\"" + tableName + "$files\"", ".* Table .* does not exist");

        assertUpdate("DROP TABLE hive.default." + tableName);
    }

    @Test
    void testHiveSelectFromHudiTable()
    {
        String tableName = "test_hive_select_from_hudi_" + randomNameSuffix();
        assertQueryFails("CREATE TABLE hudi.default." + tableName + "(a bigint)", "This connector does not support creating tables");

        // TODO should be "Cannot query Hudi table" once CREATE TABLE is supported
        assertQueryFails("SELECT * FROM hive.default." + tableName, ".* Table .* does not exist");
        assertQueryFails("SELECT * FROM hive.default.\"" + tableName + "$partitions\"", ".* Table .* does not exist");
        assertQueryFails("SELECT * FROM hive.default.\"" + tableName + "$properties\"", "Table .* not found");
    }

    @Test
    void testHudiCannotCreateTableNamesakeToHiveTable()
    {
        String tableName = "test_hudi_create_namesake_hive_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE hive.default." + tableName + "(a bigint)");

        assertQueryFails("CREATE TABLE hudi.default." + tableName + "(a bigint)", ".* Table .* of unsupported type already exists");

        assertUpdate("DROP TABLE hive.default." + tableName);
    }

    @Test
    void testHiveCannotCreateTableNamesakeToHudiTable()
    {
        String tableName = "test_hive_create_namesake_hudi_table_" + randomNameSuffix();
        assertQueryFails("CREATE TABLE hudi.default." + tableName + "(a bigint)", "This connector does not support creating tables");
        // TODO implement test like testHiveCannotCreateTableNamesakeToIcebergTable when CREATE TABLE supported
    }

    @Test
    void testHiveSelectTableColumns()
    {
        assertThat(query("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = 'default' AND table_name = 'region'"))
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('hive', '" + "default" + "', 'region', 'regionkey')," +
                        "('hive', '" + "default" + "', 'region', 'name')," +
                        "('hive', '" + "default" + "', 'region', 'comment')");

        // Hive does not show any information about tables with unsupported format
        assertQueryReturnsEmptyResult("SELECT table_cat, table_schem, table_name, column_name FROM system.jdbc.columns WHERE table_cat = 'hive' AND table_schem = 'default' AND table_name = 'nation'");
    }

    @Test
    void testHiveListsHudiTable()
    {
        String tableName = "test_hive_lists_hudi_table_" + randomNameSuffix();
        assertQueryFails("CREATE TABLE hudi.default." + tableName + "(a bigint)", "This connector does not support creating tables");
        // TODO change doesNotContain to contains once CREATE TABLE supported
        assertThat(query("SHOW TABLES FROM hive.default")).result().onlyColumnAsSet().doesNotContain(tableName);
    }

    @Test
    void testHudiListsHiveTable()
    {
        String tableName = "test_hudi_lists_hive_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE hive.default." + tableName + "(a bigint)");
        assertThat(query("SHOW TABLES FROM hudi.default")).result().onlyColumnAsSet().contains(tableName);
        assertUpdate("DROP TABLE hive.default." + tableName);
    }

    @Test
    void testHudiSelectFromHiveView()
    {
        String tableName = "hudi_from_hive_table_" + randomNameSuffix();
        String viewName = "hudi_from_trino_hive_view_" + randomNameSuffix();
        assertUpdate("CREATE TABLE hive.default." + tableName + " AS SELECT 1 a", 1);
        assertUpdate("CREATE VIEW hive.default." + viewName + " AS TABLE hive.default." + tableName);

        assertQueryFails("SELECT * FROM hudi.default." + viewName, "Not a Hudi table: .*");

        assertUpdate("DROP VIEW hive.default." + viewName);
        assertUpdate("DROP TABLE hive.default." + tableName);
    }

    @Test
    void testHiveSelectFromHudiView()
    {
        assertQueryFails("CREATE VIEW hudi.default.a_new_view AS SELECT 1 a", "This connector does not support creating views");
        // TODO test reading via Hive once Hudi supports CREATE VIEW
    }
}
