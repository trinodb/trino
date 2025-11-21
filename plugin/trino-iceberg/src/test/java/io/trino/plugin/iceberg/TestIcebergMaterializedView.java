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
package io.trino.plugin.iceberg;

import io.trino.Session;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergMaterializedView
        extends BaseIcebergMaterializedViewTest
{
    private Session secondIceberg;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .build();
        try {
            metastore = getHiveMetastore(queryRunner);

            queryRunner.createCatalog("iceberg2", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg2-catalog").toString(),
                    "iceberg.hive-catalog-name", "hive",
                    "fs.hadoop.enabled", "true"));

            secondIceberg = Session.builder(queryRunner.getDefaultSession())
                    .setCatalog("iceberg2")
                    .build();

            queryRunner.createCatalog("iceberg_legacy_mv", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toString(),
                    "iceberg.hive-catalog-name", "hive",
                    "iceberg.materialized-views.hide-storage-table", "false",
                    "fs.hadoop.enabled", "true"));

            queryRunner.execute(secondIceberg, "CREATE SCHEMA " + secondIceberg.getSchema().orElseThrow());

            queryRunner.installPlugin(createMockConnectorPlugin());
            queryRunner.createCatalog("mock", "mock");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Override
    protected String getSchemaDirectory()
    {
        return "local:///tpch";
    }

    @Override
    protected String getStorageMetadataLocation(String materializedViewName)
    {
        Table table = metastore.getTable("tpch", materializedViewName).orElseThrow();
        return table.getParameters().get(METADATA_LOCATION_PROP);
    }

    @Test
    public void testTwoIcebergCatalogs()
    {
        Session defaultIceberg = getSession();

        // Base table for staleness check
        String createTable = "CREATE TABLE common_base_table AS SELECT 10 value";
        assertUpdate(secondIceberg, createTable, 1); // this one will be used by MV
        assertUpdate(defaultIceberg, createTable, 1); // this one exists so that it can be mistakenly treated as the base table

        assertUpdate(defaultIceberg,
                """
                CREATE MATERIALIZED VIEW iceberg.tpch.mv_on_iceberg2
                AS SELECT sum(value) AS s FROM iceberg2.tpch.common_base_table
                """);

        // The MV is initially stale
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"common_base_table\"");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After REFRESH, the MV is fresh
        assertUpdate(defaultIceberg, "REFRESH MATERIALIZED VIEW mv_on_iceberg2", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After INSERT to the base table, the MV is still fresh, because it currently does not detect changes to tables in other catalog.
        assertUpdate(secondIceberg, "INSERT INTO common_base_table VALUES 7", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '10'");

        // After REFRESH, the MV is fresh again
        assertUpdate(defaultIceberg, "REFRESH MATERIALIZED VIEW mv_on_iceberg2", 1);
        assertThat(getExplainPlan("TABLE mv_on_iceberg2", ExplainType.Type.IO))
                .contains("\"table\" : \"mv_on_iceberg2$materialized_view_storage")
                .doesNotContain("common_base_table");
        assertThat(query("TABLE mv_on_iceberg2"))
                .matches("VALUES BIGINT '17'");

        assertUpdate(secondIceberg, "DROP TABLE common_base_table");
        assertUpdate(defaultIceberg, "DROP TABLE common_base_table");
        assertUpdate("DROP MATERIALIZED VIEW mv_on_iceberg2");
    }

    /**
     * Test CREATE OR REPLACE MATERIALIZED VIEW and with all configurations for
     * unique table location and hiding storage table.
     */
    @Test
    public void testReplaceAndDropMaterializedView() {
        testReplaceAndDropMaterializedView(false, false);
        testReplaceAndDropMaterializedView(false, true);
        testReplaceAndDropMaterializedView(true, false);
        testReplaceAndDropMaterializedView(true, true);
    }

    /**
     * Test CREATE OR REPLACE MATERIALIZED VIEW and DROP MATERIALIZED VIEW statements.
     * The test creates an Iceberg table and a materialized view on top of it,
     * then replaces the materialized view definition twice, and finally drops the materialized view.
     */
    private void testReplaceAndDropMaterializedView(boolean uniqueTableLocation, boolean hideStorageTable) {
        QueryRunner queryRunner = getQueryRunner();
        String catalog = String.format("iceberg_%s_%s", uniqueTableLocation ? "unique" : "not_unique",
                hideStorageTable ? "storage" : "not_storage");
        queryRunner.createCatalog(catalog, "iceberg", Map.of(
                "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                "iceberg.unique-table-location", uniqueTableLocation ? "true" : "false",
                "hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve(catalog + "-catalog").toString(),
                "iceberg.hive-catalog-name", "hive",
                "iceberg.materialized-views.hide-storage-table", hideStorageTable ? "true" : "false",
                "fs.hadoop.enabled", "true"));
        Session session = Session.builder(queryRunner.getDefaultSession())
                .setCatalog(catalog)
                .setSchema("default")
                .build();
        assertUpdate(session, "CREATE SCHEMA default");

        assertUpdate(session, "CREATE TABLE replace_base_table AS SELECT 10 value", 1);

        // A new materialized view is created and fresh results are stored in its storage table
        // Test that the materialized view works as expected (one row with proper value)
        assertUpdate(session, "CREATE OR REPLACE MATERIALIZED VIEW replace_view" +
                " AS SELECT sum(value) AS s FROM replace_base_table");
        assertUpdate(session, "REFRESH MATERIALIZED VIEW replace_view", 1);
        assertQuery(session, "SELECT count(*) FROM replace_view", "VALUES 1");
        assertQuery(session, "SELECT * FROM replace_view", "VALUES 10");

        // The materialized view is replaced with a new definition and fresh results are stored in its storage table
        // Test that the materialized view works as expected (one row with proper value)
        assertUpdate(session, "CREATE OR REPLACE MATERIALIZED VIEW replace_view" +
                " AS SELECT 2 * sum(value) AS t FROM replace_base_table");
        assertUpdate(session, "REFRESH MATERIALIZED VIEW replace_view", 1);
        assertQuery(session, "SELECT count(*) FROM replace_view", "VALUES 1");
        assertQuery(session, "SELECT * FROM replace_view", "VALUES 20");

        // The materialized view is replaced again and tested
        assertUpdate(session, "CREATE OR REPLACE MATERIALIZED VIEW replace_view" +
                " AS SELECT 3 * sum(value) AS v FROM replace_base_table");
        assertUpdate(session, "REFRESH MATERIALIZED VIEW replace_view", 1);
        assertQuery(session, "SELECT count(*) FROM replace_view", "VALUES 1");
        assertQuery(session, "SELECT * FROM replace_view", "VALUES 30");

        // The materialized view is dropped and tested for absence
        assertUpdate(session, "DROP MATERIALIZED VIEW replace_view");
        assertQueryFails(session, "SELECT count(*) FROM replace_view", "line 1:22: Table '" + catalog +".default.replace_view' does not exist");

        // Re-create the materialized view after dropping and test it again
        assertUpdate(session, "CREATE OR REPLACE MATERIALIZED VIEW replace_view" +
                " AS SELECT 4 * sum(value) AS v FROM replace_base_table");
        assertUpdate(session, "REFRESH MATERIALIZED VIEW replace_view", 1);
        assertQuery(session, "SELECT count(*) FROM replace_view", "VALUES 1");
        assertQuery(session, "SELECT * FROM replace_view", "VALUES 40");

        assertUpdate(session, "DROP MATERIALIZED VIEW replace_view");
    }
}
