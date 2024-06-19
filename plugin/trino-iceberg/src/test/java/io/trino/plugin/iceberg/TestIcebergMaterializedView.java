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
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.Table;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.testing.TestingNames.randomNameSuffix;
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
            metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                    .getInstance(HiveMetastoreFactory.class)
                    .createMetastore(Optional.empty());

            queryRunner.createCatalog("iceberg2", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg2-catalog").toString(),
                    "iceberg.hive-catalog-name", "hive"));

            secondIceberg = Session.builder(queryRunner.getDefaultSession())
                    .setCatalog("iceberg2")
                    .build();

            queryRunner.createCatalog("iceberg_legacy_mv", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toString(),
                    "iceberg.hive-catalog-name", "hive",
                    "iceberg.materialized-views.hide-storage-table", "false"));

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
    public void testIncrementalRefresh()
    {
        String sourceTableName = "source_table" + randomNameSuffix();
        String materializedViewName = "test_materialized_view_" + randomNameSuffix();

        Session defaultSession = getSession();
        Session incrementalRefreshDisabled = Session.builder(getSession())
                .setCatalogSessionProperty("iceberg", "incremental_refresh_enabled", "false")
                .build();

        String matViewDef = "SELECT a, b FROM %s WHERE a < 3 OR a > 5".formatted(sourceTableName);

        // create source table and two identical MVs
        assertUpdate("CREATE TABLE %s (a int, b varchar)".formatted(sourceTableName));
        assertUpdate("INSERT INTO %s VALUES (1, 'abc'), (2, 'def')".formatted(sourceTableName), 2);
        assertUpdate("CREATE MATERIALIZED VIEW %s_1 AS %s".formatted(materializedViewName, matViewDef));
        assertUpdate("CREATE MATERIALIZED VIEW %s_2 AS %s".formatted(materializedViewName, matViewDef));

        // execute first refresh: afterwards both MVs will contain: (1, 'abc'), (2, 'def')
        assertUpdate("REFRESH MATERIALIZED VIEW %s_1".formatted(materializedViewName), 2);
        assertUpdate("REFRESH MATERIALIZED VIEW %s_2".formatted(materializedViewName), 2);

        // add some new rows to source
        assertUpdate("INSERT INTO %s VALUES (3, 'ghi'), (4, 'jkl'), (5, 'mno'), (6, 'pqr')".formatted(sourceTableName), 4);

        // will do incremental refresh, and only add: (6, 'pqr')
        assertUpdate(defaultSession, "REFRESH MATERIALIZED VIEW %s_1".formatted(materializedViewName), 1);
        // will do full refresh, and (re)add: (1, 'abc'), (2, 'def'), (6, 'pqr')
        assertUpdate(incrementalRefreshDisabled, "REFRESH MATERIALIZED VIEW %s_2".formatted(materializedViewName), 3);

        // verify that view contents are the same
        assertThat(query("TABLE %s_1".formatted(materializedViewName))).matches("VALUES (1, VARCHAR 'abc'), (2, VARCHAR 'def'), (6, VARCHAR 'pqr')");
        assertThat(query("TABLE %s_2".formatted(materializedViewName))).matches("VALUES (1, VARCHAR 'abc'), (2, VARCHAR 'def'), (6, VARCHAR 'pqr')");

        // cleanup
        assertUpdate("DROP MATERIALIZED VIEW %s_1".formatted(materializedViewName));
        assertUpdate("DROP MATERIALIZED VIEW %s_2".formatted(materializedViewName));
        assertUpdate("DROP TABLE %s".formatted(sourceTableName));
    }

    @Test
    public void testFullRefreshForUnion()
    {
        String sourceTableName = "source_table" + randomNameSuffix();
        String materializedViewName = "test_materialized_view_" + randomNameSuffix();

        Session defaultSession = getSession();

        String matViewDef = """
                SELECT a, b FROM %s a WHERE a.a < 3 UNION ALL
                SELECT * FROM %s b WHERE b.a > 5""".formatted(sourceTableName, sourceTableName);

        // create source table and two identical MVs
        assertUpdate("CREATE TABLE %s (a int, b varchar)".formatted(sourceTableName));
        assertUpdate("INSERT INTO %s VALUES (1, 'abc'), (2, 'def')".formatted(sourceTableName), 2);
        assertUpdate("CREATE MATERIALIZED VIEW %s AS %s".formatted(materializedViewName, matViewDef));

        // execute first refresh: afterwards both MVs will contain: (1, 'abc'), (2, 'def')
        assertUpdate("REFRESH MATERIALIZED VIEW %s".formatted(materializedViewName), 2);

        // add some new rows to source
        assertUpdate("INSERT INTO %s VALUES (3, 'ghi'), (4, 'jkl'), (5, 'mno'), (6, 'pqr')".formatted(sourceTableName), 4);

        // will do a full refresh
        assertUpdate(defaultSession, "REFRESH MATERIALIZED VIEW %s".formatted(materializedViewName), 3);

        // verify that view contents are the same
        assertThat(query("TABLE %s".formatted(materializedViewName))).matches("VALUES (1, VARCHAR 'abc'), (2, VARCHAR 'def'), (6, VARCHAR 'pqr')");

        // cleanup
        assertUpdate("DROP MATERIALIZED VIEW %s".formatted(materializedViewName));
        assertUpdate("DROP TABLE %s".formatted(sourceTableName));
    }

    @Test
    public void testFullRefreshForUpdates()
    {
        String sourceTableName = "source_table" + randomNameSuffix();
        String materializedViewName = "test_materialized_view_" + randomNameSuffix();

        Session defaultSession = getSession();

        String matViewDef = "SELECT a, b FROM %s WHERE a < 3 OR a > 5".formatted(sourceTableName);

        // create source table and an MV
        assertUpdate("CREATE TABLE %s (a int, b varchar)".formatted(sourceTableName));
        assertUpdate("INSERT INTO %s VALUES (1, 'abc'), (2, 'def')".formatted(sourceTableName), 2);
        assertUpdate("CREATE MATERIALIZED VIEW %s AS %s".formatted(materializedViewName, matViewDef));

        // execute first refresh: afterwards both MVs will contain: (1, 'abc'), (2, 'def')
        assertUpdate("REFRESH MATERIALIZED VIEW %s".formatted(materializedViewName), 2);

        // add some new rows to source
        assertUpdate("INSERT INTO %s VALUES (3, 'ghi'), (4, 'jkl'), (5, 'mno'), (6, 'pqr')".formatted(sourceTableName), 4);

        // will do incremental refresh, and only add: (6, 'pqr')
        assertUpdate(defaultSession, "REFRESH MATERIALIZED VIEW %s".formatted(materializedViewName), 1);

        // update one row and append one
        assertUpdate("UPDATE %s SET b = 'updated' WHERE a = 1".formatted(sourceTableName), 1);
        assertUpdate("INSERT INTO %s VALUES (7, 'stv')".formatted(sourceTableName), 1);

        // will do full refresh due to the above update command
        assertUpdate(defaultSession, "REFRESH MATERIALIZED VIEW %s".formatted(materializedViewName), 4);
        // verify view contents
        assertThat(query("TABLE %s".formatted(materializedViewName))).matches("VALUES (1, VARCHAR 'updated'), (2, VARCHAR 'def'), (6, VARCHAR 'pqr'), (7, VARCHAR 'stv')");

        // add some new row to source
        assertUpdate("INSERT INTO %s VALUES (8, 'wxy')".formatted(sourceTableName), 1);
        // will do incremental refresh now since refresh window now does not contain the delete anymore, and only add: (8, 'wxy')
        assertUpdate(defaultSession, "REFRESH MATERIALIZED VIEW %s".formatted(materializedViewName), 1);
        // verify view contents
        assertThat(query("TABLE %s".formatted(materializedViewName))).matches("VALUES (1, VARCHAR 'updated'), (2, VARCHAR 'def'), (6, VARCHAR 'pqr'), (7, VARCHAR 'stv'), (8, VARCHAR 'wxy')");

        // cleanup
        assertUpdate("DROP MATERIALIZED VIEW %s".formatted(materializedViewName));
        assertUpdate("DROP TABLE %s".formatted(sourceTableName));
    }

    @Test
    public void testRefreshWithCompaction()
    {
        String sourceTableName = "source_table" + randomNameSuffix();
        String materializedViewName = "test_materialized_view_" + randomNameSuffix();

        Session defaultSession = getSession();

        String matViewDef = "SELECT a, b FROM %s WHERE a < 3 OR a > 5".formatted(sourceTableName);

        // create source table and an MV
        assertUpdate("CREATE TABLE %s (a int, b varchar)".formatted(sourceTableName));
        assertUpdate("INSERT INTO %s VALUES (1, 'abc'), (2, 'def')".formatted(sourceTableName), 2);
        assertUpdate("CREATE MATERIALIZED VIEW %s AS %s".formatted(materializedViewName, matViewDef));

        // execute first refresh: afterwards both MVs will contain: (1, 'abc'), (2, 'def')
        assertUpdate("REFRESH MATERIALIZED VIEW %s".formatted(materializedViewName), 2);

        // add some new rows to source
        assertUpdate("INSERT INTO %s VALUES (3, 'ghi'), (4, 'jkl'), (5, 'mno'), (6, 'pqr')".formatted(sourceTableName), 4);

        // will do incremental refresh, and only add: (6, 'pqr')
        assertUpdate(defaultSession, "REFRESH MATERIALIZED VIEW %s".formatted(materializedViewName), 1);
        // verify view contents
        assertThat(query("TABLE %s".formatted(materializedViewName))).matches("VALUES (1, VARCHAR 'abc'), (2, VARCHAR 'def'), (6, VARCHAR 'pqr')");

        // run compaction - after that, refresh will update 0 rows
        assertUpdate(defaultSession, "ALTER TABLE %s EXECUTE OPTIMIZE".formatted(sourceTableName));
        assertUpdate(defaultSession, "REFRESH MATERIALIZED VIEW %s".formatted(materializedViewName), 0);
        // verify view contents
        assertThat(query("TABLE %s".formatted(materializedViewName))).matches("VALUES (1, VARCHAR 'abc'), (2, VARCHAR 'def'), (6, VARCHAR 'pqr')");

        // add some new rows to source
        assertUpdate("INSERT INTO %s VALUES (7, 'stv'), (8, 'wxy')".formatted(sourceTableName), 2);
        // will do incremental refresh, and only add: (7, 'stv'), (8, 'wxy')
        assertUpdate(defaultSession, "REFRESH MATERIALIZED VIEW %s".formatted(materializedViewName), 2);
        // verify view contents
        assertThat(query("TABLE %s".formatted(materializedViewName))).matches("VALUES (1, VARCHAR 'abc'), (2, VARCHAR 'def'), (6, VARCHAR 'pqr'), (7, VARCHAR 'stv'), (8, VARCHAR 'wxy')");

        // cleanup
        assertUpdate("DROP MATERIALIZED VIEW %s".formatted(materializedViewName));
        assertUpdate("DROP TABLE %s".formatted(sourceTableName));
    }

    @Test
    public void testTwoIcebergCatalogs()
    {
        Session defaultIceberg = getSession();

        // Base table for staleness check
        String createTable = "CREATE TABLE common_base_table AS SELECT 10 value";
        assertUpdate(secondIceberg, createTable, 1); // this one will be used by MV
        assertUpdate(defaultIceberg, createTable, 1); // this one exists so that it can be mistakenly treated as the base table

        assertUpdate(defaultIceberg, """
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
}
