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

import io.airlift.slice.Slice;
import io.trino.Session;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getConnectorService;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.spi.RefreshType.INCREMENTAL;
import static io.trino.spi.connector.RetryMode.NO_RETRIES;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // Uses file metastore sharing location between catalogs
public class TestIcebergMaterializedView
        extends BaseIcebergMaterializedViewTest
{
    private Session secondIceberg;
    private HiveMetastore metastore;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session icebergSession = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();
        QueryRunner queryRunner = DistributedQueryRunner.builder(icebergSession).build();
        try {
            Path baseDataDir = queryRunner.getCoordinator().getBaseDataDir();
            queryRunner.installPlugin(new TestingIcebergPlugin(baseDataDir));
            queryRunner.createCatalog("iceberg", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    // Intentionally sharing the file metastore directory with Hive
                    "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                    "iceberg.hive-catalog-name", "hive"));

            metastore = getHiveMetastore(queryRunner);

            queryRunner.createCatalog("iceberg2", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    "hive.metastore.catalog.dir", "local:///iceberg2-catalog",
                    "iceberg.hive-catalog-name", "hive"));

            secondIceberg = Session.builder(queryRunner.getDefaultSession())
                    .setCatalog("iceberg2")
                    .build();

            queryRunner.createCatalog("iceberg_legacy_mv", "iceberg", Map.of(
                    "iceberg.catalog.type", "TESTING_FILE_METASTORE",
                    // Intentionally sharing the file metastore directory with Iceberg
                    "hive.metastore.catalog.dir", "local:///iceberg-catalog",
                    "iceberg.hive-catalog-name", "hive",
                    "iceberg.materialized-views.hide-storage-table", "false"));

            queryRunner.execute(secondIceberg, "CREATE SCHEMA " + secondIceberg.getSchema().orElseThrow());

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            queryRunner.installPlugin(createMockConnectorPlugin());
            queryRunner.createCatalog("mock", "mock");

            queryRunner.execute("CREATE SCHEMA tpch");
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
        return "local:///iceberg-catalog/tpch";
    }

    @Override
    protected String getStorageMetadataLocation(String materializedViewName)
    {
        Table table = metastore.getTable("tpch", materializedViewName).orElseThrow();
        return table.getParameters().get(METADATA_LOCATION_PROP);
    }

    @ParameterizedTest
    @CsvSource({"true, false", "true, true", "false, false", "false, true"})
    public void testConcurrentIncrementalRefresh(boolean hiddenStorage, boolean emptyFullRefresh)
    {
        String sourceTableName = "source_table_" + randomNameSuffix();
        String materializedViewName = "materialized_view_" + randomNameSuffix();
        String catalogName = hiddenStorage ? "iceberg" : "iceberg_legacy_mv";
        String qualifiedMaterializedViewName = catalogName + ".tpch." + materializedViewName;
        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 1 AS value", 1);
        assertUpdate("CREATE MATERIALIZED VIEW " + qualifiedMaterializedViewName + " AS SELECT value FROM " + catalogName + ".tpch." + sourceTableName);
        try {
            assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMaterializedViewName, 1);
            assertUpdate("INSERT INTO " + sourceTableName + " VALUES 2", 1);

            IcebergMetadata metadata = getConnectorService(getQueryRunner(), IcebergMetadataFactory.class).create(SESSION.getIdentity());
            SchemaTableName storageTableName = metadata.getMaterializedView(SESSION, new SchemaTableName("tpch", materializedViewName))
                    .orElseThrow().getStorageTable().orElseThrow().getSchemaTableName();
            ConnectorTableHandle storageTable = metadata.getTableHandle(SESSION, storageTableName, Optional.empty(), Optional.empty());
            List<ConnectorTableHandle> sourceTables = List.of(metadata.getTableHandle(
                    SESSION, new SchemaTableName("tpch", sourceTableName), Optional.empty(), Optional.empty()));
            IcebergWritableTableHandle insertHandle = (IcebergWritableTableHandle) metadata.beginRefreshMaterializedView(
                    SESSION, storageTable, sourceTables, false, NO_RETRIES, INCREMENTAL);
            assertThat(metadata.getIncrementalRefreshFromSnapshot()).isPresent();

            // Prepare one refresh's output before another refresh commits the same source rows.
            ConnectorPageSink pageSink = getConnectorService(getQueryRunner(), ConnectorPageSinkProvider.class).createPageSink(
                    new HiveTransactionHandle(true), SESSION, (ConnectorInsertTableHandle) insertHandle, metadata.getTableCredentials(SESSION, insertHandle), () -> 0);
            try {
                pageSink.appendPage(new Page(createIntsBlock(2))).join();
                Collection<Slice> fragments = pageSink.finish().join();

                if (emptyFullRefresh) {
                    // A concurrent full refresh can also invalidate the incremental input without adding any data files.
                    assertUpdate("DELETE FROM " + sourceTableName, 2);
                }
                assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMaterializedViewName, emptyFullRefresh ? 0 : 1);

                assertTrinoExceptionThrownBy(() -> metadata.finishRefreshMaterializedView(
                        SESSION, storageTable, insertHandle, fragments, List.of(), sourceTables, false, false, false))
                        .hasErrorCode(ICEBERG_COMMIT_ERROR)
                        .hasMessageContaining("Materialized view storage table changed during incremental refresh");

                assertQuery("TABLE " + qualifiedMaterializedViewName, emptyFullRefresh ? "SELECT 1 WHERE false" : "VALUES 1, 2");
                assertUpdate("REFRESH MATERIALIZED VIEW " + qualifiedMaterializedViewName, 0);
            }
            finally {
                pageSink.abort();
                metadata.rollback();
            }
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW " + qualifiedMaterializedViewName);
            assertUpdate("DROP TABLE " + sourceTableName);
        }
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
}
