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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.testing.TestingHttpServer;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.BaseIcebergMaterializedViewTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.sql.tree.ExplainType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.DelegatingRestSessionCatalog;
import org.apache.iceberg.view.RefreshState;
import org.apache.iceberg.view.RefreshStateParser;
import org.apache.iceberg.view.SourceState;
import org.apache.iceberg.view.SourceTableState;
import org.apache.iceberg.view.SourceViewState;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.iceberg.IcebergTestUtils.FILE_IO_FACTORY;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.catalog.rest.RestCatalogTestUtils.backendCatalog;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergTrinoRestCatalogMaterializedView
        extends BaseIcebergMaterializedViewTest
{
    private final String schemaName = "test_iceberg_materialized_view_" + randomNameSuffix();

    private Path warehouseLocation;
    private JdbcCatalog backend;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = Files.createTempDirectory(null);
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));

        backend = closeAfterClass((JdbcCatalog) backendCatalog(warehouseLocation));

        DelegatingRestSessionCatalog delegatingCatalog = DelegatingRestSessionCatalog.builder()
                .delegate(backend)
                .build();

        TestingHttpServer testServer = delegatingCatalog.testServer();
        testServer.start();
        closeAfterClass(testServer::stop);

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation))
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .buildOrThrow())
                .addIcebergProperty("fs.hadoop.enabled", "true")
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(ImmutableList.of())
                                .withSchemaName(schemaName)
                                .build())
                .build();
        try {
            queryRunner.createCatalog("iceberg_legacy_mv", "iceberg", ImmutableMap.<String, String>builder()
                    .put("iceberg.catalog.type", "rest")
                    .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                    .put("fs.hadoop.enabled", "true")
                    .buildOrThrow());

            queryRunner.installPlugin(createMockConnectorPlugin());
            queryRunner.createCatalog("mock", "mock");
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Override
    protected String getSchemaDirectory()
    {
        return warehouseLocation.resolve("iceberg_data").resolve(schemaName).toString();
    }

    @Override
    protected String getStorageMetadataLocation(String name)
    {
        Namespace namespace = Namespace.of(schemaName);
        try {
            View view = backend.loadView(TableIdentifier.of(namespace, name));
            return ((BaseTable) backend.loadTable(view.currentVersion().storageTable())).operations().current().metadataFileLocation();
        }
        catch (NoSuchViewException e) {
            return ((BaseTable) backend.loadTable(TableIdentifier.of(namespace, name))).operations().current().metadataFileLocation();
        }
    }

    private TableMetadata getStorageTableMetadata(String name)
    {
        TrinoFileSystem fileSystem = getFileSystemFactory(getQueryRunner()).create(ConnectorIdentity.ofUser("test"));
        return TableMetadataParser.read(FILE_IO_FACTORY.create(fileSystem), Location.of(getStorageMetadataLocation(name)).toString());
    }

    @Test
    @Override
    public void testShowCreate()
    {
        String schema = getSession().getSchema().orElseThrow();

        assertUpdate("CREATE MATERIALIZED VIEW test_mv_show_create " +
                "WITH (\n" +
                "   partitioning = ARRAY['_date'],\n" +
                "   format = 'ORC',\n" +
                "   orc_bloom_filter_columns = ARRAY['_date'],\n" +
                "   orc_bloom_filter_fpp = 0.1) AS " +
                "SELECT _bigint, _date FROM base_table1");
        assertQuery("SELECT COUNT(*) FROM test_mv_show_create", "VALUES 6");

        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW test_mv_show_create"))
                .matches(
                        "\\QCREATE MATERIALIZED VIEW iceberg." + schema + ".test_mv_show_create\n" +
                                "WHEN STALE INLINE\n" +
                                "WITH (\n" +
                                "   format = 'ORC',\n" +
                                "   format_version = 2,\n" +
                                "   location = '" + getSchemaDirectory() + "/st_\\E[0-9a-f]+-[0-9a-f]+\\Q',\n" +
                                "   orc_bloom_filter_columns = ARRAY['_date'],\n" +
                                "   orc_bloom_filter_fpp = 1E-1,\n" +
                                "   partitioning = ARRAY['_date'],\n" +
                                "   storage_schema = '" + schema + "'\n" +
                                ") AS\n" +
                                "SELECT\n" +
                                "  _bigint\n" +
                                ", _date\n" +
                                "FROM\n" +
                                "  base_table1");
        assertUpdate("DROP MATERIALIZED VIEW test_mv_show_create");
    }

    @Test
    @Override
    public void testSqlFeatures()
    {
        String schema = getSession().getSchema().orElseThrow();

        // Materialized views to test SQL features
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_window WITH (partitioning = ARRAY['_date']) AS SELECT _date, " +
                "sum(_bigint) OVER (PARTITION BY _date ORDER BY _date) as sum_ints from base_table1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_window", 6);
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_union WITH (partitioning = ARRAY['_date']) AS " +
                "select _date, count(_date) as num_dates from base_table1 group by 1 UNION " +
                "select _date, count(_date) as num_dates from base_table2 group by 1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_union", 5);
        assertUpdate("CREATE MATERIALIZED VIEW materialized_view_subquery WITH (partitioning = ARRAY['_date']) AS " +
                "SELECT _date, count(_date) AS num_dates FROM base_table1 WHERE _date = (select max(_date) FROM base_table2) GROUP BY 1");
        assertUpdate("REFRESH MATERIALIZED VIEW materialized_view_subquery", 1);

        // This set of tests intend to test various SQL features in the context of materialized views. It also tests commands pertaining to materialized views.
        assertThat(getExplainPlan("SELECT * FROM materialized_view_window", ExplainType.Type.IO))
                .doesNotContain("base_table1");
        assertThat(getExplainPlan("SELECT * FROM materialized_view_union", ExplainType.Type.IO))
                .doesNotContain("base_table1");
        assertThat(getExplainPlan("SELECT * FROM materialized_view_subquery", ExplainType.Type.IO))
                .doesNotContain("base_table1");

        String qualifiedMaterializedViewName = "iceberg." + schema + ".materialized_view_window";
        assertQueryFails(
                "SHOW CREATE VIEW materialized_view_window",
                "line 1:1: Relation '" + qualifiedMaterializedViewName + "' is a materialized view, not a view");

        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW materialized_view_window"))
                .matches("\\QCREATE MATERIALIZED VIEW " + qualifiedMaterializedViewName + "\n" +
                        "WHEN STALE INLINE\n" +
                        "WITH (\n" +
                        "   format = 'PARQUET',\n" +
                        "   format_version = 2,\n" +
                        "   location = '" + getSchemaDirectory() + "/st_\\E[0-9a-f]+-[0-9a-f]+\\Q',\n" +
                        "   partitioning = ARRAY['_date'],\n" +
                        "   storage_schema = '" + schema + "'\n" +
                        ") AS\n" +
                        "SELECT\n" +
                        "  _date\n" +
                        ", sum(_bigint) OVER (PARTITION BY _date ORDER BY _date ASC) sum_ints\n" +
                        "FROM\n" +
                        "  base_table1");

        assertQueryFails(
                "INSERT INTO materialized_view_window VALUES (0, '2019-09-08'), (1, DATE '2019-09-09'), (2, DATE '2019-09-09')",
                "line 1:1: Inserting into materialized views is not supported");

        computeScalar("EXPLAIN (TYPE LOGICAL) REFRESH MATERIALIZED VIEW materialized_view_window");
        computeScalar("EXPLAIN (TYPE DISTRIBUTED) REFRESH MATERIALIZED VIEW materialized_view_window");
        computeScalar("EXPLAIN (TYPE VALIDATE) REFRESH MATERIALIZED VIEW materialized_view_window");
        computeScalar("EXPLAIN (TYPE IO) REFRESH MATERIALIZED VIEW materialized_view_window");
        computeScalar("EXPLAIN ANALYZE REFRESH MATERIALIZED VIEW materialized_view_window");

        assertUpdate("DROP MATERIALIZED VIEW materialized_view_window");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_union");
        assertUpdate("DROP MATERIALIZED VIEW materialized_view_subquery");
    }

    @Test
    @Override
    public void testReplaceWithLegacyMetastoreStorage()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        String materializedViewName = "test_materialized_view_replace_legacy" + randomNameSuffix();
        // Materialized view to test 'replace' feature
        assertUpdate(format("CREATE MATERIALIZED VIEW iceberg_legacy_mv.%1$s.%2$s WITH (partitioning = ARRAY['_date']) AS SELECT _date, count(_date) AS num_dates FROM iceberg.%1$s.base_table1 GROUP BY 1", schemaName, materializedViewName));
        String storageTableName = (String) computeScalar("SELECT storage_table FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + materializedViewName + "'");

        assertUpdate(format("REFRESH MATERIALIZED VIEW iceberg_legacy_mv.%s.%s", schemaName, materializedViewName), 3);
        TableMetadata storageMetadata = getStorageTableMetadata(storageTableName);
        String storageTableUuid = storageMetadata.uuid();
        List<Snapshot> storageSnapshots = storageMetadata.snapshots();
        assertThat(storageSnapshots).hasSize(2);

        assertUpdate(format("CREATE OR REPLACE MATERIALIZED VIEW iceberg_legacy_mv.%1$s.%2$s WITH (format='AVRO') AS SELECT sum(1) AS num_rows FROM iceberg.%1$s.base_table2", schemaName, materializedViewName));
        storageMetadata = getStorageTableMetadata(storageTableName);
        assertThat(storageMetadata.uuid()).isEqualTo(storageTableUuid);
        assertThat(storageMetadata.properties()).contains(entry(TableProperties.DEFAULT_FILE_FORMAT, "AVRO"));
        assertThat(storageMetadata.snapshots()).hasSize(3).containsAll(storageSnapshots);
        storageSnapshots = storageMetadata.snapshots();

        assertThat(getExplainPlan(format("SELECT * FROM iceberg_legacy_mv.%s.%s", schemaName, materializedViewName), ExplainType.Type.IO))
                .contains("base_table2");

        assertUpdate(format("REFRESH MATERIALIZED VIEW iceberg_legacy_mv.%s.%s", schemaName, materializedViewName), 1);
        storageMetadata = getStorageTableMetadata(storageTableName);
        // REFRESH performs "delete" and new data "append"
        assertThat(storageMetadata.snapshots()).hasSize(5).containsAll(storageSnapshots);

        computeScalar(format("SELECT * FROM iceberg_legacy_mv.%s.%s", schemaName, materializedViewName));
        assertThat(query(format("SELECT * FROM iceberg_legacy_mv.%s.%s", schemaName, materializedViewName)))
                .matches("VALUES BIGINT '3'");

        // CREATE OR REPLACE without specifying location
        assertUpdate(format("CREATE OR REPLACE MATERIALIZED VIEW iceberg_legacy_mv.%s.%s WITH (format='PARQUET') AS SELECT * FROM base_table1", schemaName, materializedViewName));
        assertThat(getExplainPlan(format("SELECT * FROM iceberg_legacy_mv.%s.%s", schemaName, materializedViewName), ExplainType.Type.IO))
                .contains("base_table1");
        storageMetadata = getStorageTableMetadata(storageTableName);
        assertThat(storageMetadata.uuid()).isEqualTo(storageTableUuid);
        assertThat(storageMetadata.properties()).contains(entry(TableProperties.DEFAULT_FILE_FORMAT, "PARQUET"));
        assertThat(storageMetadata.snapshots()).hasSize(6).containsAll(storageSnapshots);

        assertUpdate(format("DROP MATERIALIZED VIEW iceberg_legacy_mv.%s.%s", schemaName, materializedViewName));
    }

    // Overridden for the same snapshot-count reason as testReplaceWithLegacyMetastoreStorage above.
    @Test
    @Override
    public void testReplaceWithLegacyMetastoreStorageWithLocation()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        String materializedViewName = "test_materialized_view_replace_legacy" + randomNameSuffix();
        // Avoid location conflicts with the file metastore
        String storageTableLocation = getSchemaDirectory() + "/" + materializedViewName + "location";

        // Materialized view to test 'replace' feature
        assertUpdate(format("CREATE MATERIALIZED VIEW iceberg_legacy_mv.%1$s.%2$s WITH (partitioning = ARRAY['_date'], location='%3$s') AS SELECT _date, count(_date) AS num_dates FROM iceberg.%1$s.base_table1 GROUP BY 1", schemaName, materializedViewName, storageTableLocation));
        String storageTableName = (String) computeScalar("SELECT storage_table FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + materializedViewName + "'");

        assertUpdate(format("REFRESH MATERIALIZED VIEW iceberg_legacy_mv.%s.%s", schemaName, materializedViewName), 3);
        TableMetadata storageMetadata = getStorageTableMetadata(storageTableName);
        String storageTableUuid = storageMetadata.uuid();
        List<Snapshot> storageSnapshots = storageMetadata.snapshots();
        assertThat(storageSnapshots).hasSize(2);

        assertUpdate(format("CREATE OR REPLACE MATERIALIZED VIEW iceberg_legacy_mv.%1$s.%2$s WITH (location='%3$s', format='AVRO') AS SELECT sum(1) AS num_rows FROM iceberg.%1$s.base_table2", schemaName, materializedViewName, storageTableLocation));
        storageMetadata = getStorageTableMetadata(storageTableName);
        assertThat(storageMetadata.uuid()).isEqualTo(storageTableUuid);
        assertThat(storageMetadata.properties()).contains(entry(TableProperties.DEFAULT_FILE_FORMAT, "AVRO"));
        assertThat(storageMetadata.snapshots()).hasSize(3).containsAll(storageSnapshots);
        storageSnapshots = storageMetadata.snapshots();

        assertThat(getExplainPlan(format("SELECT * FROM iceberg_legacy_mv.%s.%s", schemaName, materializedViewName), ExplainType.Type.IO))
                .contains("base_table2");

        assertUpdate(format("REFRESH MATERIALIZED VIEW iceberg_legacy_mv.%s.%s", schemaName, materializedViewName), 1);
        storageMetadata = getStorageTableMetadata(storageTableName);
        // REFRESH performs "delete" and new data "append"
        assertThat(storageMetadata.snapshots()).hasSize(5).containsAll(storageSnapshots);

        computeScalar(format("SELECT * FROM iceberg_legacy_mv.%s.%s", schemaName, materializedViewName));
        assertThat(query(format("SELECT * FROM iceberg_legacy_mv.%s.%s", schemaName, materializedViewName)))
                .matches("VALUES BIGINT '3'");

        // CREATE OR REPLACE without specifying location
        assertUpdate(format("CREATE OR REPLACE MATERIALIZED VIEW iceberg_legacy_mv.%s.%s WITH (format='PARQUET') AS SELECT * FROM base_table1", schemaName, materializedViewName));
        assertThat(getExplainPlan(format("SELECT * FROM iceberg_legacy_mv.%s.%s", schemaName, materializedViewName), ExplainType.Type.IO))
                .contains("base_table1");
        storageMetadata = getStorageTableMetadata(storageTableName);
        assertThat(storageMetadata.uuid()).isEqualTo(storageTableUuid);
        assertThat(storageMetadata.properties()).contains(entry(TableProperties.DEFAULT_FILE_FORMAT, "PARQUET"));
        assertThat(storageMetadata.snapshots()).hasSize(6).containsAll(storageSnapshots);

        String newStorageTableLocation = getSchemaDirectory() + "/" + materializedViewName + "newlocation";
        assertThatThrownBy(() -> computeActual(format("CREATE OR REPLACE MATERIALIZED VIEW iceberg_legacy_mv.%s.%s WITH (location='%s') AS SELECT 1 AS data", schemaName, materializedViewName, newStorageTableLocation)))
                .hasMessage("The provided location '%s' does not match the existing storage table location '%s'".formatted(newStorageTableLocation, storageTableLocation));

        assertUpdate(format("DROP MATERIALIZED VIEW iceberg_legacy_mv.%s.%s", schemaName, materializedViewName));
    }

    @Test
    public void testMaterializedViewOnBranch()
    {
        String schema = getSession().getSchema().orElseThrow();
        String baseTableName = "test_mv_branch_base_" + randomNameSuffix();
        String materializedViewName = "test_mv_branch_" + randomNameSuffix();
        String branchName = "test_branch";
        String freshnessQuery = format(
                "SELECT freshness FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '%s'",
                materializedViewName);

        assertUpdate("CREATE TABLE " + baseTableName + " AS SELECT 1 AS a", 1);

        BaseTable icebergTable = (BaseTable) backend.loadTable(TableIdentifier.of(Namespace.of(schema), baseTableName));
        icebergTable.manageSnapshots()
                .createBranch(branchName, icebergTable.currentSnapshot().snapshotId())
                .commit();

        // Advance the table's main branch so it diverges from the branch pinned by the materialized view
        assertUpdate("INSERT INTO " + baseTableName + " VALUES 2", 1);
        assertQuery("SELECT * FROM " + baseTableName, "VALUES 1, 2");
        assertQuery("SELECT * FROM " + baseTableName + " FOR VERSION AS OF '" + branchName + "'", "VALUES 1");

        assertUpdate("CREATE MATERIALIZED VIEW " + materializedViewName + " AS SELECT * FROM " + baseTableName + " FOR VERSION AS OF '" + branchName + "'");
        assertQuery(freshnessQuery, "VALUES 'STALE'");

        assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 1);
        assertQuery("SELECT * FROM " + materializedViewName, "VALUES 1");
        assertQuery(freshnessQuery, "VALUES 'FRESH'");

        // The recorded refresh state must track the pinned branch, not just the snapshot id.
        TableMetadata storageMetadata = getStorageTableMetadata(materializedViewName);
        RefreshState refreshState = RefreshStateParser.fromJson(storageMetadata.currentSnapshot().summary().get(RefreshState.REFRESH_STATE_SUMMARY_KEY));
        assertThat(refreshState.sourceStates()).hasSize(1);
        assertThat(refreshState.sourceStates().get(0)).isInstanceOfSatisfying(SourceTableState.class, sourceTableState -> {
            assertThat(sourceTableState.name()).isEqualTo(baseTableName);
            assertThat(sourceTableState.ref()).isEqualTo(branchName);
        });

        // The pinned branch has not moved: a second refresh is incremental and adds nothing
        assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 0);

        // Moving the branch forward makes the view stale; the next refresh should pick up only the new row.
        icebergTable.refresh();
        icebergTable.manageSnapshots()
                .replaceBranch(branchName, icebergTable.currentSnapshot().snapshotId())
                .commit();
        assertQuery(freshnessQuery, "VALUES 'STALE'");

        assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 1);
        assertQuery("SELECT * FROM " + materializedViewName, "VALUES 1, 2");
        assertQuery(freshnessQuery, "VALUES 'FRESH'");

        assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
        assertUpdate("DROP TABLE " + baseTableName);
    }

    @Test
    public void testMaterializedViewOnNestedView()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String nestedViewName = "test_mv_nested_view_" + randomNameSuffix();
        String materializedViewName = "test_mv_with_nested_view_" + randomNameSuffix();
        String freshnessQuery = format(
                "SELECT freshness FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '%s'",
                materializedViewName);

        assertUpdate("CREATE VIEW " + nestedViewName + " AS SELECT _bigint, _date FROM base_table1");
        assertUpdate("CREATE MATERIALIZED VIEW " + materializedViewName + " AS SELECT * FROM " + nestedViewName);
        assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 6);
        assertQuery("SELECT count(*) FROM " + materializedViewName, "VALUES 6");
        assertQuery(freshnessQuery, "VALUES 'FRESH'");

        View nestedView = backend.loadView(TableIdentifier.of(Namespace.of(schema), nestedViewName));

        TableMetadata storageMetadata = getStorageTableMetadata(materializedViewName);
        Map<String, String> summary = storageMetadata.currentSnapshot().summary();
        RefreshState refreshState = RefreshStateParser.fromJson(summary.get(RefreshState.REFRESH_STATE_SUMMARY_KEY));

        // one source table (base_table1, resolved through the nested view) and one source view (the nested view itself)
        List<SourceState> sourceStates = refreshState.sourceStates();
        assertThat(sourceStates).hasSize(2);
        assertThat(sourceStates.stream().filter(SourceTableState.class::isInstance).map(SourceTableState.class::cast).toList())
                .singleElement()
                .satisfies(sourceTableState -> {
                    assertThat(sourceTableState.name()).isEqualTo("base_table1");
                    assertThat(sourceTableState.namespace()).containsExactly(schema);
                    assertThat(sourceTableState.ref()).isNull();
                });
        assertThat(sourceStates.stream().filter(SourceViewState.class::isInstance).map(SourceViewState.class::cast).toList())
                .singleElement()
                .satisfies(sourceViewState -> {
                    assertThat(sourceViewState.name()).isEqualTo(nestedViewName);
                    assertThat(sourceViewState.namespace()).containsExactly(schema);
                    assertThat(sourceViewState.catalog()).isEqualTo(catalog);
                    assertThat(sourceViewState.uuid()).isEqualTo(nestedView.uuid().toString());
                    assertThat(sourceViewState.versionId()).isEqualTo(nestedView.currentVersion().versionId());
                });

        // successfully resolved, so it must not also be flagged as unverifiable
        assertThat(summary).doesNotContainKey("trino.materialized-view.freshness-unknown");

        assertUpdate("CREATE OR REPLACE VIEW " + nestedViewName + " AS SELECT _bigint, _date FROM base_table1 WHERE _bigint > 0");
        assertQuery(freshnessQuery, "VALUES 'STALE'");

        assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 5);
        assertQuery("SELECT count(*) FROM " + materializedViewName, "VALUES 5");
        assertQuery(freshnessQuery, "VALUES 'FRESH'");

        View replacedNestedView = backend.loadView(TableIdentifier.of(Namespace.of(schema), nestedViewName));
        assertThat(replacedNestedView.uuid()).isEqualTo(nestedView.uuid());
        assertThat(replacedNestedView.currentVersion().versionId()).isNotEqualTo(nestedView.currentVersion().versionId());

        RefreshState updatedRefreshState = RefreshStateParser.fromJson(getStorageTableMetadata(materializedViewName).currentSnapshot().summary().get(RefreshState.REFRESH_STATE_SUMMARY_KEY));
        assertThat(updatedRefreshState.sourceStates().stream().filter(SourceViewState.class::isInstance).map(SourceViewState.class::cast).toList())
                .singleElement()
                .satisfies(sourceViewState -> assertThat(sourceViewState.versionId()).isEqualTo(replacedNestedView.currentVersion().versionId()));

        assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
        assertUpdate("DROP VIEW " + nestedViewName);
    }

    @Test
    public void testMaterializedViewOnNestedMaterializedViewFromIcebergTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        String temporaryTableName = "test_temporary_base_" + randomNameSuffix();
        String innerMaterializedViewName = "test_inner_mv_" + randomNameSuffix();
        String outerMaterializedViewName = "test_outer_mv_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + temporaryTableName + " AS SELECT 1 AS a", 1);
        assertUpdate("CREATE MATERIALIZED VIEW " + innerMaterializedViewName + " AS SELECT * FROM " + temporaryTableName);
        assertUpdate("REFRESH MATERIALIZED VIEW " + innerMaterializedViewName, 1);

        assertUpdate("CREATE MATERIALIZED VIEW " + outerMaterializedViewName + " AS SELECT * FROM " + innerMaterializedViewName);
        assertUpdate("REFRESH MATERIALIZED VIEW " + outerMaterializedViewName, 1);
        assertQuery("SELECT * FROM " + outerMaterializedViewName, "VALUES 1");

        String innerStorageTableName = (String) computeScalar("SELECT storage_table FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + innerMaterializedViewName + "'");

        // Once the inner materialized view is fresh, the outer one depends only on its storage table - a
        // real, independently-persisted Iceberg table - never on the table that originally fed it
        assertUpdate("DROP TABLE " + temporaryTableName);

        RefreshState refreshState = RefreshStateParser.fromJson(getStorageTableMetadata(outerMaterializedViewName).currentSnapshot().summary().get(RefreshState.REFRESH_STATE_SUMMARY_KEY));
        assertThat(refreshState.sourceStates().stream().filter(SourceTableState.class::isInstance).map(SourceTableState.class::cast).toList())
                .singleElement()
                .satisfies(sourceTableState -> {
                    assertThat(sourceTableState.name()).isEqualTo(innerStorageTableName);
                    assertThat(sourceTableState.namespace()).containsExactly(schema);
                });

        Table innerStorageTable = backend.loadTable(TableIdentifier.of(Namespace.of(schema), innerStorageTableName));
        assertThat(innerStorageTable.currentSnapshot()).isNotNull();

        assertQuery("SELECT * FROM " + outerMaterializedViewName, "VALUES 1");
        assertUpdate("REFRESH MATERIALIZED VIEW " + outerMaterializedViewName, 0);

        assertUpdate("DROP MATERIALIZED VIEW " + outerMaterializedViewName);
        assertUpdate("DROP MATERIALIZED VIEW " + innerMaterializedViewName);
    }

    @Test
    public void testMaterializedViewOnNestedMaterializedViewFromValue()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String baseTableName = "test_nested_mv_base_" + randomNameSuffix();
        String innerMaterializedViewName = "test_inner_mv_" + randomNameSuffix();
        String outerMaterializedViewName = "test_outer_mv_" + randomNameSuffix();
        String innerFreshnessQuery = format(
                "SELECT freshness FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '%s'",
                innerMaterializedViewName);
        String outerFreshnessQuery = format(
                "SELECT freshness FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '%s'",
                outerMaterializedViewName);

        assertUpdate("CREATE TABLE " + baseTableName + " AS SELECT 1 AS a", 1);
        assertUpdate("CREATE MATERIALIZED VIEW " + innerMaterializedViewName + " AS SELECT * FROM " + baseTableName);
        assertUpdate("CREATE MATERIALIZED VIEW " + outerMaterializedViewName + " AS SELECT * FROM " + innerMaterializedViewName);

        // The inner MV is never refreshed, so it stays stale and gets inlined (see testMaterializedViewOnFreshNestedMaterializedView for the fresh case).
        assertUpdate("REFRESH MATERIALIZED VIEW " + outerMaterializedViewName, 1);
        assertQuery("SELECT * FROM " + outerMaterializedViewName, "VALUES 1");
        assertQuery(outerFreshnessQuery, "VALUES 'FRESH'");

        View innerView = backend.loadView(TableIdentifier.of(Namespace.of(schema), innerMaterializedViewName));
        RefreshState refreshState = RefreshStateParser.fromJson(getStorageTableMetadata(outerMaterializedViewName).currentSnapshot().summary().get(RefreshState.REFRESH_STATE_SUMMARY_KEY));
        List<SourceState> sourceStates = refreshState.sourceStates();
        assertThat(sourceStates).hasSize(2);
        assertThat(sourceStates.stream().filter(SourceTableState.class::isInstance).map(SourceTableState.class::cast).toList())
                .singleElement()
                .satisfies(sourceTableState -> {
                    assertThat(sourceTableState.name()).isEqualTo(baseTableName);
                    assertThat(sourceTableState.namespace()).containsExactly(schema);
                });
        assertThat(sourceStates.stream().filter(SourceViewState.class::isInstance).map(SourceViewState.class::cast).toList())
                .singleElement()
                .satisfies(sourceViewState -> {
                    assertThat(sourceViewState.name()).isEqualTo(innerMaterializedViewName);
                    assertThat(sourceViewState.namespace()).containsExactly(schema);
                    assertThat(sourceViewState.catalog()).isEqualTo(catalog);
                    assertThat(sourceViewState.uuid()).isEqualTo(innerView.uuid().toString());
                    assertThat(sourceViewState.versionId()).isEqualTo(innerView.currentVersion().versionId());
                });

        assertUpdate("INSERT INTO " + baseTableName + " VALUES 2", 1);

        assertQuery(innerFreshnessQuery, "VALUES 'STALE'");
        assertQuery(outerFreshnessQuery, "VALUES 'STALE'");

        assertUpdate("REFRESH MATERIALIZED VIEW " + outerMaterializedViewName, 2);
        assertQuery("SELECT * FROM " + outerMaterializedViewName, "VALUES 1, 2");
        assertQuery(outerFreshnessQuery, "VALUES 'FRESH'");
        assertUpdate("REFRESH MATERIALIZED VIEW " + innerMaterializedViewName, 2);
        assertQuery(innerFreshnessQuery, "VALUES 'FRESH'");

        Table baseTable = backend.loadTable(TableIdentifier.of(Namespace.of(schema), baseTableName));
        RefreshState updatedRefreshState = RefreshStateParser.fromJson(getStorageTableMetadata(outerMaterializedViewName).currentSnapshot().summary().get(RefreshState.REFRESH_STATE_SUMMARY_KEY));
        List<SourceState> updatedSourceStates = updatedRefreshState.sourceStates();
        assertThat(updatedSourceStates).hasSize(2);
        assertThat(updatedSourceStates.stream().filter(SourceTableState.class::isInstance).map(SourceTableState.class::cast).toList())
                .singleElement()
                .satisfies(sourceTableState -> {
                    assertThat(sourceTableState.name()).isEqualTo(baseTableName);
                    assertThat(sourceTableState.namespace()).containsExactly(schema);
                    assertThat(sourceTableState.uuid()).isEqualTo(baseTable.uuid().toString());
                    assertThat(sourceTableState.snapshotId()).isEqualTo(baseTable.currentSnapshot().snapshotId());
                });
        assertThat(updatedSourceStates.stream().filter(SourceViewState.class::isInstance).map(SourceViewState.class::cast).toList())
                .singleElement()
                .satisfies(sourceViewState -> {
                    assertThat(sourceViewState.name()).isEqualTo(innerMaterializedViewName);
                    assertThat(sourceViewState.namespace()).containsExactly(schema);
                    assertThat(sourceViewState.catalog()).isEqualTo(catalog);
                    assertThat(sourceViewState.uuid()).isEqualTo(innerView.uuid().toString());
                    assertThat(sourceViewState.versionId()).isEqualTo(innerView.currentVersion().versionId());
                });

        assertUpdate("DROP MATERIALIZED VIEW " + outerMaterializedViewName);
        assertUpdate("DROP MATERIALIZED VIEW " + innerMaterializedViewName);
        assertUpdate("DROP TABLE " + baseTableName);
    }

    @Test
    public void testMaterializedViewWithoutSourceTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        String materializedViewName = "test_mv_no_source_" + randomNameSuffix();
        String freshnessQuery = format(
                "SELECT freshness FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '%s'",
                materializedViewName);

        assertUpdate("CREATE MATERIALIZED VIEW " + materializedViewName + " AS SELECT 1 AS a");
        assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 1);
        assertQuery("SELECT * FROM " + materializedViewName, "VALUES 1");
        assertQuery(freshnessQuery, "VALUES 'FRESH'");

        View view = backend.loadView(TableIdentifier.of(Namespace.of(schema), materializedViewName));
        Map<String, String> summary = getStorageTableMetadata(materializedViewName).currentSnapshot().summary();
        RefreshState refreshState = RefreshStateParser.fromJson(summary.get(RefreshState.REFRESH_STATE_SUMMARY_KEY));
        assertThat(refreshState.sourceStates()).isEmpty();
        assertThat(refreshState.viewVersionId()).isEqualTo(view.currentVersion().versionId());
        assertThat(summary).doesNotContainKey("trino.materialized-view.freshness-unknown");

        // With no dependencies to go stale, the view is always fresh, so the engine skips this refresh
        assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 0);
        assertQuery(freshnessQuery, "VALUES 'FRESH'");

        assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
    }

    @Test
    public void testMaterializedViewWithCrossCatalogSourceView()
    {
        String schema = getSession().getSchema().orElseThrow();
        String baseTableName = "test_cross_catalog_base_" + randomNameSuffix();
        String viewName = "test_cross_catalog_view_" + randomNameSuffix();
        String materializedViewName = "test_cross_catalog_mv_" + randomNameSuffix();
        String freshnessQuery = format(
                "SELECT freshness FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '%s'",
                materializedViewName);

        assertUpdate("CREATE TABLE " + baseTableName + " AS SELECT 1 AS a", 1);
        // iceberg_legacy_mv points at the same REST backend, but is a distinct Trino catalog
        assertUpdate(format("CREATE VIEW iceberg_legacy_mv.%s.%s AS SELECT * FROM iceberg.%s.%s", schema, viewName, schema, baseTableName));
        assertUpdate(format("CREATE MATERIALIZED VIEW %s AS SELECT * FROM iceberg_legacy_mv.%s.%s", materializedViewName, schema, viewName));
        assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 1);
        assertQuery("SELECT * FROM " + materializedViewName, "VALUES 1");

        Map<String, String> summary = getStorageTableMetadata(materializedViewName).currentSnapshot().summary();
        assertThat(summary).containsEntry("trino.materialized-view.freshness-unknown", "true");
        assertQuery(freshnessQuery, "VALUES 'UNKNOWN'");

        assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
        assertUpdate(format("DROP VIEW iceberg_legacy_mv.%s.%s", schema, viewName));
        assertUpdate("DROP TABLE " + baseTableName);
    }

    @Test
    public void testMaterializedViewOnFreshNestedMaterializedView()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String baseTableName = "test_fresh_nested_mv_base_" + randomNameSuffix();
        String innerMaterializedViewName = "test_inner_mv_" + randomNameSuffix();
        String outerMaterializedViewName = "test_outer_mv_" + randomNameSuffix();
        String outerFreshnessQuery = format(
                "SELECT freshness FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '%s'",
                outerMaterializedViewName);

        assertUpdate("CREATE TABLE " + baseTableName + " AS SELECT 1 AS a", 1);
        assertUpdate("CREATE MATERIALIZED VIEW " + innerMaterializedViewName + " AS SELECT * FROM " + baseTableName);
        assertUpdate("REFRESH MATERIALIZED VIEW " + innerMaterializedViewName, 1);

        assertUpdate("CREATE MATERIALIZED VIEW " + outerMaterializedViewName + " AS SELECT * FROM " + innerMaterializedViewName);

        assertUpdate("REFRESH MATERIALIZED VIEW " + outerMaterializedViewName, 1);
        assertQuery("SELECT * FROM " + outerMaterializedViewName, "VALUES 1");

        View innerView = backend.loadView(TableIdentifier.of(Namespace.of(schema), innerMaterializedViewName));
        String innerStorageTableName = (String) computeScalar("SELECT storage_table FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '" + innerMaterializedViewName + "'");

        Table innerStorageTable = backend.loadTable(TableIdentifier.of(Namespace.of(schema), innerStorageTableName));
        String initialRefreshStateJson = getStorageTableMetadata(outerMaterializedViewName).currentSnapshot().summary().get(RefreshState.REFRESH_STATE_SUMMARY_KEY);
        RefreshState refreshState = RefreshStateParser.fromJson(initialRefreshStateJson);
        List<SourceState> sourceStates = refreshState.sourceStates();
        assertThat(sourceStates).hasSize(2);
        assertThat(sourceStates.stream().filter(SourceTableState.class::isInstance).map(SourceTableState.class::cast).toList())
                .singleElement()
                .satisfies(sourceTableState -> {
                    assertThat(sourceTableState.name()).isEqualTo(innerStorageTableName);
                    assertThat(sourceTableState.namespace()).containsExactly(schema);
                    assertThat(sourceTableState.snapshotId()).isEqualTo(innerStorageTable.currentSnapshot().snapshotId());
                });
        assertThat(sourceStates.stream().filter(SourceViewState.class::isInstance).map(SourceViewState.class::cast).toList())
                .singleElement()
                .satisfies(sourceViewState -> {
                    assertThat(sourceViewState.name()).isEqualTo(innerMaterializedViewName);
                    assertThat(sourceViewState.namespace()).containsExactly(schema);
                    assertThat(sourceViewState.catalog()).isEqualTo(catalog);
                    assertThat(sourceViewState.uuid()).isEqualTo(innerView.uuid().toString());
                    assertThat(sourceViewState.versionId()).isEqualTo(innerView.currentVersion().versionId());
                });

        // Refreshing the inner MV independently changes only its storage table's snapshot, not its ViewVersion,
        // so the outer MV's recorded SourceTableState (not its SourceViewState) is what picks up the staleness.
        assertUpdate("INSERT INTO " + baseTableName + " VALUES 2", 1);
        assertUpdate("REFRESH MATERIALIZED VIEW " + innerMaterializedViewName, 1);
        assertQuery("SELECT * FROM " + innerMaterializedViewName, "VALUES 1, 2");

        assertQuery(outerFreshnessQuery, "VALUES 'STALE'");
        String unchangedRefreshStateJson = getStorageTableMetadata(outerMaterializedViewName).currentSnapshot().summary().get(RefreshState.REFRESH_STATE_SUMMARY_KEY);
        assertThat(unchangedRefreshStateJson).isEqualTo(initialRefreshStateJson);

        assertUpdate("REFRESH MATERIALIZED VIEW " + outerMaterializedViewName, 2);
        assertQuery("SELECT * FROM " + outerMaterializedViewName, "VALUES 1, 2");
        assertQuery(outerFreshnessQuery, "VALUES 'FRESH'");

        Table innerStorageTableUpdated = backend.loadTable(TableIdentifier.of(Namespace.of(schema), innerStorageTableName));
        RefreshState updatedRefreshState = RefreshStateParser.fromJson(getStorageTableMetadata(outerMaterializedViewName).currentSnapshot().summary().get(RefreshState.REFRESH_STATE_SUMMARY_KEY));
        assertThat(updatedRefreshState.sourceStates().stream().filter(SourceTableState.class::isInstance).map(SourceTableState.class::cast).toList())
                .singleElement()
                .satisfies(sourceTableState -> {
                    assertThat(sourceTableState.name()).isEqualTo(innerStorageTableName);
                    assertThat(sourceTableState.snapshotId()).isEqualTo(innerStorageTableUpdated.currentSnapshot().snapshotId());
                });
        assertThat(updatedRefreshState.sourceStates().stream().filter(SourceViewState.class::isInstance).map(SourceViewState.class::cast).toList())
                .singleElement()
                .satisfies(sourceViewState -> {
                    assertThat(sourceViewState.name()).isEqualTo(innerMaterializedViewName);
                    assertThat(sourceViewState.namespace()).containsExactly(schema);
                    assertThat(sourceViewState.catalog()).isEqualTo(catalog);
                    assertThat(sourceViewState.uuid()).isEqualTo(innerView.uuid().toString());
                    assertThat(sourceViewState.versionId()).isEqualTo(innerView.currentVersion().versionId());
                });

        assertUpdate("DROP MATERIALIZED VIEW " + outerMaterializedViewName);
        assertUpdate("DROP MATERIALIZED VIEW " + innerMaterializedViewName);
        assertUpdate("DROP TABLE " + baseTableName);
    }

    @Test
    public void testMaterializedViewGoesStaleWhenSourceSchemaChangesWithoutNewSnapshot()
    {
        String sourceTableName = "test_schema_only_change_source_" + randomNameSuffix();
        String materializedViewName = "test_schema_only_change_mv_" + randomNameSuffix();
        String freshnessQuery = format(
                "SELECT freshness FROM system.metadata.materialized_views WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND name = '%s'",
                materializedViewName);

        assertUpdate("CREATE TABLE " + sourceTableName + " (id INT, name VARCHAR)");
        assertUpdate("INSERT INTO " + sourceTableName + " VALUES (1, 'a')", 1);
        assertUpdate("CREATE MATERIALIZED VIEW " + materializedViewName + " AS SELECT * FROM " + sourceTableName);
        assertUpdate("REFRESH MATERIALIZED VIEW " + materializedViewName, 1);
        assertQuery(freshnessQuery, "VALUES 'FRESH'");

        // A column rename is schema-only: it doesn't create a new snapshot, so the recorded snapshot id
        // alone would look unchanged despite the source's schema having evolved
        assertUpdate("ALTER TABLE " + sourceTableName + " RENAME COLUMN name TO full_name");
        assertQuery(freshnessQuery, "VALUES 'STALE'");

        assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
        assertUpdate("DROP TABLE " + sourceTableName);
    }
}
