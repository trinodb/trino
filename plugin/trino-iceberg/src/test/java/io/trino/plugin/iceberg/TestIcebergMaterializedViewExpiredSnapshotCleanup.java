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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeId;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Snapshot;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Lists.newArrayList;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.metastore.TableInfo.ICEBERG_MATERIALIZED_VIEW_COMMENT;
import static io.trino.plugin.hive.HiveMetadata.TRINO_CREATED_BY;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.ViewReaderUtil.PRESTO_VIEW_FLAG;
import static io.trino.plugin.iceberg.IcebergMaterializedViewDefinition.encodeMaterializedViewData;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.SESSION;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.plugin.iceberg.IcebergTestUtils.getHiveMetastore;
import static io.trino.plugin.iceberg.IcebergTestUtils.getTrinoCatalog;
import static io.trino.plugin.iceberg.catalog.AbstractTrinoCatalog.TRINO_CREATED_BY_VALUE;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static java.lang.String.format;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergMaterializedViewExpiredSnapshotCleanup
        extends AbstractTestQueryFramework
{
    private static final String MV_METADATA_FILE_NAME = "00005-b395c800-9c5a-48d2-9dbe-ba7b630881aa.metadata.json";
    private static final String BUCKET_NAME = "test-bucket-mv-refresh-expired-snapshots";
    private static final String RESOURCE_DIRECTORY = "materialized_view_expired_snapshots";
    private static final String S3_LOCATION_PREFIX = format("s3://%s/%s", BUCKET_NAME, RESOURCE_DIRECTORY);

    private Minio minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(BUCKET_NAME);

        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.of(
                        "iceberg.materialized-views.refresh-max-snapshots-to-expire", "5",
                        "fs.native-s3.enabled", "true",
                        "s3.aws-access-key", MINIO_ACCESS_KEY,
                        "s3.aws-secret-key", MINIO_SECRET_KEY,
                        "s3.region", MINIO_REGION,
                        "s3.endpoint", minio.getMinioAddress(),
                        "iceberg.register-table-procedure.enabled", "true",
                        "s3.path-style-access", "true"))
                .build();
    }

    /**
     * @see iceberg.materialized_view_expired_snapshots
     */
    @Test
    public void testPreviousSnapshotCleanupDuringRefresh()
            throws IOException
    {
        String sourceTableName = "source_table";
        String materializedViewName = "materialized_view";

        minio.copyResources(format("iceberg/%s", RESOURCE_DIRECTORY), BUCKET_NAME, RESOURCE_DIRECTORY);
        getQueryRunner().execute(format("CALL system.register_table(CURRENT_SCHEMA, '%s', '%s')", sourceTableName, format("%s/source_table", S3_LOCATION_PREFIX)));
        assertQuery(format("SELECT * FROM %s", sourceTableName), "VALUES 4");

        registerMaterializedView(
                materializedViewName,
                sourceTableName,
                format("%s/materialized_view/metadata/%s", S3_LOCATION_PREFIX, MV_METADATA_FILE_NAME));
        assertQuery(format("SELECT * FROM %s", materializedViewName), "VALUES 4");

        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(getQueryRunner());

        var firstSnapshotFiles = getMaterializedViewSnapshots(materializedViewName, fileSystemFactory);
        assertThat(firstSnapshotFiles.size()).isEqualTo(10);

        // Test that snapshots are removed up to the cap
        assertUpdate(format("DELETE FROM %s WHERE a = 4", sourceTableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES 5", sourceTableName), 1);
        assertUpdate(format("REFRESH MATERIALIZED VIEW %s", materializedViewName), 1);
        assertQuery(format("SELECT a FROM %s", materializedViewName), "VALUES 5");

        List<Snapshot> secondSnapshotFiles = getMaterializedViewSnapshots(materializedViewName, fileSystemFactory);
        Ordering<Snapshot> snapshotOrdering = Ordering.from(Comparator.comparing(Snapshot::timestampMillis));

        assertThat(secondSnapshotFiles.size()).isEqualTo(7);
        assertThat(secondSnapshotFiles).doesNotContainAnyElementsOf(snapshotOrdering.leastOf(firstSnapshotFiles, 5));

        assertUpdate(format("DELETE FROM %s WHERE a = 5", sourceTableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES 6", sourceTableName), 1);
        assertUpdate(format("REFRESH MATERIALIZED VIEW %s", materializedViewName), 1);
        assertQuery(format("SELECT a FROM %s", materializedViewName), "VALUES 6");

        List<Snapshot> thirdSnapshotFiles = getMaterializedViewSnapshots(materializedViewName, fileSystemFactory);

        assertThat(thirdSnapshotFiles.size()).isEqualTo(4);
        assertThat(thirdSnapshotFiles).doesNotContainAnyElementsOf(snapshotOrdering.leastOf(secondSnapshotFiles, 5));

        // All additional snapshots are within the retention threshold, and will not be removed
        assertUpdate(format("DELETE FROM %s WHERE a = 6", sourceTableName), 1);
        assertUpdate(format("INSERT INTO %s VALUES 7", sourceTableName), 1);
        assertUpdate(format("REFRESH MATERIALIZED VIEW %s", materializedViewName), 1);
        assertQuery(format("SELECT a FROM %s", materializedViewName), "VALUES 7");

        List<Snapshot> fourthSnapshotFiles = getMaterializedViewSnapshots(materializedViewName, fileSystemFactory);

        assertThat(fourthSnapshotFiles.size()).isEqualTo(6);
        assertThat(fourthSnapshotFiles).containsAll(thirdSnapshotFiles);

        assertUpdate(format("DROP MATERIALIZED VIEW %s", materializedViewName));
        assertUpdate(format("DROP TABLE %s", sourceTableName));
    }

    private void registerMaterializedView(String materializedViewName, String sourceTableName, String metadataFileLocation)
    {
        HiveMetastore metastore = getHiveMetastore(getQueryRunner());
        String schema = getSession().getSchema().orElseThrow();

        List<IcebergMaterializedViewDefinition.Column> columns = ImmutableList.of(
                new IcebergMaterializedViewDefinition.Column("a", TypeId.of("integer"), Optional.empty()));
        IcebergMaterializedViewDefinition definition = new IcebergMaterializedViewDefinition(
                format("SELECT * FROM %s", sourceTableName),
                Optional.of(ICEBERG_CATALOG),
                Optional.of(schema),
                columns,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(new CatalogSchemaName(ICEBERG_CATALOG, schema)));
        String encodedDefinition = encodeMaterializedViewData(definition);

        Table materializedView = Table.builder()
                .setDatabaseName(schema)
                .setTableName(materializedViewName)
                .setOwner(Optional.of("test"))
                .setTableType(VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(new Column("dummy", HIVE_STRING, Optional.empty(), ImmutableMap.of())))
                .setParameters(Map.of(
                        METADATA_LOCATION_PROP, metadataFileLocation,
                        PRESTO_VIEW_FLAG, "true",
                        TRINO_CREATED_BY, TRINO_CREATED_BY_VALUE,
                        TABLE_COMMENT, ICEBERG_MATERIALIZED_VIEW_COMMENT))
                .withStorage(storage -> storage.setStorageFormat(VIEW_STORAGE_FORMAT))
                .withStorage(storage -> storage.setLocation(""))
                .setViewOriginalText(Optional.of(encodedDefinition))
                .build();
        metastore.createTable(materializedView, NO_PRIVILEGES);
    }

    private List<Snapshot> getMaterializedViewSnapshots(String materializedViewName, TrinoFileSystemFactory fileSystemFactory)
    {
        TrinoCatalog catalog = getTrinoCatalog(getHiveMetastore(getQueryRunner()), fileSystemFactory, ICEBERG_CATALOG);
        BaseTable table = catalog.getMaterializedViewStorageTable(SESSION, new SchemaTableName(getSession().getSchema().orElseThrow(), materializedViewName))
                .orElseThrow(() -> new TrinoException(TABLE_NOT_FOUND, "Storage table metadata not found for materialized view " + materializedViewName));
        return newArrayList(table.snapshots());
    }

    @AfterAll
    public void destroy()
            throws Exception
    {
        minio = null;
    }
}
