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
package io.trino.plugin.iceberg.catalog.nessie;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.containers.NessieContainer;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.nessie.NessieCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.CatalogProperties.CATALOG_IMPL;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.CatalogUtil.buildIcebergCatalog;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestIcebergNessieCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private Path tempDir;
    private NessieCatalog catalog;

    public TestIcebergNessieCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @AfterAll
    public void teardown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        NessieContainer nessieContainer = closeAfterClass(NessieContainer.builder().build());
        nessieContainer.start();

        tempDir = Files.createTempDirectory("test_trino_nessie_catalog");

        catalog = (NessieCatalog) buildIcebergCatalog("tpch", ImmutableMap.<String, String>builder()
                        .put(CATALOG_IMPL, NessieCatalog.class.getName())
                        .put(URI, nessieContainer.getRestApiUri())
                        .put(WAREHOUSE_LOCATION, tempDir.toString())
                        .buildOrThrow(),
                new Configuration(false));

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(tempDir))
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.file-format", format.name(),
                                "iceberg.catalog.type", "nessie",
                                "iceberg.nessie-catalog.uri", nessieContainer.getRestApiUri(),
                                "iceberg.nessie-catalog.default-warehouse-dir", tempDir.toString(),
                                "iceberg.writer-sort-buffer-size", "1MB",
                                "iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max"))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(ImmutableList.<TpchTable<?>>builder()
                                        .addAll(REQUIRED_TPCH_TABLES)
                                        .build())
                                .build())
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_VIEW, SUPPORTS_CREATE_MATERIALIZED_VIEW, SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasStackTraceContaining("createView is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasStackTraceContaining("createMaterializedView is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("renameNamespace is not supported for Iceberg Nessie catalogs");
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        // used when registering a table, which is not supported by the Nessie catalog
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        BaseTable table = (BaseTable) catalog.loadTable(TableIdentifier.of("tpch", tableName));
        return table.operations().current().metadataFileLocation();
    }

    @Test
    @Override
    public void testRegisterTableWithTableLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTableLocation)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithComments()
    {
        assertThatThrownBy(super::testRegisterTableWithComments)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithShowCreateTable()
    {
        assertThatThrownBy(super::testRegisterTableWithShowCreateTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithReInsert()
    {
        assertThatThrownBy(super::testRegisterTableWithReInsert)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithDroppedTable()
    {
        assertThatThrownBy(super::testRegisterTableWithDroppedTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithDifferentTableName()
    {
        assertThatThrownBy(super::testRegisterTableWithDifferentTableName)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithMetadataFile()
    {
        assertThatThrownBy(super::testRegisterTableWithMetadataFile)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTrailingSpaceInLocation)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testUnregisterTable()
    {
        assertThatThrownBy(super::testUnregisterTable)
                .hasStackTraceContaining("unregisterTable is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testUnregisterBrokenTable()
    {
        assertThatThrownBy(super::testUnregisterBrokenTable)
                .hasStackTraceContaining("unregisterTable is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testUnregisterTableNotExistingTable()
    {
        assertThatThrownBy(super::testUnregisterTableNotExistingTable)
                .hasStackTraceContaining("unregisterTable is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testRepeatUnregisterTable()
    {
        assertThatThrownBy(super::testRepeatUnregisterTable)
                .hasStackTraceContaining("unregisterTable is not supported for Iceberg Nessie catalogs");
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_metadata_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        Location metadataLocation = Location.of(getMetadataLocation(tableName));
        Location tableLocation = Location.of(getTableLocation(tableName));

        // Delete current metadata file
        fileSystem.deleteFile(metadataLocation);
        assertThat(fileSystem.newInputFile(metadataLocation).exists())
                .describedAs("Current metadata file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should exist")
                .isTrue();
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_snapshot_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        String metadataLocation = getMetadataLocation(tableName);
        TableMetadata tableMetadata = TableMetadataParser.read(new ForwardingFileIo(fileSystem), metadataLocation);
        Location tableLocation = Location.of(tableMetadata.location());
        Location currentSnapshotFile = Location.of(tableMetadata.currentSnapshot().manifestListLocation());

        // Delete current snapshot file
        fileSystem.deleteFile(currentSnapshotFile);
        assertThat(fileSystem.newInputFile(currentSnapshotFile).exists())
                .describedAs("Current snapshot file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should exist")
                .isTrue();
    }

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_manifest_list_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);

        String metadataLocation = getMetadataLocation(tableName);
        FileIO fileIo = new ForwardingFileIo(fileSystem);
        TableMetadata tableMetadata = TableMetadataParser.read(fileIo, metadataLocation);
        Location tableLocation = Location.of(tableMetadata.location());
        Location manifestListFile = Location.of(tableMetadata.currentSnapshot().allManifests(fileIo).get(0).path());

        // Delete Manifest List file
        fileSystem.deleteFile(manifestListFile);
        assertThat(fileSystem.newInputFile(manifestListFile).exists())
                .describedAs("Manifest list file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should exist")
                .isTrue();
    }

    @Test
    @Override
    public void testDropTableWithMissingDataFile()
            throws Exception
    {
        String tableName = "test_drop_table_with_missing_data_file_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, 'INDIA' y", 1);
        assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'POLAND')", 1);

        Location tableLocation = Location.of(getTableLocation(tableName));
        Location tableDataPath = tableLocation.appendPath("data");
        FileIterator fileIterator = fileSystem.listFiles(tableDataPath);
        assertThat(fileIterator.hasNext()).isTrue();
        Location dataFile = fileIterator.next().location();

        // Delete data file
        fileSystem.deleteFile(dataFile);
        assertThat(fileSystem.newInputFile(dataFile).exists())
                .describedAs("Data file should not exist")
                .isFalse();

        // try to drop table
        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(fileSystem.listFiles(tableLocation).hasNext())
                .describedAs("Table location should exist")
                .isTrue();
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        if (format == PARQUET) {
            return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
        }
        return checkOrcFileSorting(fileSystem, path, sortColumnName);
    }

    @Override
    protected void deleteDirectory(String location)
    {
        // used when unregistering a table, which is not supported by the Nessie catalog
    }

    @Override
    protected String schemaPath()
    {
        return format("%s/%s", tempDir, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        return Files.exists(Path.of(location));
    }
}
