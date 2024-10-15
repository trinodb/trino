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

import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergConnector;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.iceberg.BaseTable;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@Isolated // TODO remove
@TestInstance(PER_CLASS)
final class TestIcebergPolarisCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private TestingPolarisCatalog polarisCatalog;
    private File warehouseLocation;

    public TestIcebergPolarisCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        polarisCatalog = closeAfterClass(new TestingPolarisCatalog(warehouseLocation.getPath()));

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation.toPath()))
                .addIcebergProperty("iceberg.file-format", format.name())
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                .addIcebergProperty("iceberg.writer-sort-buffer-size", "1MB")
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", polarisCatalog.restUri() + "/api/catalog")
                .addIcebergProperty("iceberg.rest-catalog.warehouse", TestingPolarisCatalog.WAREHOUSE)
                .addIcebergProperty("iceberg.rest-catalog.security", "OAUTH2")
                .addIcebergProperty("iceberg.rest-catalog.oauth2.credential", polarisCatalog.oauth2Credentials())
                .addIcebergProperty("iceberg.rest-catalog.oauth2.scope", "PRINCIPAL_ROLE:ALL")
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        polarisCatalog.dropTable(getSession().getSchema().orElseThrow(), tableName);
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        TrinoCatalogFactory catalogFactory = ((IcebergConnector) getQueryRunner().getCoordinator().getConnector("iceberg")).getInjector().getInstance(TrinoCatalogFactory.class);
        TrinoCatalog trinoCatalog = catalogFactory.create(getSession().getIdentity().toConnectorIdentity());
        BaseTable table = (BaseTable) trinoCatalog.loadTable(getSession().toConnectorSession(), new SchemaTableName(getSession().getSchema().orElseThrow(), tableName));
        return table.operations().current().metadataFileLocation();
    }

    @Override
    protected String getTableLocation(String tableName)
    {
        TrinoCatalogFactory catalogFactory = ((IcebergConnector) getQueryRunner().getCoordinator().getConnector("iceberg")).getInjector().getInstance(TrinoCatalogFactory.class);
        TrinoCatalog trinoCatalog = catalogFactory.create(getSession().getIdentity().toConnectorIdentity());
        BaseTable table = (BaseTable) trinoCatalog.loadTable(getSession().toConnectorSession(), new SchemaTableName(getSession().getSchema().orElseThrow(), tableName));
        return table.operations().current().location();
    }

    @Override
    protected String schemaPath()
    {
        return format("file://%s/%s", warehouseLocation, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        return java.nio.file.Files.exists(Path.of(location));
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
        try {
            deleteRecursively(Path.of(location.replaceAll("^file://", "")), ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasMessageContaining("createMaterializedView is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessageContaining("renameNamespace is not supported for Iceberg REST catalog");
    }

    @Test
    @Override
    @Disabled("Disable as register table is broken with S3 in Polaris. More info at https://github.com/trinodb/trino/pull/23099")
    public void testRegisterTableWithDroppedTable()
    {
        assertThatThrownBy(super::testRegisterTableWithDroppedTable)
                .hasStackTraceContaining("Failed to open input stream for file");
    }

    @Test
    @Override
    public void testDropTableWithMissingDataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingDataFile)
                .hasMessageContaining("Expecting value to be false but was true");
    }

    @Test
    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTrailingSpaceInLocation)
                .hasStackTraceContaining("Illegal character in path");
    }

    @Test
    @Override
    public void testCreateTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testCreateTableWithTrailingSpaceInLocation)
                .hasStackTraceContaining("Illegal character in path");
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingMetadataFile)
                .hasMessageMatching(".* Table '.*' does not exist");
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingSnapshotFile)
                .hasStackTraceContaining("Expecting value to be false but was true");
    }

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingManifestListFile)
                .hasMessageContaining("Table location should not exist");
    }

    @Test
    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        assertThatThrownBy(super::testDropTableWithNonExistentTableLocation)
                .hasMessageMatching(".* Table '.*' does not exist");
    }

    @Test
    @Override
    public void testDeleteRowsConcurrently()
    {
        abort("Skipped for now due to flakiness");
    }
}
