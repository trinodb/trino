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
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.containers.UnityCatalogContainer;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergUnityRestCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final Path warehouseLocation;
    private UnityCatalogContainer unityCatalog;

    public TestIcebergUnityRestCatalogConnectorSmokeTest()
            throws IOException
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
        warehouseLocation = Files.createTempDirectory(null);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_MATERIALIZED_VIEW,
                 SUPPORTS_RENAME_SCHEMA -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        closeAfterClass(() -> deleteRecursively(warehouseLocation, ALLOW_INSECURE));
        unityCatalog = closeAfterClass(new UnityCatalogContainer("unity", "tpch"));

        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(warehouseLocation))
                .addIcebergProperty("iceberg.file-format", format.name())
                .addIcebergProperty("iceberg.security", "read_only")
                .addIcebergProperty("iceberg.catalog.type", "rest")
                .addIcebergProperty("iceberg.rest-catalog.uri", unityCatalog.uri() + "/iceberg")
                .addIcebergProperty("iceberg.rest-catalog.warehouse", "unity")
                .addIcebergProperty("iceberg.register-table-procedure.enabled", "true")
                .disableSchemaInitializer()
                .build();

        unityCatalog.copyTpchTables(REQUIRED_TPCH_TABLES);

        return queryRunner;
    }

    @Override
    protected void createSchema(String schemaName)
    {
        unityCatalog.createSchema(schemaName);
    }

    @Override
    protected AutoCloseable createTable(String schema, String tableName, String tableDefinition)
    {
        unityCatalog.createTable(schema, tableName, tableDefinition);
        return () -> unityCatalog.dropTable(schema, tableName);
    }

    @Override
    protected void dropSchema(String schema)
    {
        unityCatalog.dropSchema(schema);
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String schemaPath()
    {
        return format("%s/%s", warehouseLocation, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try {
            deleteRecursively(Path.of(location), ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testCommentView()
    {
        assertThatThrownBy(super::testCommentView)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        assertThatThrownBy(super::testCommentViewColumn)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchemas)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateTable()
    {
        assertThatThrownBy(super::testCreateTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        assertThatThrownBy(super::testCreateTableAsSelect)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testInsert()
    {
        assertThatThrownBy(super::testInsert)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testHiddenPathColumn()
    {
        assertThatThrownBy(super::testHiddenPathColumn)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        assertThatThrownBy(super::testRowLevelDelete)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDeleteAllDataFromTable()
    {
        assertThatThrownBy(super::testDeleteAllDataFromTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDeleteRowsConcurrently()
    {
        assertThatThrownBy(super::testDeleteRowsConcurrently)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateOrReplaceTable()
    {
        assertThatThrownBy(super::testCreateOrReplaceTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateOrReplaceTableChangeColumnNamesAndTypes()
    {
        assertThatThrownBy(super::testCreateOrReplaceTableChangeColumnNamesAndTypes)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithTableLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTableLocation)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithComments()
    {
        assertThatThrownBy(super::testRegisterTableWithComments)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        assertThatThrownBy(super::testRowLevelUpdate)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testMerge()
    {
        assertThatThrownBy(super::testMerge)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateSchema()
    {
        assertThatThrownBy(super::testCreateSchema)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        assertThatThrownBy(super::testCreateSchemaWithNonLowercaseOwnerName)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithShowCreateTable()
    {
        assertThatThrownBy(super::testRegisterTableWithShowCreateTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithReInsert()
    {
        assertThatThrownBy(super::testRegisterTableWithReInsert)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithDroppedTable()
    {
        assertThatThrownBy(super::testRegisterTableWithDroppedTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithDifferentTableName()
    {
        assertThatThrownBy(super::testRegisterTableWithDifferentTableName)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithMetadataFile()
    {
        assertThatThrownBy(super::testRegisterTableWithMetadataFile)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testCreateTableWithTrailingSpaceInLocation)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTrailingSpaceInLocation)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testUnregisterTable()
    {
        assertThatThrownBy(super::testUnregisterTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testUnregisterBrokenTable()
    {
        assertThatThrownBy(super::testUnregisterBrokenTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testUnregisterTableNotExistingTable()
    {
        assertThatThrownBy(super::testUnregisterTableNotExistingTable)
                .hasStackTraceContaining("Table .* not found");
    }

    @Test
    @Override
    public void testUnregisterTableNotExistingSchema()
    {
        assertThatThrownBy(super::testUnregisterTableNotExistingSchema)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRepeatUnregisterTable()
    {
        assertThatThrownBy(super::testRepeatUnregisterTable)
                .hasStackTraceContaining("Table .* not found");
    }

    @Test
    @Override
    public void testUnregisterTableAccessControl()
    {
        assertThatThrownBy(super::testUnregisterTableAccessControl)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateTableWithNonExistingSchemaVerifyLocation()
    {
        assertThatThrownBy(super::testCreateTableWithNonExistingSchemaVerifyLocation)
                .hasStackTraceContaining("Access Denied");
    }

    @Test
    @Override
    public void testSortedNationTable()
    {
        assertThatThrownBy(super::testSortedNationTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testFileSortingWithLargerTable()
    {
        assertThatThrownBy(super::testFileSortingWithLargerTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingMetadataFile)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingSnapshotFile)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingManifestListFile)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDropTableWithMissingDataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingDataFile)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        assertThatThrownBy(super::testDropTableWithNonExistentTableLocation)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testMetadataTables()
    {
        assertThatThrownBy(super::testMetadataTables)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testPartitionFilterRequired()
    {
        assertThatThrownBy(super::testPartitionFilterRequired)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testTableChangesFunction()
    {
        assertThatThrownBy(super::testTableChangesFunction)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testRowLevelDeletesWithTableChangesFunction()
    {
        assertThatThrownBy(super::testRowLevelDeletesWithTableChangesFunction)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testCreateOrReplaceWithTableChangesFunction()
    {
        assertThatThrownBy(super::testCreateOrReplaceWithTableChangesFunction)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testTruncateTable()
    {
        assertThatThrownBy(super::testTruncateTable)
                .hasMessageContaining("Access Denied");
    }

    @Test
    @Override
    public void testMetadataDeleteAfterCommitEnabled()
    {
        assertThatThrownBy(super::testMetadataDeleteAfterCommitEnabled)
                .hasStackTraceContaining("Access Denied");
    }
}
