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
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
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
    protected void dropSchema(String schema)
    {
        unityCatalog.dropSchema(schema);
    }

    @Override
    protected AutoCloseable createTable(String schema, String tableName, String tableDefinition)
    {
        unityCatalog.createTable(schema, tableName, tableDefinition);
        return () -> unityCatalog.dropTable(schema, tableName);
    }

    @Override
    protected void dropTableFromCatalog(String tableName)
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
        testFailsDueToReadOnlyCatalog(super::testView);
    }

    @Test
    @Override
    public void testCommentView()
    {
        testFailsDueToReadOnlyCatalog(super::testCommentView);
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        testFailsDueToReadOnlyCatalog(super::testCommentViewColumn);
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        testFailsDueToReadOnlyCatalog(super::testMaterializedView);
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        testFailsDueToReadOnlyCatalog(super::testRenameSchema);
    }

    @Test
    @Override
    public void testRenameTable()
    {
        testFailsDueToReadOnlyCatalog(super::testRenameTable);
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        testFailsDueToReadOnlyCatalog(super::testRenameTableAcrossSchemas);
    }

    @Test
    @Override
    public void testCreateTable()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateTable);
    }

    @Test
    @Override
    public void testCreateTableAsSelect()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateTableAsSelect);
    }

    @Test
    @Override
    public void testUpdate()
    {
        testFailsDueToReadOnlyCatalog(super::testUpdate);
    }

    @Test
    @Override
    public void testInsert()
    {
        testFailsDueToReadOnlyCatalog(super::testInsert);
    }

    @Test
    @Override
    public void testHiddenPathColumn()
    {
        testFailsDueToReadOnlyCatalog(super::testHiddenPathColumn);
    }

    @Test
    @Override
    public void testRowLevelDelete()
    {
        testFailsDueToReadOnlyCatalog(super::testRowLevelDelete);
    }

    @Test
    @Override
    public void testDeleteAllDataFromTable()
    {
        testFailsDueToReadOnlyCatalog(super::testDeleteAllDataFromTable);
    }

    @Test
    @Override
    public void testDeleteRowsConcurrently()
    {
        testFailsDueToReadOnlyCatalog(super::testDeleteRowsConcurrently);
    }

    @Test
    @Override
    public void testCreateOrReplaceTable()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateOrReplaceTable);
    }

    @Test
    @Override
    public void testCreateOrReplaceTableChangeColumnNamesAndTypes()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateOrReplaceTableChangeColumnNamesAndTypes);
    }

    @Test
    @Override
    public void testRecreateTableWithSameName()
    {
        testFailsDueToReadOnlyCatalog(super::testRecreateTableWithSameName);
    }

    @Test
    @Override
    public void testRegisterTableWithTableLocation()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithTableLocation);
    }

    @Test
    @Override
    public void testRegisterTableWithComments()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithComments);
    }

    @Test
    @Override
    public void testRowLevelUpdate()
    {
        testFailsDueToReadOnlyCatalog(super::testRowLevelUpdate);
    }

    @Test
    @Override
    public void testMerge()
    {
        testFailsDueToReadOnlyCatalog(super::testMerge);
    }

    @Test
    @Override
    public void testCreateSchema()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateSchema);
    }

    @Test
    @Override
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateSchemaWithNonLowercaseOwnerName);
    }

    @Test
    @Override
    public void testRegisterTableWithShowCreateTable()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithShowCreateTable);
    }

    @Test
    @Override
    public void testRegisterTableWithReInsert()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithReInsert);
    }

    @Test
    @Override
    public void testRegisterTableWithDroppedTable()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithDroppedTable);
    }

    @Test
    @Override
    public void testRegisterTableWithDifferentTableName()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithDifferentTableName);
    }

    @Test
    @Override
    public void testRegisterTableWithMetadataFile()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithMetadataFile);
    }

    @Test
    @Override
    public void testCreateTableWithTrailingSpaceInLocation()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateTableWithTrailingSpaceInLocation);
    }

    @Test
    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        testFailsDueToReadOnlyCatalog(super::testRegisterTableWithTrailingSpaceInLocation);
    }

    @Test
    @Override
    public void testUnregisterTable()
    {
        testFailsDueToReadOnlyCatalog(super::testUnregisterTable);
    }

    @Test
    @Override
    public void testUnregisterBrokenTable()
    {
        testFailsDueToReadOnlyCatalog(super::testUnregisterBrokenTable);
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
        testFailsDueToReadOnlyCatalog(super::testUnregisterTableNotExistingSchema);
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
        testFailsDueToReadOnlyCatalog(super::testUnregisterTableAccessControl);
    }

    @Test
    @Override
    public void testCreateTableWithNonExistingSchemaVerifyLocation()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateTableWithNonExistingSchemaVerifyLocation);
    }

    @Test
    @Override
    public void testSortedNationTable()
    {
        testFailsDueToReadOnlyCatalog(super::testSortedNationTable);
    }

    @Test
    @Override
    public void testFileSortingWithLargerTable()
    {
        testFailsDueToReadOnlyCatalog(super::testFileSortingWithLargerTable);
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        testFailsDueToReadOnlyCatalog(super::testDropTableWithMissingMetadataFile);
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        testFailsDueToReadOnlyCatalog(super::testDropTableWithMissingSnapshotFile);
    }

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        testFailsDueToReadOnlyCatalog(super::testDropTableWithMissingManifestListFile);
    }

    @Test
    @Override
    public void testDropTableWithMissingDataFile()
    {
        testFailsDueToReadOnlyCatalog(super::testDropTableWithMissingDataFile);
    }

    @Test
    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        testFailsDueToReadOnlyCatalog(super::testDropTableWithNonExistentTableLocation);
    }

    @Test
    @Override
    public void testMetadataTables()
    {
        testFailsDueToReadOnlyCatalog(super::testMetadataTables);
    }

    @Test
    @Override
    public void testPartitionFilterRequired()
    {
        testFailsDueToReadOnlyCatalog(super::testPartitionFilterRequired);
    }

    @Test
    @Override
    public void testTableChangesFunction()
    {
        testFailsDueToReadOnlyCatalog(super::testTableChangesFunction);
    }

    @Test
    @Override
    public void testRowLevelDeletesWithTableChangesFunction()
    {
        testFailsDueToReadOnlyCatalog(super::testRowLevelDeletesWithTableChangesFunction);
    }

    @Test
    @Override
    public void testCreateOrReplaceWithTableChangesFunction()
    {
        testFailsDueToReadOnlyCatalog(super::testCreateOrReplaceWithTableChangesFunction);
    }

    @Test
    @Override
    public void testTruncateTable()
    {
        testFailsDueToReadOnlyCatalog(super::testTruncateTable);
    }

    @Test
    @Override
    public void testMetadataDeleteAfterCommitEnabled()
    {
        testFailsDueToReadOnlyCatalog(super::testMetadataDeleteAfterCommitEnabled);
    }

    @Test
    @Override
    public void testAnalyze()
    {
        testFailsDueToReadOnlyCatalog(super::testAnalyze);
    }

    /**
     * Verifies that the given action fails due to the read-only security mode
     * configured via {@code iceberg.security=read_only}.
     */
    private static void testFailsDueToReadOnlyCatalog(ThrowingCallable callable)
    {
        String[] expectedReasons = Stream.of(
                        "Cannot create schema",
                        "Cannot rename schema",
                        "Cannot create table",
                        "Cannot create materialized view",
                        "Cannot create view",
                        "Cannot execute procedure")
                .map(reason -> "Access Denied: " + reason)
                .toArray(String[]::new);
        assertThatThrownBy(callable)
                .satisfies(throwable -> assertThat(getStackTraceAsString(throwable))
                        .containsAnyOf(expectedReasons));
    }
}
