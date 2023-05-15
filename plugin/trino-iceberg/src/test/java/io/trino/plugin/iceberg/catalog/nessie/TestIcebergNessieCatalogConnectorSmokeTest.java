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
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.plugin.iceberg.containers.NessieContainer;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIcebergNessieCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private Path tempDir;

    public TestIcebergNessieCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
    }

    @AfterClass(alwaysRun = true)
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

        return IcebergQueryRunner.builder()
                .setBaseDataDir(Optional.of(tempDir))
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.file-format", format.name(),
                                "iceberg.catalog.type", "nessie",
                                "iceberg.nessie-catalog.uri", nessieContainer.getRestApiUri(),
                                "iceberg.nessie-catalog.default-warehouse-dir", tempDir.toString(),
                                "iceberg.writer-sort-buffer-size", "1MB"))
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

    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasStackTraceContaining("createView is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasStackTraceContaining("createMaterializedView is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("renameNamespace is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testDeleteRowsConcurrently()
    {
        throw new SkipException("skipped for now due to flakiness");
    }

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        // used when registering a table, which is not supported by the Nessie catalog
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        // used when registering a table, which is not supported by the Nessie catalog
        throw new UnsupportedOperationException("metadata location for register_table is not supported");
    }

    @Override
    public void testRegisterTableWithTableLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTableLocation)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Override
    public void testRegisterTableWithComments()
    {
        assertThatThrownBy(super::testRegisterTableWithComments)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Override
    public void testRegisterTableWithShowCreateTable()
    {
        assertThatThrownBy(super::testRegisterTableWithShowCreateTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Override
    public void testRegisterTableWithReInsert()
    {
        assertThatThrownBy(super::testRegisterTableWithReInsert)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Override
    public void testRegisterTableWithDroppedTable()
    {
        assertThatThrownBy(super::testRegisterTableWithDroppedTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Override
    public void testRegisterTableWithDifferentTableName()
    {
        assertThatThrownBy(super::testRegisterTableWithDifferentTableName)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Override
    public void testRegisterTableWithMetadataFile()
    {
        assertThatThrownBy(super::testRegisterTableWithMetadataFile)
                .hasMessageContaining("metadata location for register_table is not supported");
    }

    @Override
    public void testRegisterTableWithTrailingSpaceInLocation()
    {
        assertThatThrownBy(super::testRegisterTableWithTrailingSpaceInLocation)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Override
    public void testUnregisterTable()
    {
        assertThatThrownBy(super::testUnregisterTable)
                .hasStackTraceContaining("unregisterTable is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testUnregisterBrokenTable()
    {
        assertThatThrownBy(super::testUnregisterBrokenTable)
                .hasStackTraceContaining("unregisterTable is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testUnregisterTableNotExistingTable()
    {
        assertThatThrownBy(super::testUnregisterTableNotExistingTable)
                .hasStackTraceContaining("unregisterTable is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testRepeatUnregisterTable()
    {
        assertThatThrownBy(super::testRepeatUnregisterTable)
                .hasStackTraceContaining("unregisterTable is not supported for Iceberg Nessie catalogs");
    }

    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingMetadataFile)
                .hasMessageMatching("metadata location for register_table is not supported");
    }

    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingSnapshotFile)
                .hasMessageMatching("metadata location for register_table is not supported");
    }

    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingManifestListFile)
                .hasMessageContaining("metadata location for register_table is not supported");
    }

    @Override
    public void testDropTableWithNonExistentTableLocation()
    {
        assertThatThrownBy(super::testDropTableWithNonExistentTableLocation)
                .hasMessageMatching("Cannot drop corrupted table (.*)");
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
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
