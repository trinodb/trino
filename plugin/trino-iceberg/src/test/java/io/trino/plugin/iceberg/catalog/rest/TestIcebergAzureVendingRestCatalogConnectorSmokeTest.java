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

import com.azure.core.util.Context;
import com.azure.storage.blob.implementation.util.BlobSasImplUtil;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.azure.AzureAuthAccessKey;
import io.trino.filesystem.azure.AzureFileSystemConfig;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.containers.IcebergAzureRestCatalogBackendContainer;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// TODO: Reduce duplication
public class TestIcebergAzureVendingRestCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String directory;
    private String warehouseLocation;
    private IcebergAzureRestCatalogBackendContainer restCatalogBackendContainer;

    public TestIcebergAzureVendingRestCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
        this.directory = "test-iceberg-azure-vending-rest-connector-smoke-test-" + randomNameSuffix();
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
        Network network = Network.newNetwork();

        String container = requireEnv("ABFS_CONTAINER");
        String account = requireEnv("ABFS_ACCOUNT");
        String abfsAccessKey = requireEnv("ABFS_ACCESS_KEY");

        this.warehouseLocation = format("abfs://%s@%s.dfs.core.windows.net/%s/", container, account, directory);

        OffsetDateTime expiry = OffsetDateTime.ofInstant(Instant.now().plus(Duration.ofDays(1)), ZoneOffset.UTC);
        BlobSasPermission permission = new BlobSasPermission()
                .setReadPermission(true)
                .setAddPermission(true)
                .setCreatePermission(true)
                .setWritePermission(true)
                .setDeletePermission(true)
                .setListPermission(true);
        String sasToken = new BlobSasImplUtil(
                new BlobServiceSasSignatureValues(expiry, permission),
                container
        ).generateSas(new StorageSharedKeyCredential(account, abfsAccessKey), Context.NONE);

        restCatalogBackendContainer = closeAfterClass(new IcebergAzureRestCatalogBackendContainer(
                Optional.of(network),
                warehouseLocation,
                account,
                sasToken));

        restCatalogBackendContainer.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", "http://" + restCatalogBackendContainer.getRestCatalogEndpoint())
                                .put("iceberg.rest-catalog.vended-credentials-enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .put("iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max")
                                .put("fs.native-azure.enabled", "true")
                                .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    @BeforeAll
    public void initFileSystem()
    {
        this.fileSystem = new AzureFileSystemFactory(
                OpenTelemetry.noop(),
                new AzureAuthAccessKey(requireEnv("ABFS_ACCESS_KEY")),
                new AzureFileSystemConfig().setAuthType(AzureFileSystemConfig.AuthType.ACCESS_KEY)
        ).create(SESSION);
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

    @Override
    protected void dropTableFromMetastore(String tableName)
    {
        // TODO: Get register table tests working
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        try (RESTSessionCatalog catalog = new RESTSessionCatalog()) {
            catalog.initialize("rest-catalog", ImmutableMap.of(CatalogProperties.URI, "http://" + restCatalogBackendContainer.getRestCatalogEndpoint()));
            SessionCatalog.SessionContext context = new SessionCatalog.SessionContext(
                    "user-default",
                    "user",
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    SESSION.getIdentity());
            return ((BaseTable) catalog.loadTable(context, toIdentifier(tableName))).operations().current().metadataFileLocation();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String schemaPath()
    {
        return format("%s%s", warehouseLocation, getSession().getSchema().orElseThrow());
    }

    @Override
    protected boolean locationExists(String location)
    {
        return java.nio.file.Files.exists(Path.of(location));
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
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testRepeatUnregisterTable()
    {
        assertThatThrownBy(super::testRepeatUnregisterTable)
                .hasMessageContaining("register_table procedure is disabled");
    }

    @Test
    @Override
    public void testDropTableWithMissingMetadataFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingMetadataFile)
                .hasMessageMatching("Failed to load table: (.*)");
    }

    @Test
    @Override
    public void testDropTableWithMissingSnapshotFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingSnapshotFile)
                .isInstanceOf(QueryFailedException.class)
                .cause()
                .hasMessageContaining("Failed to drop table")
                .hasNoCause();
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
                .hasMessageMatching("Failed to load table: (.*)");
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
        // TODO: Fix method
        try {
            fileSystem.deleteDirectory(Location.of(location));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private TableIdentifier toIdentifier(String tableName)
    {
        return TableIdentifier.of(getSession().getSchema().orElseThrow(), tableName);
    }
}
