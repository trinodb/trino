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

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.sas.AccountSasPermission;
import com.azure.storage.common.sas.AccountSasResourceType;
import com.azure.storage.common.sas.AccountSasService;
import com.azure.storage.common.sas.AccountSasSignatureValues;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.OffsetDateTime;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergAbfsVendingRestCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final Logger LOG = Logger.get(TestIcebergAbfsVendingRestCatalogConnectorSmokeTest.class);

    private final String account;
    private final String accessKey;
    private final String warehouseLocation;

    private IcebergAzureRestCatalogBackendContainer restCatalog;

    public TestIcebergAbfsVendingRestCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
        String container = requiredNonEmptySystemProperty("testing.azure-abfs-container");
        account = requiredNonEmptySystemProperty("testing.azure-abfs-account");
        accessKey = requiredNonEmptySystemProperty("testing.azure-abfs-access-key");
        warehouseLocation = "abfss://%s@%s.dfs.core.windows.net/azure-vending-rest-test-%s/".formatted(container, account, randomNameSuffix());
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
        OffsetDateTime sasTokenExpiry = OffsetDateTime.now().plusHours(1);
        String sasToken = generateAccountSasToken(sasTokenExpiry);

        restCatalog = closeAfterClass(new IcebergAzureRestCatalogBackendContainer(
                warehouseLocation,
                account,
                accessKey,
                sasToken,
                sasTokenExpiry.toInstant().toEpochMilli()));
        restCatalog.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", restCatalog.catalogUri())
                                .put("iceberg.rest-catalog.vended-credentials-enabled", "true")
                                .put("iceberg.rest-catalog.socket-timeout", "30s")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .put("fs.azure.enabled", "true")
                                .put("azure.auth-type", "DEFAULT")
                                .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    private String generateAccountSasToken(OffsetDateTime expiryTime)
    {
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(account, accessKey);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint("https://%s.blob.core.windows.net".formatted(account))
                .credential(credential)
                .buildClient();

        AccountSasPermission permissions = new AccountSasPermission()
                .setReadPermission(true)
                .setWritePermission(true)
                .setDeletePermission(true)
                .setListPermission(true)
                .setCreatePermission(true);

        AccountSasResourceType resourceTypes = new AccountSasResourceType()
                .setContainer(true)
                .setObject(true)
                .setService(true);

        AccountSasService services = new AccountSasService()
                .setBlobAccess(true);

        AccountSasSignatureValues sasValues = new AccountSasSignatureValues(
                expiryTime,
                permissions,
                services,
                resourceTypes);

        return blobServiceClient.generateAccountSas(sasValues);
    }

    @Override
    @BeforeAll
    public void initFileSystem()
    {
        AzureAuthAccessKey azureAuth = new AzureAuthAccessKey(accessKey);
        fileSystem = new AzureFileSystemFactory(
                OpenTelemetry.noop(),
                azureAuth,
                new AzureFileSystemConfig())
                .create(SESSION);
    }

    @AfterAll
    public void removeTestData()
    {
        if (fileSystem == null) {
            return;
        }
        try {
            fileSystem.deleteDirectory(Location.of(stripTrailingSlash(warehouseLocation)));
        }
        catch (IOException e) {
            LOG.warn(e, "Failed to clean up Azure test directory: %s", warehouseLocation);
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

    @Override
    protected void dropTableFromCatalog(String tableName)
    {
        // TODO: Get register table tests working
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        try (RESTSessionCatalog catalog = new RESTSessionCatalog()) {
            catalog.initialize("rest-catalog", ImmutableMap.of(CatalogProperties.URI, restCatalog.catalogUri()));
            SessionCatalog.SessionContext context = new SessionCatalog.SessionContext(
                    "user-default",
                    "user",
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    SESSION.getIdentity());
            TableIdentifier identifier = TableIdentifier.of(getSession().getSchema().orElseThrow(), tableName);
            return ((BaseTable) catalog.loadTable(context, identifier)).operations().current().metadataFileLocation();
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
        try {
            return fileSystem.newInputFile(Location.of(location)).exists();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
    @Disabled("TODO: Re-enable once https://github.com/apache/iceberg/issues/15760 is fixed bumped in Trino")
    public void testDropTableWithMissingMetadataFile() {}

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
    @Disabled("TODO: Re-enable once https://github.com/apache/iceberg/issues/15760 is fixed and bumped in Trino")
    public void testDropTableWithMissingDataFile() {}

    @Test
    @Override
    @Disabled("TODO: Re-enable once https://github.com/apache/iceberg/issues/15760 is fixed and bumped in Trino")
    public void testDropTableWithNonExistentTableLocation() {}

    @Test
    @Override
    public void testDropTableWithMissingManifestListFile()
    {
        assertThatThrownBy(super::testDropTableWithMissingManifestListFile)
                .hasMessageContaining("Table location should not exist");
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
            fileSystem.deleteDirectory(Location.of(location));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
