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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.containers.FlociGcp;
import io.trino.testing.containers.IcebergGcsRestCatalogBackendContainer;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergTestUtils.checkOrcFileSorting;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.FlociGcp.FLOCI_GCP_PROJECT_ID;
import static java.lang.String.format;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergGcsVendingRestCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final Logger LOG = Logger.get(TestIcebergGcsVendingRestCatalogConnectorSmokeTest.class);
    private static final String OAUTH_TOKEN = "test-oauth-token";

    private final String bucketName = "test-iceberg-gcs-vending-rest-" + randomNameSuffix();

    private String warehouseLocation;
    private FlociGcp flociGcp;
    private GcsFileSystemFactory flociFileSystemFactory;
    private IcebergGcsRestCatalogBackendContainer restCatalog;

    public TestIcebergGcsVendingRestCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
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
        Network network = closeAfterClass(Network.newNetwork());
        flociGcp = closeAfterClass(new FlociGcp().withNetwork(network));
        flociGcp.start();

        warehouseLocation = "gs://%s/gcs-vending-rest-test-%s/".formatted(bucketName, randomNameSuffix());
        GcsFileSystemConfig config = new GcsFileSystemConfig()
                .setEndpoint(Optional.of(flociGcp.getEndpoint().toString()))
                .setProjectId(FLOCI_GCP_PROJECT_ID);
        GcsStorageFactory storageFactory = new GcsStorageFactory(
                config,
                (builder, _) -> builder.setCredentials(GoogleCredentials.create(new AccessToken(OAUTH_TOKEN, null))));
        storageFactory.create(SESSION.getIdentity()).create(BucketInfo.of(bucketName));
        flociFileSystemFactory = new GcsFileSystemFactory(config, storageFactory);

        restCatalog = closeAfterClass(new IcebergGcsRestCatalogBackendContainer(
                Optional.of(network),
                warehouseLocation,
                FLOCI_GCP_PROJECT_ID,
                flociGcp.getContainerEndpoint().toString(),
                OAUTH_TOKEN));
        restCatalog.start();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", restCatalog.catalogUri())
                                .put("iceberg.rest-catalog.vended-credentials-enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .put("fs.gcs.enabled", "true")
                                .put("gcs.auth-type", "APPLICATION_DEFAULT")
                                .put("gcs.endpoint", flociGcp.getEndpoint().toString())
                                .put("gcs.project-id", FLOCI_GCP_PROJECT_ID)
                                .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    @BeforeAll
    public void initFileSystem()
    {
        fileSystem = flociFileSystemFactory.create(SESSION);
    }

    @AfterAll
    public void removeTestData()
    {
        if (fileSystem == null) {
            return;
        }
        try {
            fileSystem.deleteDirectory(Location.of(warehouseLocation));
        }
        catch (IOException e) {
            LOG.warn(e, "Failed to clean up Floci test directory: %s", warehouseLocation);
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
    @Disabled("TODO: Re-enable once https://github.com/apache/iceberg/pull/15734 is merged and bumped in Trino")
    public void testDropTableWithMissingDataFile() {}

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
        try {
            fileSystem.deleteDirectory(Location.of(location));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
