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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.server.HttpConfig;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.ServerFeature;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.trino.filesystem.Location;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsServiceAccountAuth;
import io.trino.filesystem.gcs.GcsServiceAccountAuthConfig;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryFailedException;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.QuotedETagRestCatalogServlet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergGcsVendingRestCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private static final Logger LOG = Logger.get(TestIcebergGcsVendingRestCatalogConnectorSmokeTest.class);

    private final String gcpCredentialKey;
    private final String warehouseLocation;

    private JdbcCatalog backend;

    public TestIcebergGcsVendingRestCatalogConnectorSmokeTest()
    {
        super(new IcebergConfig().getFileFormat().toIceberg());
        gcpCredentialKey = requiredNonEmptySystemProperty("testing.gcp-credentials-key");
        String gcpStorageBucket = requiredNonEmptySystemProperty("testing.gcp-storage-bucket");
        warehouseLocation = "gs://%s/gcs-vending-rest-test-%s/".formatted(gcpStorageBucket, randomNameSuffix());
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
        byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
        String gcpCredentials = new String(jsonKeyBytes, UTF_8);

        GoogleCredentials credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(jsonKeyBytes))
                .createScoped("https://www.googleapis.com/auth/cloud-platform");
        AccessToken accessToken = credentials.refreshAccessToken();
        String oauthToken = accessToken.getTokenValue();
        long tokenExpiresAtMs = accessToken.getExpirationTime().getTime();

        JsonMapper mapper = new JsonMapper();
        JsonNode jsonKey = mapper.readTree(gcpCredentials);
        String gcpProjectId = jsonKey.get("project_id").asText();

        backend = closeAfterClass(buildBackendCatalog(gcpProjectId, oauthToken, tokenExpiresAtMs));

        VendedCredentialsRestCatalogAdapter adapter = new VendedCredentialsRestCatalogAdapter(backend)
        {
            @Override
            public Map<String, String> getVendedCredentialsConfig(String restServerUri)
            {
                return ImmutableMap.<String, String>builder()
                        .put(GCPProperties.GCS_OAUTH2_TOKEN, oauthToken)
                        .put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(tokenExpiresAtMs))
                        .buildOrThrow();
            }
        };

        QuotedETagRestCatalogServlet servlet = new QuotedETagRestCatalogServlet(adapter);

        NodeInfo nodeInfo = new NodeInfo("test");
        HttpServerConfig config = new HttpServerConfig()
                .setHttpEnabled(true);
        HttpServerInfo httpServerInfo = new HttpServerInfo(config, Optional.of(new HttpConfig().setHttpPort(0)), Optional.empty(), nodeInfo);
        TestingHttpServer testServer = new TestingHttpServer("rest-catalog", httpServerInfo, nodeInfo, config, servlet, ServerFeature.builder()
                .withLegacyUriCompliance(true)
                .build());
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.file-format", format.name())
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .put("iceberg.rest-catalog.vended-credentials-enabled", "true")
                                .put("iceberg.writer-sort-buffer-size", "1MB")
                                .put("fs.gcs.enabled", "true")
                                .put("gcs.auth-type", "APPLICATION_DEFAULT")
                                .buildOrThrow())
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    private JdbcCatalog buildBackendCatalog(String gcpProjectId, String oauthToken, long tokenExpiresAtMs)
            throws IOException
    {
        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("backend_jdbc", ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, "jdbc:h2:file:" + Files.createTempFile(null, null).toAbsolutePath())
                .put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation)
                .put(CatalogProperties.FILE_IO_IMPL, GCSFileIO.class.getName())
                .put(JdbcCatalog.PROPERTY_PREFIX + "username", "user")
                .put(JdbcCatalog.PROPERTY_PREFIX + "password", "password")
                .put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1")
                .put(GCPProperties.GCS_PROJECT_ID, gcpProjectId)
                .put(GCPProperties.GCS_OAUTH2_TOKEN, oauthToken)
                .put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(tokenExpiresAtMs))
                .buildOrThrow());
        return catalog;
    }

    @Override
    @BeforeAll
    public void initFileSystem()
    {
        byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
        GcsFileSystemConfig config = new GcsFileSystemConfig();
        GcsServiceAccountAuthConfig authConfig = new GcsServiceAccountAuthConfig().setJsonKey(new String(jsonKeyBytes, UTF_8));
        GcsStorageFactory storageFactory;
        try {
            storageFactory = new GcsStorageFactory(config, new GcsServiceAccountAuth(authConfig));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        fileSystem = new GcsFileSystemFactory(config, storageFactory).create(SESSION);
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
            // The GCS bucket should be configured to expire objects automatically. Clean up issues do not need to fail the test.
            LOG.warn(e, "Failed to clean up GCS test directory: %s", warehouseLocation);
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
        TableIdentifier identifier = TableIdentifier.of(getSession().getSchema().orElseThrow(), tableName);
        return ((BaseTable) backend.loadTable(identifier)).operations().current().metadataFileLocation();
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
                .hasMessageContaining("Cannot drop corrupted table");
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
                .hasMessageContaining("Cannot drop corrupted table");
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
