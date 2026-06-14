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
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsServiceAccountAuth;
import io.trino.filesystem.gcs.GcsServiceAccountAuthConfig;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link GcsFileSystem} transparently encrypts and decrypts
 * objects when the Iceberg REST catalog vends a customer-supplied encryption
 * key via the {@code gcs.encryption-key}/{@code gcs.decryption-key} properties.
 */
class TestIcebergGcsVendingRestCatalogCustomerSuppliedEncryptionKey
        extends AbstractTestQueryFramework
{
    private static final Logger LOG = Logger.get(TestIcebergGcsVendingRestCatalogCustomerSuppliedEncryptionKey.class);

    // 32-byte AES-256 customer-supplied encryption key encoded in Base64
    private static final String BASE64_ENCRYPTION_KEY = randomBase64AesKey();

    private static String randomBase64AesKey()
    {
        byte[] key = new byte[32];
        new SecureRandom().nextBytes(key);
        return Base64.getEncoder().encodeToString(key);
    }

    private final String gcpCredentialKey;
    private final String warehouseLocation;

    private GcsFileSystemFactory gcsFileSystemFactory;

    TestIcebergGcsVendingRestCatalogCustomerSuppliedEncryptionKey()
    {
        gcpCredentialKey = requiredNonEmptySystemProperty("testing.gcp-credentials-key");
        String gcpStorageBucket = requiredNonEmptySystemProperty("testing.gcp-storage-bucket");
        warehouseLocation = "gs://%s/gcs-vending-rest-encryption-%s".formatted(gcpStorageBucket, randomNameSuffix());
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

        JsonNode jsonKey = new JsonMapper().readTree(gcpCredentials);
        String gcpProjectId = jsonKey.get("project_id").asText();

        GcsFileSystemConfig fsConfig = new GcsFileSystemConfig();
        GcsServiceAccountAuthConfig authConfig = new GcsServiceAccountAuthConfig().setJsonKey(gcpCredentials);
        GcsStorageFactory storageFactory = new GcsStorageFactory(fsConfig, new GcsServiceAccountAuth(authConfig));
        gcsFileSystemFactory = new GcsFileSystemFactory(fsConfig, storageFactory);
        // register factory shutdown first so it runs last, after cleanup below
        closeAfterClass(gcsFileSystemFactory::stop);
        closeAfterClass(this::cleanupWarehouse);

        JdbcCatalog backend = closeAfterClass(createGcsBackendCatalog(gcpProjectId, accessToken));

        Map<String, String> vendedConfig = ImmutableMap.of(
                GCPProperties.GCS_ENCRYPTION_KEY, BASE64_ENCRYPTION_KEY,
                GCPProperties.GCS_DECRYPTION_KEY, BASE64_ENCRYPTION_KEY);
        TestingHttpServer testServer = RestCatalogTestingHttpServers.create(
                new RESTCatalogServlet(new VendingRESTCatalogAdapter(backend, vendedConfig)));
        testServer.start();
        closeAfterClass(testServer::stop);

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "rest")
                                .put("iceberg.rest-catalog.uri", testServer.getBaseUrl().toString())
                                .put("iceberg.rest-catalog.vended-credentials-enabled", "true")
                                .put("fs.native-gcs.enabled", "true")
                                .put("gcs.json-key", gcpCredentials)
                                .buildOrThrow())
                .build();
    }

    private void cleanupWarehouse()
    {
        try {
            gcsFileSystemFactory.create(ConnectorIdentity.ofUser("test"))
                    .deleteDirectory(Location.of(warehouseLocation));
        }
        catch (IOException e) {
            // The GCS bucket should be configured to expire objects automatically;
            // leaked objects from the test do not need to fail the teardown.
            LOG.warn(e, "Failed to clean up GCS test directory: %s", warehouseLocation);
        }
    }

    private JdbcCatalog createGcsBackendCatalog(String gcpProjectId, AccessToken accessToken)
            throws IOException
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, "jdbc:h2:file:" + Files.createTempFile(null, null).toAbsolutePath())
                .put(JdbcCatalog.PROPERTY_PREFIX + "username", "user")
                .put(JdbcCatalog.PROPERTY_PREFIX + "password", "password")
                .put(JdbcCatalog.PROPERTY_PREFIX + "schema-version", "V1")
                .put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation)
                .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.gcp.gcs.GCSFileIO")
                .put(GCPProperties.GCS_OAUTH2_TOKEN, accessToken.getTokenValue())
                .put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(accessToken.getExpirationTime().getTime()))
                .put(GCPProperties.GCS_PROJECT_ID, gcpProjectId)
                .put(GCPProperties.GCS_ENCRYPTION_KEY, BASE64_ENCRYPTION_KEY)
                .put(GCPProperties.GCS_DECRYPTION_KEY, BASE64_ENCRYPTION_KEY)
                .buildOrThrow();

        JdbcCatalog catalog = new JdbcCatalog();
        catalog.initialize("backend_jdbc", properties);
        return catalog;
    }

    @Test
    void testWriteAndReadWithVendedEncryptionKey()
    {
        String tableName = "test_gcs_csek_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x, VARCHAR 'hello' y", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'hello')");

            assertUpdate("INSERT INTO " + tableName + " VALUES (2, 'world')", 1);
            assertQuery("SELECT * FROM " + tableName + " ORDER BY x", "VALUES (1, 'hello'), (2, 'world')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    void testDataFilesAreEncrypted()
            throws IOException
    {
        String tableName = "test_gcs_csek_encrypted_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 x", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES 1");

            String dataFilePath = (String) computeScalar(
                    "SELECT file_path FROM \"" + tableName + "$files\" LIMIT 1");
            assertThat(dataFilePath).startsWith("gs://");

            // Reading the object content without the encryption key must fail,
            // proving the data is actually encrypted on storage.
            // Note: GCS allows HEAD/metadata operations without the key, so we must
            // attempt to read content (not just length) to exercise the decryption path.
            TrinoFileSystem plainFileSystem = gcsFileSystemFactory.create(ConnectorIdentity.ofUser("test"));
            TrinoInputFile plainInputFile = plainFileSystem.newInputFile(Location.of(dataFilePath));
            assertThatThrownBy(() -> {
                try (InputStream stream = plainInputFile.newStream()) {
                    stream.readAllBytes();
                }
            }).isInstanceOf(IOException.class);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }
}
