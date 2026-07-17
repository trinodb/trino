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
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsServiceAccountAuth;
import io.trino.filesystem.gcs.GcsServiceAccountAuthConfig;
import io.trino.filesystem.gcs.GcsStorageFactory;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.function.Consumer;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.CatalogProperties.FILE_IO_IMPL;

final class TestIcebergGcsRestCatalogStorageCredentialsRefresh
        extends AbstractTestIcebergRestCatalogVendedCredentialsRefresh
{
    private static final Logger LOG = Logger.get(TestIcebergGcsRestCatalogStorageCredentialsRefresh.class);

    private final String gcpCredentialKey = requiredNonEmptySystemProperty("testing.gcp-credentials-key");
    private final String gcpStorageBucket = requiredNonEmptySystemProperty("testing.gcp-storage-bucket");

    private String oauthToken;
    private long tokenExpiresAtMs;
    private String gcpProjectId;
    private TrinoFileSystem fileSystem;

    @BeforeAll
    public void initFileSystem()
            throws Exception
    {
        byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
        GcsFileSystemConfig config = new GcsFileSystemConfig();
        GcsServiceAccountAuthConfig authConfig = new GcsServiceAccountAuthConfig().setJsonKey(new String(jsonKeyBytes, UTF_8));
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, new GcsServiceAccountAuth(authConfig));
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

    @Override
    protected String setupStorageAndGetWarehouseLocation()
            throws Exception
    {
        byte[] jsonKeyBytes = Base64.getDecoder().decode(gcpCredentialKey);
        String gcpCredentials = new String(jsonKeyBytes, UTF_8);

        GoogleCredentials credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(jsonKeyBytes))
                .createScoped("https://www.googleapis.com/auth/cloud-platform");
        AccessToken accessToken = credentials.refreshAccessToken();
        oauthToken = accessToken.getTokenValue();
        tokenExpiresAtMs = accessToken.getExpirationTime().getTime();

        JsonMapper mapper = new JsonMapper();
        JsonNode jsonKey = mapper.readTree(gcpCredentials);
        gcpProjectId = jsonKey.get("project_id").asText();

        return "gs://%s/gcs-storage-creds-rest-refresh-test-%s/".formatted(gcpStorageBucket, randomNameSuffix());
    }

    @Override
    protected Map<String, String> backendCatalogFileIoProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put(FILE_IO_IMPL, GCSFileIO.class.getName())
                .put(GCPProperties.GCS_PROJECT_ID, gcpProjectId)
                .put(GCPProperties.GCS_OAUTH2_TOKEN, oauthToken)
                .put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(tokenExpiresAtMs))
                .buildOrThrow();
    }

    @Override
    protected VendedCredentialsRestCatalogServlet createServlet(Catalog backendCatalog)
    {
        RESTCatalogAdapter adapter = new RESTCatalogAdapter(backendCatalog)
        {
            @Override
            protected <T extends RESTResponse> T execute(
                    HTTPRequest request,
                    Class<T> responseType,
                    Consumer<ErrorResponse> errorHandler,
                    Consumer<Map<String, String>> responseHeaders)
            {
                T response = super.execute(request, responseType, errorHandler, responseHeaders);
                if (response instanceof LoadTableResponse loadTableResponse) {
                    String host = request.headers().entries("host").stream()
                            .findFirst()
                            .orElseThrow(() -> new IllegalStateException("Host header not found in request"))
                            .value();
                    String restServerUri = "http://" + host;
                    return responseType.cast(LoadTableResponse.builder()
                            .withTableMetadata(loadTableResponse.tableMetadata())
                            // Credentials are returned as storage credentials in the credentials list, not as flat config
                            .addCredential(ImmutableCredential.builder()
                                    .prefix("gs://" + gcpStorageBucket + "/")
                                    .putAllConfig(ImmutableMap.<String, String>builder()
                                            .put(GCPProperties.GCS_OAUTH2_TOKEN, oauthToken)
                                            .put(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED, "true")
                                            .put(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT, restServerUri + "/credentials")
                                            .put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(sessionTokenExpirationTime.get().toEpochMilli()))
                                            .buildOrThrow())
                                    .build())
                            .build());
                }
                return response;
            }
        };
        return new VendedCredentialsRestCatalogServlet(adapter)
        {
            @Override
            public String getPrefix()
            {
                return "gs://" + gcpStorageBucket + "/";
            }

            @Override
            public Map<String, String> getCredentialsConfig()
            {
                return ImmutableMap.<String, String>builder()
                        .put(GCPProperties.GCS_OAUTH2_TOKEN, oauthToken)
                        .put(GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED, "true")
                        .put(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(tokenExpiresAtMs))
                        .buildOrThrow();
            }
        };
    }

    @Override
    protected Map<String, String> catalogFileSystemProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.gcs.enabled", "true")
                .put("gcs.auth-type", "APPLICATION_DEFAULT")
                .buildOrThrow();
    }
}
