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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.containers.FlociGcp;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.gcp.gcs.GCSFileIO;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

import java.util.Map;
import java.util.function.Consumer;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.FlociGcp.FLOCI_GCP_PROJECT_ID;
import static org.apache.iceberg.CatalogProperties.FILE_IO_IMPL;

final class TestIcebergGcsRestCatalogStorageCredentialsRefresh
        extends AbstractTestIcebergRestCatalogVendedCredentialsRefresh
{
    private final String gcpStorageBucket = "test-iceberg-gcs-storage-credentials-refresh-" + randomNameSuffix();

    private final String oauthToken = "test-oauth-token";
    private final long tokenExpiresAtMs = sessionTokenExpirationTime.get().toEpochMilli();
    private FlociGcp flociGcp;

    @Override
    protected String setupStorageAndGetWarehouseLocation()
            throws Exception
    {
        flociGcp = closeAfterClass(new FlociGcp());
        flociGcp.start();
        flociGcp.createBucket(gcpStorageBucket);

        return "gs://%s/gcs-storage-creds-rest-refresh-test-%s/".formatted(gcpStorageBucket, randomNameSuffix());
    }

    @Override
    protected Map<String, String> backendCatalogFileIoProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put(FILE_IO_IMPL, GCSFileIO.class.getName())
                .put(GCPProperties.GCS_PROJECT_ID, FLOCI_GCP_PROJECT_ID)
                .put(GCPProperties.GCS_SERVICE_HOST, flociGcp.getEndpoint().toString())
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
                .put("gcs.endpoint", flociGcp.getEndpoint().toString())
                .put("gcs.project-id", FLOCI_GCP_PROJECT_ID)
                .buildOrThrow();
    }
}
