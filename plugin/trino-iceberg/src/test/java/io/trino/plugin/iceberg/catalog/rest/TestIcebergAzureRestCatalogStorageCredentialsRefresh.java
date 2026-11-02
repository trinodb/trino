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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.azure.AzureAuthAccessKey;
import io.trino.filesystem.azure.AzureFileSystemConfig;
import io.trino.filesystem.azure.AzureFileSystemFactory;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.azure.adlsv2.ADLSFileIO;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.function.Consumer;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;
import static org.apache.iceberg.CatalogProperties.FILE_IO_IMPL;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

final class TestIcebergAzureRestCatalogStorageCredentialsRefresh
        extends AbstractTestIcebergRestCatalogVendedCredentialsRefresh
{
    private static final Logger LOG = Logger.get(TestIcebergAzureRestCatalogStorageCredentialsRefresh.class);

    private final String container = requiredNonEmptySystemProperty("testing.azure-abfs-container");
    private final String account = requiredNonEmptySystemProperty("testing.azure-abfs-account");
    private final String accessKey = requiredNonEmptySystemProperty("testing.azure-abfs-access-key");
    // Vending catalogs may key SAS token properties by the account host instead of the bare account name
    private final String accountHost = account + ".dfs.core.windows.net";
    private final String containerLocation = "abfss://%s@%s/".formatted(container, accountHost);

    private String sasToken;
    private long sasTokenExpiresAtMs;
    private TrinoFileSystem fileSystem;

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

    @Override
    protected String setupStorageAndGetWarehouseLocation()
    {
        OffsetDateTime sasTokenExpiry = OffsetDateTime.now().plusHours(1);
        sasToken = generateAccountSasToken(sasTokenExpiry);
        sasTokenExpiresAtMs = sasTokenExpiry.toInstant().toEpochMilli();

        return "%sazure-storage-creds-rest-refresh-test-%s/".formatted(containerLocation, randomNameSuffix());
    }

    @Override
    protected Map<String, String> backendCatalogFileIoProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put(FILE_IO_IMPL, ADLSFileIO.class.getName())
                .put(AzureProperties.ADLS_SHARED_KEY_ACCOUNT_NAME, account)
                .put(AzureProperties.ADLS_SHARED_KEY_ACCOUNT_KEY, accessKey)
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
                                    .prefix(containerLocation)
                                    .putAllConfig(ImmutableMap.<String, String>builder()
                                            .put(AzureProperties.ADLS_SAS_TOKEN_PREFIX + accountHost, sasToken)
                                            .put(AzureProperties.ADLS_REFRESH_CREDENTIALS_ENABLED, "true")
                                            .put(AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT, restServerUri + "/credentials")
                                            .put(AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + accountHost, Long.toString(sessionTokenExpirationTime.get().toEpochMilli()))
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
                return containerLocation;
            }

            @Override
            public Map<String, String> getCredentialsConfig()
            {
                return ImmutableMap.<String, String>builder()
                        .put(AzureProperties.ADLS_SAS_TOKEN_PREFIX + accountHost, sasToken)
                        .put(AzureProperties.ADLS_REFRESH_CREDENTIALS_ENABLED, "true")
                        .put(AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + accountHost, Long.toString(sasTokenExpiresAtMs))
                        .buildOrThrow();
            }
        };
    }

    @Override
    protected Map<String, String> catalogFileSystemProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.azure.enabled", "true")
                .put("azure.auth-type", "DEFAULT")
                .buildOrThrow();
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
}
