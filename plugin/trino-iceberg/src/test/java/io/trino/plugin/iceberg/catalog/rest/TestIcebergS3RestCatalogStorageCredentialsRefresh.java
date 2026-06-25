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
import io.trino.testing.containers.Floci;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.util.Map;
import java.util.function.Consumer;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static org.apache.iceberg.CatalogProperties.FILE_IO_IMPL;

final class TestIcebergS3RestCatalogStorageCredentialsRefresh
        extends AbstractTestIcebergRestCatalogVendedCredentialsRefresh
{
    private final String bucketName = "test-iceberg-storage-creds-rest-" + randomNameSuffix();
    private Floci floci;

    @Override
    protected String setupStorageAndGetWarehouseLocation()
    {
        floci = closeAfterClass(new Floci());
        floci.start();
        floci.createBucket(bucketName);
        return "s3://%s/default/".formatted(bucketName);
    }

    @Override
    protected Map<String, String> backendCatalogFileIoProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put(FILE_IO_IMPL, S3FileIO.class.getName())
                .put(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
                .put(AwsClientProperties.CLIENT_REGION, FLOCI_REGION)
                .put(S3FileIOProperties.ACCESS_KEY_ID, FLOCI_ACCESS_KEY)
                .put(S3FileIOProperties.SECRET_ACCESS_KEY, FLOCI_SECRET_KEY)
                .put(S3FileIOProperties.ENDPOINT, floci.endpoint().toString())
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
                    Credentials sessionCredentials = getSessionCredentials();
                    return responseType.cast(LoadTableResponse.builder()
                            .withTableMetadata(loadTableResponse.tableMetadata())
                            // Credentials are returned as storage credentials in the credentials list, not as flat config
                            .addCredential(ImmutableCredential.builder()
                                    .prefix("s3://" + bucketName + "/")
                                    .putAllConfig(ImmutableMap.<String, String>builder()
                                            .put(AwsClientProperties.CLIENT_REGION, FLOCI_REGION)
                                            .put(S3FileIOProperties.ACCESS_KEY_ID, sessionCredentials.accessKeyId())
                                            .put(S3FileIOProperties.SECRET_ACCESS_KEY, sessionCredentials.secretAccessKey())
                                            .put(S3FileIOProperties.SESSION_TOKEN, sessionCredentials.sessionToken())
                                            .put(AwsClientProperties.REFRESH_CREDENTIALS_ENABLED, "true")
                                            .put(AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT, restServerUri + "/credentials")
                                            .put(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS, Long.toString(sessionTokenExpirationTime.get().toEpochMilli()))
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
                return "s3://" + bucketName + "/";
            }

            @Override
            public Map<String, String> getCredentialsConfig()
            {
                Credentials sessionCredentials = getSessionCredentials();
                return ImmutableMap.<String, String>builder()
                        .put(AwsClientProperties.CLIENT_REGION, FLOCI_REGION)
                        .put(S3FileIOProperties.ACCESS_KEY_ID, sessionCredentials.accessKeyId())
                        .put(S3FileIOProperties.SECRET_ACCESS_KEY, sessionCredentials.secretAccessKey())
                        .put(S3FileIOProperties.SESSION_TOKEN, sessionCredentials.sessionToken())
                        .put(AwsClientProperties.REFRESH_CREDENTIALS_ENABLED, "true")
                        .put(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS, Long.toString(sessionCredentials.expiration().toEpochMilli()))
                        .buildOrThrow();
            }
        };
    }

    @Override
    protected Map<String, String> catalogFileSystemProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("fs.s3.enabled", "true")
                .put("s3.endpoint", floci.endpoint().toString())
                .put("s3.region", FLOCI_REGION)
                .put("s3.path-style-access", "true")
                .buildOrThrow();
    }

    public Credentials getSessionCredentials()
    {
        try (StsClient stsClient = StsClient.builder().applyMutation(floci::updateClient).build()) {
            AssumeRoleResponse assumeRoleResponse = stsClient.assumeRole(AssumeRoleRequest.builder()
                    .roleArn("arn:aws:iam::000000000000:role/test")
                    .roleSessionName("test")
                    .build());
            return assumeRoleResponse.credentials();
        }
    }
}
