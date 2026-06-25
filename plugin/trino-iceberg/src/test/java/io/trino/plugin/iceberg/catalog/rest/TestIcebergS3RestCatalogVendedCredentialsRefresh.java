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
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static org.apache.iceberg.CatalogProperties.FILE_IO_IMPL;

final class TestIcebergS3RestCatalogVendedCredentialsRefresh
        extends AbstractTestIcebergRestCatalogVendedCredentialsRefresh
{
    private final String bucketName = "test-iceberg-refresh-vending-credentials-rest-" + randomNameSuffix();
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
        VendedCredentialsRestCatalogAdapter adapter = new VendedCredentialsRestCatalogAdapter(backendCatalog)
        {
            @Override
            public Map<String, String> getVendedCredentialsConfig(String restServerUri)
            {
                Credentials sessionCredentials = getSessionCredentials();
                ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                        .put(AwsClientProperties.CLIENT_REGION, FLOCI_REGION)
                        .put(S3FileIOProperties.ACCESS_KEY_ID, sessionCredentials.accessKeyId())
                        .put(S3FileIOProperties.SECRET_ACCESS_KEY, sessionCredentials.secretAccessKey())
                        .put(S3FileIOProperties.SESSION_TOKEN, sessionCredentials.sessionToken())
                        .put(AwsClientProperties.REFRESH_CREDENTIALS_ENABLED, "true")
                        .put(AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT, restServerUri + "/credentials")
                        .put(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS, Long.toString(sessionTokenExpirationTime.get().toEpochMilli()));
                return builder.buildOrThrow();
            }
        };
        return new VendedCredentialsRestCatalogServlet(adapter)
        {
            @Override
            public String getPrefix()
            {
                return "s3://";
            }

            @Override
            public Map<String, String> getCredentialsConfig()
            {
                Credentials sessionCredentials = getSessionCredentials();
                ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                        .put(AwsClientProperties.CLIENT_REGION, FLOCI_REGION)
                        .put(S3FileIOProperties.ACCESS_KEY_ID, sessionCredentials.accessKeyId())
                        .put(S3FileIOProperties.SECRET_ACCESS_KEY, sessionCredentials.secretAccessKey())
                        .put(S3FileIOProperties.SESSION_TOKEN, sessionCredentials.sessionToken())
                        .put(AwsClientProperties.REFRESH_CREDENTIALS_ENABLED, "true")
                        .put(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS, Long.toString(sessionCredentials.expiration().toEpochMilli()));
                return builder.buildOrThrow();
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
