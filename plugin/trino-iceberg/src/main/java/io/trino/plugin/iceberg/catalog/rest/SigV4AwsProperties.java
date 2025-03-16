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
import com.google.inject.Inject;
import io.trino.filesystem.s3.S3FileSystemConfig;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.catalog.rest.SigV4AwsCredentialProvider.AWS_IAM_ROLE;
import static io.trino.plugin.iceberg.catalog.rest.SigV4AwsCredentialProvider.AWS_IAM_ROLE_SESSION_NAME;
import static io.trino.plugin.iceberg.catalog.rest.SigV4AwsCredentialProvider.AWS_ROLE_EXTERNAL_ID;
import static io.trino.plugin.iceberg.catalog.rest.SigV4AwsCredentialProvider.AWS_STS_ACCESS_KEY_ID;
import static io.trino.plugin.iceberg.catalog.rest.SigV4AwsCredentialProvider.AWS_STS_ENDPOINT;
import static io.trino.plugin.iceberg.catalog.rest.SigV4AwsCredentialProvider.AWS_STS_REGION;
import static io.trino.plugin.iceberg.catalog.rest.SigV4AwsCredentialProvider.AWS_STS_SECRET_ACCESS_KEY;
import static io.trino.plugin.iceberg.catalog.rest.SigV4AwsCredentialProvider.AWS_STS_SIGNER_REGION;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.aws.AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER;
import static org.apache.iceberg.aws.AwsProperties.REST_ACCESS_KEY_ID;
import static org.apache.iceberg.aws.AwsProperties.REST_SECRET_ACCESS_KEY;
import static org.apache.iceberg.aws.AwsProperties.REST_SIGNER_REGION;
import static org.apache.iceberg.aws.AwsProperties.REST_SIGNING_NAME;

public class SigV4AwsProperties
        implements AwsProperties
{
    // Copy of `org.apache.iceberg.aws.AwsClientProperties.CLIENT_CREDENTIAL_PROVIDER_PREFIX` https://github.com/apache/iceberg/blob/ab6fc83ec0269736355a0a89c51e44e822264da8/aws/src/main/java/org/apache/iceberg/aws/AwsClientProperties.java#L69
    private static final String CLIENT_CREDENTIAL_PROVIDER_PREFIX = "client.credentials-provider.";

    private static final String CLIENT_CREDENTIAL_AWS_ACCESS_KEY_ID = CLIENT_CREDENTIAL_PROVIDER_PREFIX + AWS_STS_ACCESS_KEY_ID;
    private static final String CLIENT_CREDENTIAL_AWS_SECRET_ACCESS_KEY = CLIENT_CREDENTIAL_PROVIDER_PREFIX + AWS_STS_SECRET_ACCESS_KEY;
    private static final String CLIENT_CREDENTIAL_AWS_SIGNER_REGION = CLIENT_CREDENTIAL_PROVIDER_PREFIX + AWS_STS_SIGNER_REGION;

    private static final String CLIENT_CREDENTIAL_AWS_STS_REGION = CLIENT_CREDENTIAL_PROVIDER_PREFIX + AWS_STS_REGION;
    private static final String CLIENT_CREDENTIAL_AWS_STS_ENDPOINT = CLIENT_CREDENTIAL_PROVIDER_PREFIX + AWS_STS_ENDPOINT;
    private static final String CLIENT_CREDENTIAL_AWS_IAM_ROLE = CLIENT_CREDENTIAL_PROVIDER_PREFIX + AWS_IAM_ROLE;
    private static final String CLIENT_CREDENTIAL_AWS_ROLE_EXTERNAL_ID = CLIENT_CREDENTIAL_PROVIDER_PREFIX + AWS_ROLE_EXTERNAL_ID;
    private static final String CLIENT_CREDENTIAL_AWS_IAM_ROLE_SESSION_NAME = CLIENT_CREDENTIAL_PROVIDER_PREFIX + AWS_IAM_ROLE_SESSION_NAME;

    private final Map<String, String> properties;

    @Inject
    public SigV4AwsProperties(IcebergRestCatalogSigV4Config sigV4Config, S3FileSystemConfig s3Config)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
                .put("rest.auth.type", "sigv4")
                .put(REST_SIGNING_NAME, sigV4Config.getSigningName())
                .put(REST_SIGNER_REGION, requireNonNull(s3Config.getRegion(), "s3.region is null"))
                .put("rest-metrics-reporting-enabled", "false");

        if (s3Config.getIamRole() != null) {
            builder
                    .put(CLIENT_CREDENTIALS_PROVIDER, SigV4AwsCredentialProvider.class.getName())
                    .put(CLIENT_CREDENTIAL_AWS_IAM_ROLE, s3Config.getIamRole())
                    .put(CLIENT_CREDENTIAL_AWS_IAM_ROLE_SESSION_NAME, "trino-iceberg-rest-catalog")
                    .put(CLIENT_CREDENTIAL_AWS_SIGNER_REGION, s3Config.getRegion());
            Optional.ofNullable(s3Config.getExternalId()).ifPresent(externalId -> builder.put(CLIENT_CREDENTIAL_AWS_ROLE_EXTERNAL_ID, externalId));

            Optional.ofNullable(s3Config.getStsRegion()).ifPresent(stsRegion -> builder.put(CLIENT_CREDENTIAL_AWS_STS_REGION, stsRegion));
            Optional.ofNullable(s3Config.getAwsAccessKey()).ifPresent(accessKey -> builder.put(CLIENT_CREDENTIAL_AWS_ACCESS_KEY_ID, accessKey));
            Optional.ofNullable(s3Config.getAwsSecretKey()).ifPresent(secretAccessKey -> builder.put(CLIENT_CREDENTIAL_AWS_SECRET_ACCESS_KEY, secretAccessKey));
            Optional.ofNullable(s3Config.getStsEndpoint()).ifPresent(endpoint -> builder.put(CLIENT_CREDENTIAL_AWS_STS_ENDPOINT, endpoint));
        }
        else {
            builder
                    .put(REST_ACCESS_KEY_ID, requireNonNull(s3Config.getAwsAccessKey(), "s3.aws-access-key is null"))
                    .put(REST_SECRET_ACCESS_KEY, requireNonNull(s3Config.getAwsSecretKey(), "s3.aws-secret-key is null"));
        }

        properties = builder.buildOrThrow();
    }

    @Override
    public Map<String, String> get()
    {
        return properties;
    }
}
