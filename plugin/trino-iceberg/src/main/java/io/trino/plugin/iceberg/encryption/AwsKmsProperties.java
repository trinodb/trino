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
package io.trino.plugin.iceberg.encryption;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.plugin.base.encryption.AwsKmsConfig;
import io.trino.plugin.iceberg.StsAwsCredentialProvider;

import java.util.Map;

import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_IAM_ROLE;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_IAM_ROLE_SESSION_NAME;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_ROLE_EXTERNAL_ID;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_STS_ACCESS_KEY_ID;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_STS_ENDPOINT;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_STS_REGION;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_STS_SECRET_ACCESS_KEY;
import static io.trino.plugin.iceberg.StsAwsCredentialProvider.AWS_STS_SIGNER_REGION;
import static org.apache.iceberg.CatalogProperties.ENCRYPTION_KMS_TYPE;
import static org.apache.iceberg.CatalogProperties.ENCRYPTION_KMS_TYPE_AWS;
import static org.apache.iceberg.aws.AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER;
import static org.apache.iceberg.aws.AwsClientProperties.CLIENT_REGION;
import static org.apache.iceberg.aws.AwsProperties.KMS_ENDPOINT;

public class AwsKmsProperties
        implements KmsProperties
{
    // Copy of `org.apache.iceberg.aws.AwsClientProperties.CLIENT_CREDENTIAL_PROVIDER_PREFIX` https://github.com/apache/iceberg/blob/ab6fc83ec0269736355a0a89c51e44e822264da8/aws/src/main/java/org/apache/iceberg/aws/AwsClientProperties.java#L69
    private static final String PREFIX = "client.credentials-provider.";

    private final Map<String, String> properties;

    @Inject
    public AwsKmsProperties(AwsKmsConfig config)
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        builder.put(ENCRYPTION_KMS_TYPE, ENCRYPTION_KMS_TYPE_AWS);
        config.getEndpoint().ifPresent(endpoint -> builder.put(KMS_ENDPOINT, endpoint.toString()));
        config.getRegion().ifPresent(region -> builder.put(CLIENT_REGION, region));

        if (config.getIamRole().isPresent()) {
            builder.put(CLIENT_CREDENTIALS_PROVIDER, StsAwsCredentialProvider.class.getName());
            builder.put(PREFIX + AWS_IAM_ROLE, config.getIamRole().get());
            builder.put(PREFIX + AWS_IAM_ROLE_SESSION_NAME, "trino-iceberg-encryption");
            config.getRegion().ifPresent(region -> builder.put(PREFIX + AWS_STS_SIGNER_REGION, region));
            config.getExternalId().ifPresent(externalId -> builder.put(PREFIX + AWS_ROLE_EXTERNAL_ID, externalId));
            config.getStsRegion().ifPresent(stsRegion -> builder.put(PREFIX + AWS_STS_REGION, stsRegion));
            config.getStsEndpoint().ifPresent(endpoint -> builder.put(PREFIX + AWS_STS_ENDPOINT, endpoint.toString()));
            config.getAccessKey().ifPresent(key -> builder.put(PREFIX + AWS_STS_ACCESS_KEY_ID, key));
            config.getSecretKey().ifPresent(secret -> builder.put(PREFIX + AWS_STS_SECRET_ACCESS_KEY, secret));
        }
        else if (config.getAccessKey().isPresent() && config.getSecretKey().isPresent()) {
            builder.put(CLIENT_CREDENTIALS_PROVIDER, StaticAwsCredentialsProvider.class.getName());
            config.getAccessKey().ifPresent(key -> builder.put(PREFIX + "access-key-id", key));
            config.getSecretKey().ifPresent(secret -> builder.put(PREFIX + "secret-access-key", secret));
        }

        properties = builder.buildOrThrow();
    }

    @Override
    public Map<String, String> get()
    {
        return properties;
    }
}
