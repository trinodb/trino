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
package io.trino.plugin.iceberg;

import com.google.common.annotations.VisibleForTesting;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;
import static java.util.Objects.requireNonNull;

public class StsAwsCredentialProvider
        implements AwsCredentialsProvider
{
    public static final String AWS_STS_ACCESS_KEY_ID = "aws_sts_access_key_id";
    public static final String AWS_STS_SECRET_ACCESS_KEY = "aws_sts_secret_access_key";
    public static final String AWS_STS_SIGNER_REGION = "aws_sts_signer_region";
    public static final String AWS_STS_REGION = "aws_sts_region";
    public static final String AWS_STS_ENDPOINT = "aws_sts_endpoint";

    public static final String AWS_IAM_ROLE = "aws_iam_role";
    public static final String AWS_ROLE_EXTERNAL_ID = "aws_external_id";
    public static final String AWS_IAM_ROLE_SESSION_NAME = "aws_iam_role_session_name";

    // Iceberg REST SigV4 creates a new AuthSession (and calls create()) per catalog
    // touch. roleArn is static catalog config, so one client per key is enough.
    // Do not implement AutoCloseable: Iceberg may close per-session providers, which
    // would shut down a shared client still in use.
    private static final ConcurrentHashMap<ClientKey, StsAwsCredentialProvider> PROVIDERS = new ConcurrentHashMap<>();

    private final AwsCredentialsProvider delegate;
    private final StsClient stsClient;

    public StsAwsCredentialProvider(AwsCredentialsProvider delegate)
    {
        this(delegate, null);
    }

    private StsAwsCredentialProvider(AwsCredentialsProvider delegate, StsClient stsClient)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.stsClient = stsClient;
    }

    public static StsAwsCredentialProvider create(Map<String, String> properties)
    {
        if (!properties.containsKey(AWS_IAM_ROLE)) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "IAM role configs are not configured");
        }
        return PROVIDERS.computeIfAbsent(ClientKey.from(properties), ignored -> createUncached(properties));
    }

    private static StsAwsCredentialProvider createUncached(Map<String, String> properties)
    {
        String accessKey = properties.get(AWS_STS_ACCESS_KEY_ID);
        String secretAccessKey = properties.get(AWS_STS_SECRET_ACCESS_KEY);
        Optional<AwsCredentialsProvider> staticCredentialsProvider = createStaticCredentialsProvider(accessKey, secretAccessKey);
        StsClient stsClient = createStsClient(
                properties.get(AWS_STS_ENDPOINT),
                properties.get(AWS_STS_REGION),
                properties.get(AWS_STS_SIGNER_REGION),
                staticCredentialsProvider);
        return new StsAwsCredentialProvider(StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(request -> request
                        .roleArn(properties.get(AWS_IAM_ROLE))
                        .roleSessionName(properties.get(AWS_IAM_ROLE_SESSION_NAME))
                        .externalId(properties.get(AWS_ROLE_EXTERNAL_ID)))
                .stsClient(stsClient)
                .asyncCredentialUpdateEnabled(true)
                .build(),
                stsClient);
    }

    @Override
    public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity(Consumer<ResolveIdentityRequest.Builder> consumer)
    {
        return delegate.resolveIdentity(consumer);
    }

    @Override
    public CompletableFuture<? extends AwsCredentialsIdentity> resolveIdentity()
    {
        return delegate.resolveIdentity();
    }

    @Override
    public AwsCredentials resolveCredentials()
    {
        return delegate.resolveCredentials();
    }

    @Override
    public Class<AwsCredentialsIdentity> identityType()
    {
        return delegate.identityType();
    }

    @Override
    public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest request)
    {
        return delegate.resolveIdentity(request);
    }

    private static Optional<AwsCredentialsProvider> createStaticCredentialsProvider(String accessKey, String secretKey)
    {
        if (accessKey != null || secretKey != null) {
            return Optional.of(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)));
        }
        return Optional.empty();
    }

    private static StsClient createStsClient(String stsEndpoint, String stsRegion, String region, Optional<AwsCredentialsProvider> credentialsProvider)
    {
        StsClientBuilder sts = StsClient.builder();
        Optional.ofNullable(stsEndpoint).map(URI::create).ifPresent(sts::endpointOverride);
        Optional.ofNullable(stsRegion)
                .or(() -> Optional.ofNullable(region))
                .map(Region::of).ifPresent(sts::region);
        credentialsProvider.ifPresent(sts::credentialsProvider);
        return sts.build();
    }

    @VisibleForTesting
    StsClient stsClient()
    {
        return stsClient;
    }

    @VisibleForTesting
    static void resetCache()
    {
        PROVIDERS.clear();
    }

    private record ClientKey(
            String roleArn,
            String externalId,
            String sessionName,
            String stsEndpoint,
            String stsRegion,
            String signerRegion,
            String accessKeyId,
            String secretAccessKey)
    {
        static ClientKey from(Map<String, String> properties)
        {
            return new ClientKey(
                    properties.get(AWS_IAM_ROLE),
                    properties.get(AWS_ROLE_EXTERNAL_ID),
                    properties.get(AWS_IAM_ROLE_SESSION_NAME),
                    properties.get(AWS_STS_ENDPOINT),
                    properties.get(AWS_STS_REGION),
                    properties.get(AWS_STS_SIGNER_REGION),
                    properties.get(AWS_STS_ACCESS_KEY_ID),
                    properties.get(AWS_STS_SECRET_ACCESS_KEY));
        }
    }
}
