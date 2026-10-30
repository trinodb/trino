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
package io.trino.filesystem.s3;

import com.google.common.annotations.VisibleForTesting;
import io.trino.filesystem.s3.S3FileSystemConfig.S3AuthType;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;

import static java.lang.Math.toIntExact;

final class S3FileSystemUtils
{
    // A pre-signed URI stops working when the temporary credentials that signed it expire, so the pre-signer
    // must not sign with credentials that are about to expire. Credentials are refreshed automatically between
    // `prefetchTime` and `staleTime` before expiration. In the AWS SDK `prefetchTime` defaults to 5 min and
    // `staleTime` to 1 min, which means a segment presigned URI may be valid only 1 minute. We provide higher
    // values to minimize the chances of URIs expiring before segments are fetched.
    @VisibleForTesting
    static final Duration PRE_SIGNER_SESSION_DURATION = Duration.ofHours(1);
    @VisibleForTesting
    static final Duration PRE_SIGNER_CREDENTIALS_STALE_TIME = Duration.ofMinutes(30);
    @VisibleForTesting
    static final Duration PRE_SIGNER_CREDENTIALS_PREFETCH_TIME = PRE_SIGNER_CREDENTIALS_STALE_TIME.plusMinutes(2);

    private S3FileSystemUtils() {}

    public static S3Presigner createS3PreSigner(S3FileSystemConfig config, S3Client s3Client)
    {
        Optional<AwsCredentialsProvider> staticCredentialsProvider = createStaticCredentialsProvider(config);
        Optional<String> staticRegion = Optional.ofNullable(config.getRegion());
        Optional<String> staticEndpoint = Optional.ofNullable(config.getEndpoint());
        boolean pathStyleAccess = config.isPathStyleAccess();
        S3AuthType authType = config.getAuthType();
        Optional<String> staticIamRole = Optional.ofNullable(config.getIamRole());
        String staticRoleSessionName = config.getRoleSessionName();
        String externalId = config.getExternalId();

        S3Presigner.Builder s3 = S3Presigner.builder();
        s3.s3Client(s3Client);

        staticRegion.map(Region::of).ifPresent(s3::region);
        staticEndpoint.map(URI::create).ifPresent(s3::endpointOverride);
        s3.serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(pathStyleAccess)
                .build());

        switch (authType) {
            case ANONYMOUS -> s3.credentialsProvider(AnonymousCredentialsProvider.create());
            case WEB_IDENTITY -> s3.credentialsProvider(createPreSignerWebIdentityCredentialsProvider(WebIdentityTokenFileCredentialsProvider.builder()));
            case IAM_ROLE -> s3.credentialsProvider(createPreSignerAssumeRoleCredentialsProvider(
                    createStsClient(config, staticCredentialsProvider),
                    staticIamRole.orElseThrow(),
                    staticRoleSessionName,
                    externalId));
            case DEFAULT -> staticCredentialsProvider.ifPresent(s3::credentialsProvider);
        }

        return s3.build();
    }

    /**
     * The role and token file are read from {@code AWS_ROLE_ARN} and {@code AWS_WEB_IDENTITY_TOKEN_FILE}
     * unless set on {@code provider}.
     */
    @VisibleForTesting
    static WebIdentityTokenFileCredentialsProvider createPreSignerWebIdentityCredentialsProvider(WebIdentityTokenFileCredentialsProvider.Builder provider)
    {
        return provider
                .asyncCredentialUpdateEnabled(true)
                .roleSessionDuration(PRE_SIGNER_SESSION_DURATION)
                .prefetchTime(PRE_SIGNER_CREDENTIALS_PREFETCH_TIME)
                .staleTime(PRE_SIGNER_CREDENTIALS_STALE_TIME)
                .build();
    }

    @VisibleForTesting
    static StsAssumeRoleCredentialsProvider createPreSignerAssumeRoleCredentialsProvider(StsClient stsClient, String iamRole, String roleSessionName, String externalId)
    {
        return StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(request -> request
                        .roleArn(iamRole)
                        .roleSessionName(roleSessionName)
                        .externalId(externalId)
                        .durationSeconds(toIntExact(PRE_SIGNER_SESSION_DURATION.toSeconds())))
                .stsClient(stsClient)
                .asyncCredentialUpdateEnabled(true)
                .prefetchTime(PRE_SIGNER_CREDENTIALS_PREFETCH_TIME)
                .staleTime(PRE_SIGNER_CREDENTIALS_STALE_TIME)
                .build();
    }

    static StsClient createStsClient(S3FileSystemConfig config, Optional<AwsCredentialsProvider> credentialsProvider)
    {
        StsClientBuilder sts = StsClient.builder();
        Optional.ofNullable(config.getStsEndpoint()).map(URI::create).ifPresent(sts::endpointOverride);
        Optional.ofNullable(config.getStsRegion())
                .or(() -> Optional.ofNullable(config.getRegion()))
                .map(Region::of).ifPresent(sts::region);
        credentialsProvider.ifPresent(sts::credentialsProvider);
        return sts.build();
    }

    static Optional<AwsCredentialsProvider> createStaticCredentialsProvider(S3FileSystemConfig config)
    {
        if ((config.getAwsAccessKey() != null) || (config.getAwsSecretKey() != null)) {
            return Optional.of(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey(), config.getAwsSecretKey())));
        }
        return Optional.empty();
    }
}
