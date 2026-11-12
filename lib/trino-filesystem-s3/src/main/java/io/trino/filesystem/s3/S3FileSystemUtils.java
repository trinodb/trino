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
import java.util.Optional;

final class S3FileSystemUtils
{
    private S3FileSystemUtils() {}

    public static S3Presigner createS3PreSigner(S3PresignerSettings settings, S3Client s3Client)
    {
        S3Presigner.Builder s3 = S3Presigner.builder();
        s3.s3Client(s3Client);

        settings.region().map(Region::of).ifPresent(s3::region);
        settings.endpoint().map(URI::create).ifPresent(s3::endpointOverride);
        s3.serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(settings.pathStyleAccess())
                .build());

        switch (settings.authType()) {
            case ANONYMOUS -> s3.credentialsProvider(AnonymousCredentialsProvider.create());
            case WEB_IDENTITY -> s3.credentialsProvider(WebIdentityTokenFileCredentialsProvider.builder()
                    .asyncCredentialUpdateEnabled(true)
                    .build());
            case IAM_ROLE -> s3.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(request -> request
                            .roleArn(settings.iamRole().orElseThrow())
                            .roleSessionName(settings.roleSessionName())
                            .externalId(settings.externalId().orElse(null)))
                    .stsClient(createStsClient(settings.stsEndpoint(), settings.stsRegion(), settings.credentialsProvider()))
                    .asyncCredentialUpdateEnabled(true)
                    .build());
            case DEFAULT -> settings.credentialsProvider().ifPresent(s3::credentialsProvider);
        }

        return s3.build();
    }

    static StsClient createStsClient(Optional<String> endpoint, Optional<String> region, Optional<AwsCredentialsProvider> credentialsProvider)
    {
        StsClientBuilder sts = StsClient.builder();
        endpoint.map(URI::create).ifPresent(sts::endpointOverride);
        region.map(Region::of).ifPresent(sts::region);
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
