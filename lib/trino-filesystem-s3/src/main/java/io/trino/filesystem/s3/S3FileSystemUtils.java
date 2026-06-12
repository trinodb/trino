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
import java.util.Optional;

final class S3FileSystemUtils
{
    private S3FileSystemUtils() {}

    public static S3Presigner createS3PreSigner(S3FileSystemConfig config, S3Client s3Client)
    {
        return createS3PreSigner(config, s3Client, Optional.empty());
    }

    public static S3Presigner createS3PreSigner(S3FileSystemConfig config, S3Client s3Client, Optional<S3SecurityMappingResult> mapping)
    {
        Optional<AwsCredentialsProvider> staticCredentialsProvider = createStaticCredentialsProvider(config);
        Optional<String> staticRegion = Optional.ofNullable(config.getRegion());
        Optional<String> staticEndpoint = Optional.ofNullable(config.getEndpoint());
        S3AuthType authType = config.getAuthType();
        Optional<String> staticIamRole = Optional.ofNullable(config.getIamRole());
        String staticRoleSessionName = config.getRoleSessionName();
        String externalId = config.getExternalId();

        Optional<AwsCredentialsProvider> credentialsProvider = mapping
                .flatMap(S3SecurityMappingResult::credentialsProvider)
                .or(() -> staticCredentialsProvider);
        Optional<String> region = mapping.flatMap(S3SecurityMappingResult::region).or(() -> staticRegion);
        Optional<String> endpoint = mapping.flatMap(S3SecurityMappingResult::endpoint).or(() -> staticEndpoint);
        Optional<String> iamRole = mapping.flatMap(S3SecurityMappingResult::iamRole).or(() -> staticIamRole);
        String roleSessionName = mapping.flatMap(S3SecurityMappingResult::roleSessionName).orElse(staticRoleSessionName);
        boolean pathStyleAccess = mapping
                .flatMap(S3SecurityMappingResult::pathStyleAccess)
                .orElse(config.isPathStyleAccess());

        S3Presigner.Builder s3 = S3Presigner.builder();
        s3.s3Client(s3Client);

        region.map(Region::of).ifPresent(s3::region);
        endpoint.map(URI::create).ifPresent(s3::endpointOverride);
        s3.serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(pathStyleAccess)
                .build());

        switch (authType) {
            case ANONYMOUS -> s3.credentialsProvider(AnonymousCredentialsProvider.create());
            case WEB_IDENTITY -> s3.credentialsProvider(WebIdentityTokenFileCredentialsProvider.builder()
                    .asyncCredentialUpdateEnabled(true)
                    .build());
            case IAM_ROLE, DEFAULT -> {
                // A security mapping may supply a per-request IAM role even when the static auth type is DEFAULT.
                if (iamRole.isPresent()) {
                    s3.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                            .refreshRequest(request -> request
                                    .roleArn(iamRole.get())
                                    .roleSessionName(roleSessionName)
                                    .externalId(externalId))
                            .stsClient(createStsClient(config, credentialsProvider))
                            .asyncCredentialUpdateEnabled(true)
                            .build());
                }
                else {
                    credentialsProvider.ifPresent(s3::credentialsProvider);
                }
            }
        }

        return s3.build();
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
