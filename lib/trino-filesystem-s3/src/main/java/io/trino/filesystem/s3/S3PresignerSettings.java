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
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Optional;

import static io.trino.filesystem.s3.S3FileSystemUtils.createStaticCredentialsProvider;
import static java.util.Objects.requireNonNull;

record S3PresignerSettings(
        Optional<AwsCredentialsProvider> credentialsProvider,
        Optional<String> region,
        Optional<String> endpoint,
        boolean pathStyleAccess,
        S3AuthType authType,
        Optional<String> iamRole,
        String roleSessionName,
        Optional<String> externalId,
        Optional<String> stsEndpoint,
        Optional<String> stsRegion)
{
    public S3PresignerSettings
    {
        requireNonNull(credentialsProvider, "credentialsProvider is null");
        requireNonNull(region, "region is null");
        requireNonNull(endpoint, "endpoint is null");
        requireNonNull(authType, "authType is null");
        requireNonNull(iamRole, "iamRole is null");
        requireNonNull(roleSessionName, "roleSessionName is null");
        requireNonNull(externalId, "externalId is null");
        requireNonNull(stsEndpoint, "stsEndpoint is null");
        requireNonNull(stsRegion, "stsRegion is null");
    }

    public S3PresignerSettings(S3FileSystemConfig config)
    {
        this(createStaticCredentialsProvider(config),
                Optional.ofNullable(config.getRegion()),
                Optional.ofNullable(config.getEndpoint()),
                config.isPathStyleAccess(),
                config.getAuthType(),
                Optional.ofNullable(config.getIamRole()),
                config.getRoleSessionName(),
                Optional.ofNullable(config.getExternalId()),
                Optional.ofNullable(config.getStsEndpoint()),
                Optional.ofNullable(config.getStsRegion())
                        .or(() -> Optional.ofNullable(config.getRegion())));
    }
}
