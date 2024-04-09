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

import io.trino.filesystem.s3.S3FileSystemConfig.ObjectCannedAcl;
import io.trino.filesystem.s3.S3FileSystemConfig.S3SseType;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.s3.model.RequestPayer;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

record S3Context(int partSize, boolean requesterPays, S3SseType sseType, String sseKmsKeyId, Optional<AwsCredentialsProvider> credentialsProviderOverride, ObjectCannedAcl cannedAcl)
{
    private static final int MIN_PART_SIZE = 5 * 1024 * 1024; // S3 requirement

    public S3Context
    {
        checkArgument(partSize >= MIN_PART_SIZE, "partSize must be at least %s bytes", MIN_PART_SIZE);
        requireNonNull(sseType, "sseType is null");
        checkArgument((sseType != S3SseType.KMS) || (sseKmsKeyId != null), "sseKmsKeyId is null for SSE-KMS");
        requireNonNull(credentialsProviderOverride, "credentialsProviderOverride is null");
    }

    public RequestPayer requestPayer()
    {
        return requesterPays ? RequestPayer.REQUESTER : null;
    }

    public S3Context withCredentialsProviderOverride(AwsCredentialsProvider credentialsProviderOverride)
    {
        return new S3Context(
                partSize,
                requesterPays,
                sseType,
                sseKmsKeyId,
                Optional.of(credentialsProviderOverride),
                cannedAcl);
    }

    public void applyCredentialProviderOverride(AwsRequestOverrideConfiguration.Builder builder)
    {
        credentialsProviderOverride.ifPresent(builder::credentialsProvider);
    }
}
