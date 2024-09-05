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
import io.trino.spi.security.ConnectorIdentity;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.s3.model.RequestPayer;

import java.util.Base64;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.filesystem.s3.S3FileSystemConfig.S3SseType.CUSTOMER;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SSEC_KEY;
import static java.util.Objects.requireNonNull;
import static software.amazon.awssdk.utils.Md5Utils.md5AsBase64;

record S3Context(int partSize, boolean requesterPays, S3SseType sseType, String sseKmsKeyId, Optional<AwsCredentialsProvider> credentialsProviderOverride, Optional<String> encryptionKey, ObjectCannedAcl cannedAcl, boolean exclusiveWriteSupported)
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

    public S3Context withKmsKeyId(String kmsKeyId)
    {
        return new S3Context(partSize, requesterPays, sseType, kmsKeyId, credentialsProviderOverride, encryptionKey, cannedAcl, exclusiveWriteSupported);
    }

    public S3Context withCredentials(ConnectorIdentity identity)
    {
        if (identity.getExtraCredentials().containsKey(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY)) {
            AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsSessionCredentials.create(
                    identity.getExtraCredentials().get(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY),
                    identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY),
                    identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY)));
            return withCredentialsProviderOverride(credentialsProvider);
        }

        if (identity.getExtraCredentials().containsKey(EXTRA_CREDENTIALS_SSEC_KEY)) {
            byte[] decodedKey = Base64.getDecoder().decode(identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SSEC_KEY));
            verify(decodedKey.length == 32, "AES encryption key must be 256 bits long");
            return withCustomerEncryptionKey(identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SSEC_KEY));
        }

        return this;
    }

    public S3Context withCredentialsProviderOverride(AwsCredentialsProvider credentialsProviderOverride)
    {
        return new S3Context(
                partSize,
                requesterPays,
                sseType,
                sseKmsKeyId,
                Optional.of(credentialsProviderOverride),
                encryptionKey,
                cannedAcl,
                exclusiveWriteSupported);
    }

    public S3Context withCustomerEncryptionKey(String serializedKey)
    {
        verify(sseType == S3SseType.NONE, "encryption key can only be set when S3SseType is NONE");
        return new S3Context(
                partSize,
                requesterPays,
                CUSTOMER,
                sseKmsKeyId,
                credentialsProviderOverride,
                Optional.of(serializedKey),
                cannedAcl,
                exclusiveWriteSupported);
    }

    public void applyCredentialProviderOverride(AwsRequestOverrideConfiguration.Builder builder)
    {
        credentialsProviderOverride.ifPresent(builder::credentialsProvider);
    }

    public Optional<String> encryptionKeyMd5()
    {
        return encryptionKey.map(S3Context::md5Sum);
    }

    private static String md5Sum(String key)
    {
        return md5AsBase64(Base64.getDecoder().decode(key));
    }
}
