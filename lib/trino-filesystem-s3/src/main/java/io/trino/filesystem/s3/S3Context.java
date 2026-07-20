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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.s3.S3FileSystemConfig.ObjectCannedAcl;
import io.trino.filesystem.s3.S3FileSystemConfig.S3SseType;
import io.trino.filesystem.s3.S3FileSystemConfig.StorageClassType;
import io.trino.spi.security.ConnectorIdentity;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.s3.model.RequestPayer;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.filesystem.s3.S3FileSystemConfig.S3SseType.CUSTOMER;
import static io.trino.filesystem.s3.S3FileSystemConfig.S3SseType.KMS;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_OBJECT_TAGS;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

record S3Context(
        int partSize,
        boolean requesterPays,
        S3SseContext s3SseContext,
        Optional<AwsCredentialsProvider> credentialsProviderOverride,
        StorageClassType storageClass,
        ObjectCannedAcl cannedAcl,
        Map<String, String> objectTags,
        Set<String> objectTagsPrefixes)
{
    private static final int MIN_PART_SIZE = 5 * 1024 * 1024; // S3 requirement

    public S3Context
    {
        checkArgument(partSize >= MIN_PART_SIZE, "partSize must be at least %s bytes", MIN_PART_SIZE);
        requireNonNull(s3SseContext, "sseContext is null");
        requireNonNull(credentialsProviderOverride, "credentialsProviderOverride is null");
        requireNonNull(objectTags, "objectTags is null");
        requireNonNull(objectTagsPrefixes, "objectTagsPrefixes is null");
    }

    public RequestPayer requestPayer()
    {
        return requesterPays ? RequestPayer.REQUESTER : null;
    }

    public S3Context withKmsKeyId(String kmsKeyId)
    {
        return new S3Context(partSize, requesterPays, S3SseContext.withKmsKeyId(kmsKeyId), credentialsProviderOverride, storageClass, cannedAcl, objectTags, objectTagsPrefixes);
    }

    public S3Context withCredentials(ConnectorIdentity identity)
    {
        S3Context context = this;
        if (identity.getExtraCredentials().containsKey(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY)) {
            AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsSessionCredentials.create(
                    identity.getExtraCredentials().get(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY),
                    identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY),
                    identity.getExtraCredentials().get(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY)));
            context = context.withCredentialsProviderOverride(credentialsProvider);
        }
        if (identity.getExtraCredentials().containsKey(EXTRA_CREDENTIALS_OBJECT_TAGS)) {
            Map<String, String> mergedTags = new HashMap<>(context.objectTags());
            mergedTags.putAll(parseTags(identity.getExtraCredentials().get(EXTRA_CREDENTIALS_OBJECT_TAGS)));
            context = context.withObjectTags(mergedTags);
        }
        return context;
    }

    public S3Context withSseCustomerKey(String key)
    {
        return new S3Context(partSize, requesterPays, S3SseContext.withSseCustomerKey(key), credentialsProviderOverride, storageClass, cannedAcl, objectTags, objectTagsPrefixes);
    }

    public S3Context withObjectTags(Map<String, String> objectTags)
    {
        return new S3Context(partSize, requesterPays, s3SseContext, credentialsProviderOverride, storageClass, cannedAcl, ImmutableMap.copyOf(objectTags), objectTagsPrefixes);
    }

    public S3Context withCredentialsProviderOverride(AwsCredentialsProvider credentialsProviderOverride)
    {
        return new S3Context(
                partSize,
                requesterPays,
                s3SseContext,
                Optional.of(credentialsProviderOverride),
                storageClass,
                cannedAcl,
                objectTags,
                objectTagsPrefixes);
    }

    public void applyCredentialProviderOverride(AwsRequestOverrideConfiguration.Builder builder)
    {
        credentialsProviderOverride.ifPresent(builder::credentialsProvider);
    }

    private static Map<String, String> parseTags(String tags)
    {
        Map<String, String> parsed = Splitter.on(',').omitEmptyStrings().trimResults()
                .withKeyValueSeparator(Splitter.on('=').limit(2))
                .split(nullToEmpty(tags));
        checkArgument(parsed.size() <= 10, "Maximum 10 object tags allowed");
        parsed.forEach((key, value) -> {
            checkArgument(key.length() <= 128, "Object tag key too long: %s", key);
            checkArgument(value.length() <= 256, "Object tag value too long for key '%s': %s", key, value);
        });
        return parsed;
    }

    record S3SseContext(S3SseType sseType, Optional<String> sseKmsKeyId, Optional<S3SseCustomerKey> sseCustomerKey)
    {
        S3SseContext
        {
            requireNonNull(sseType, "sseType is null");
            requireNonNull(sseKmsKeyId, "sseKmsKeyId is null");
            requireNonNull(sseCustomerKey, "sseCustomerKey is null");
            switch (sseType) {
                case KMS -> checkArgument(sseKmsKeyId.isPresent(), "sseKmsKeyId is missing for SSE-KMS");
                case CUSTOMER -> checkArgument(sseCustomerKey.isPresent(), "sseCustomerKey is missing for SSE-C");
                case NONE, S3 -> {}
            }
        }

        public static S3SseContext of(S3SseType sseType, String sseKmsKeyId, String sseCustomerKey)
        {
            return new S3SseContext(sseType, Optional.ofNullable(sseKmsKeyId), Optional.ofNullable(sseCustomerKey).map(S3SseCustomerKey::onAes256));
        }

        public static S3SseContext withKmsKeyId(String kmsKeyId)
        {
            return new S3SseContext(KMS, Optional.ofNullable(kmsKeyId), Optional.empty());
        }

        public static S3SseContext withSseCustomerKey(String key)
        {
            return new S3SseContext(CUSTOMER, Optional.empty(), Optional.ofNullable(key).map(S3SseCustomerKey::onAes256));
        }
    }
}
