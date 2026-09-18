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
package io.trino.plugin.exchange.filesystem.containers;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.exchange.filesystem.s3.ExchangeS3Config.S3SseType;
import io.trino.testing.containers.Floci;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;

import java.util.Map;

import static io.trino.plugin.exchange.filesystem.s3.ExchangeS3Config.S3SseType.KMS;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;
import static java.util.Objects.requireNonNull;

public final class FlociStorage
        implements AutoCloseable
{
    private final Floci floci = new Floci();
    private final String bucketName;
    private final S3SseType sseType;
    private String kmsKeyId;

    public FlociStorage(String bucketName, S3SseType sseType)
    {
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.sseType = requireNonNull(sseType, "sseType is null");
    }

    public void start()
    {
        floci.start();
        floci.createBucket(bucketName);
        if (sseType == KMS) {
            try (KmsClient kmsClient = KmsClient.builder().applyMutation(floci::updateClient).build()) {
                kmsKeyId = kmsClient.createKey(CreateKeyRequest.builder().build()).keyMetadata().arn();
            }
        }
    }

    public Map<String, String> getExchangeManagerProperties()
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("exchange.base-directories", "s3://" + bucketName)
                // to trigger file split in some tests
                .put("exchange.sink-max-file-size", "16MB")
                .put("exchange.s3.aws-access-key", FLOCI_ACCESS_KEY)
                .put("exchange.s3.aws-secret-key", FLOCI_SECRET_KEY)
                .put("exchange.s3.region", FLOCI_REGION)
                .put("exchange.s3.path-style-access", "true")
                .put("exchange.s3.endpoint", floci.endpoint().toString())
                .put("exchange.s3.sse.type", sseType.name())
                // create more granular source handles given the fault-tolerant execution target task input size is set to lower value for testing
                .put("exchange.source-handle-target-data-size", "1MB");
        if (sseType == KMS) {
            properties.put("exchange.s3.sse.kms-key-id", kmsKeyId);
        }
        return properties.buildOrThrow();
    }

    @Override
    public void close()
    {
        floci.close();
    }
}
