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
package io.trino.filesystem.gcs;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.gcs.GcsFileSystemConfig.AuthType;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.filesystem.gcs.GcsFileSystemConfig.GcsSseType.CUSTOMER;
import static io.trino.filesystem.gcs.GcsFileSystemConfig.GcsSseType.KMS;
import static io.trino.filesystem.gcs.GcsFileSystemConfig.GcsSseType.NONE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGcsFileSystemConfig
{
    private static final String ENCRYPTION_KEY = Base64.getEncoder().encodeToString(new byte[32]);
    private static final String DECRYPTION_KEY = Base64.getEncoder().encodeToString("01234567890123456789012345678901".getBytes(UTF_8));

    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GcsFileSystemConfig.class)
                .setReadBlockSize(DataSize.of(2, MEGABYTE))
                .setWriteBlockSize(DataSize.of(16, MEGABYTE))
                .setPageSize(100)
                .setBatchSize(100)
                .setProjectId(null)
                .setEndpoint(Optional.empty())
                .setAuthType(AuthType.SERVICE_ACCOUNT)
                .setMaxRetries(20)
                .setBackoffScaleFactor(3.0)
                .setMaxRetryTime(new Duration(25, SECONDS))
                .setMinBackoffDelay(new Duration(10, MILLISECONDS))
                .setMaxBackoffDelay(new Duration(2000, MILLISECONDS))
                .setApplicationId("Trino")
                .setSseType(NONE)
                .setSseKmsKeyName(null)
                .setCustomerEncryptionKey(null)
                .setCustomerDecryptionKey(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("gcs.read-block-size", "51MB")
                .put("gcs.write-block-size", "52MB")
                .put("gcs.page-size", "10")
                .put("gcs.batch-size", "11")
                .put("gcs.project-id", "project")
                .put("gcs.endpoint", "http://custom.dns.org:8000")
                .put("gcs.auth-type", "access_token")
                .put("gcs.client.max-retries", "10")
                .put("gcs.client.backoff-scale-factor", "4.0")
                .put("gcs.client.max-retry-time", "10s")
                .put("gcs.client.min-backoff-delay", "20ms")
                .put("gcs.client.max-backoff-delay", "20ms")
                .put("gcs.application-id", "application id")
                .put("gcs.sse.type", "CUSTOMER")
                .put("gcs.customer-encryption-key", ENCRYPTION_KEY)
                .put("gcs.customer-decryption-key", DECRYPTION_KEY)
                .buildOrThrow();

        GcsFileSystemConfig expected = new GcsFileSystemConfig()
                .setReadBlockSize(DataSize.of(51, MEGABYTE))
                .setWriteBlockSize(DataSize.of(52, MEGABYTE))
                .setPageSize(10)
                .setBatchSize(11)
                .setProjectId("project")
                .setEndpoint(Optional.of("http://custom.dns.org:8000"))
                .setAuthType(AuthType.ACCESS_TOKEN)
                .setMaxRetries(10)
                .setBackoffScaleFactor(4.0)
                .setMaxRetryTime(new Duration(10, SECONDS))
                .setMinBackoffDelay(new Duration(20, MILLISECONDS))
                .setMaxBackoffDelay(new Duration(20, MILLISECONDS))
                .setApplicationId("application id")
                .setSseType(CUSTOMER)
                .setCustomerEncryptionKey(ENCRYPTION_KEY)
                .setCustomerDecryptionKey(DECRYPTION_KEY);
        assertFullMapping(properties, expected, Set.of("gcs.sse.kms-key-name"));
    }

    @Test
    void testKmsPropertyMapping()
    {
        GcsFileSystemConfig config = new ConfigurationFactory(ImmutableMap.of(
                "gcs.sse.type", "KMS",
                "gcs.sse.kms-key-name", "kmsKeyName"))
                .build(GcsFileSystemConfig.class);

        assertThat(config.getSseType()).isEqualTo(KMS);
        assertThat(config.getSseKmsKeyName()).contains("kmsKeyName");
    }

    @Test
    public void testValidation()
    {
        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setMinBackoffDelay(new Duration(20, MILLISECONDS))
                        .setMaxBackoffDelay(new Duration(19, MILLISECONDS)),
                "retryDelayValid",
                "gcs.client.min-backoff-delay must be less than or equal to gcs.client.max-backoff-delay",
                AssertTrue.class);
    }

    @Test
    void testServerSideEncryptionValidation()
    {
        assertValidates(new GcsFileSystemConfig());
        assertValidates(new GcsFileSystemConfig()
                .setSseType(KMS)
                .setSseKmsKeyName("kmsKeyName"));
        assertValidates(new GcsFileSystemConfig()
                .setSseType(CUSTOMER)
                .setCustomerEncryptionKey(ENCRYPTION_KEY));
        assertValidates(new GcsFileSystemConfig()
                .setSseType(CUSTOMER)
                .setCustomerEncryptionKey(ENCRYPTION_KEY)
                .setCustomerDecryptionKey(DECRYPTION_KEY));

        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setCustomerEncryptionKey(ENCRYPTION_KEY),
                "customerEncryptionKeyConfigValid",
                "gcs.customer-encryption-key must be a Base64-encoded 256-bit key when, and only when, gcs.sse.type=CUSTOMER",
                AssertTrue.class);
        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setCustomerDecryptionKey(DECRYPTION_KEY),
                "customerDecryptionKeyConfigValid",
                "gcs.customer-decryption-key must be a Base64-encoded 256-bit key when set, and can only be set when gcs.sse.type=CUSTOMER",
                AssertTrue.class);
        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setSseType(CUSTOMER)
                        .setCustomerDecryptionKey(DECRYPTION_KEY),
                "customerEncryptionKeyConfigValid",
                "gcs.customer-encryption-key must be a Base64-encoded 256-bit key when, and only when, gcs.sse.type=CUSTOMER",
                AssertTrue.class);
        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setSseType(CUSTOMER)
                        .setCustomerEncryptionKey("not-base64")
                        .setCustomerDecryptionKey(DECRYPTION_KEY),
                "customerEncryptionKeyConfigValid",
                "gcs.customer-encryption-key must be a Base64-encoded 256-bit key when, and only when, gcs.sse.type=CUSTOMER",
                AssertTrue.class);
        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setSseType(CUSTOMER)
                        .setCustomerEncryptionKey(ENCRYPTION_KEY)
                        .setCustomerDecryptionKey(Base64.getEncoder().encodeToString(new byte[31])),
                "customerDecryptionKeyConfigValid",
                "gcs.customer-decryption-key must be a Base64-encoded 256-bit key when set, and can only be set when gcs.sse.type=CUSTOMER",
                AssertTrue.class);
        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setSseKmsKeyName("kmsKeyName"),
                "sseKmsKeyNameConfigValid",
                "gcs.sse.kms-key-name must be set when, and only when, gcs.sse.type=KMS",
                AssertTrue.class);
        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setSseType(KMS),
                "sseKmsKeyNameConfigValid",
                "gcs.sse.kms-key-name must be set when, and only when, gcs.sse.type=KMS",
                AssertTrue.class);
        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setSseType(KMS)
                        .setSseKmsKeyName(" "),
                "sseKmsKeyNameConfigValid",
                "gcs.sse.kms-key-name must be set when, and only when, gcs.sse.type=KMS",
                AssertTrue.class);
    }
}
