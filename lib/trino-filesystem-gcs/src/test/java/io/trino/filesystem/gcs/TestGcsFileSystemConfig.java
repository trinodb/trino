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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.gcs.GcsFileSystemConfig.AuthType;
import io.trino.filesystem.gcs.GcsFileSystemConfig.FileAccessPattern;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestGcsFileSystemConfig
{
    private static final String BASE64_KEY = "YmFzZTY0LWtleQ==";

    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GcsFileSystemConfig.class)
                .setReadBlockSize(DataSize.of(2, MEGABYTE))
                .setWriteBlockSize(DataSize.of(16, MEGABYTE))
                .setPageSize(100)
                .setBatchSize(100)
                .setProjectId(null)
                .setUserProject(Optional.empty())
                .setEndpoint(Optional.empty())
                .setClientLibToken(Optional.empty())
                .setAuthType(AuthType.SERVICE_ACCOUNT)
                .setMaxRetries(20)
                .setBackoffScaleFactor(3.0)
                .setMaxRetryTime(new Duration(25, SECONDS))
                .setMinBackoffDelay(new Duration(10, MILLISECONDS))
                .setMaxBackoffDelay(new Duration(2000, MILLISECONDS))
                .setApplicationId("Trino")
                .setAnalyticsCoreEnabled(false)
                .setAnalyticsCoreFileAccessPattern(FileAccessPattern.AUTO_RANDOM)
                .setAnalyticsCoreFooterPrefetchEnabled(true)
                .setAnalyticsCoreReadThreadCount(16)
                .setDecryptionKey(Optional.empty()));
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
                .put("gcs.user-project", "billing-project")
                .put("gcs.endpoint", "http://custom.dns.org:8000")
                .put("gcs.client-lib-token", "client-lib-token")
                .put("gcs.auth-type", "access_token")
                .put("gcs.client.max-retries", "10")
                .put("gcs.client.backoff-scale-factor", "4.0")
                .put("gcs.client.max-retry-time", "10s")
                .put("gcs.client.min-backoff-delay", "20ms")
                .put("gcs.client.max-backoff-delay", "20ms")
                .put("gcs.application-id", "application id")
                .put("gcs.analytics-core.enabled", "true")
                .put("gcs.analytics-core.file-access-pattern", "RANDOM")
                .put("gcs.analytics-core.footer-prefetch-enabled", "false")
                .put("gcs.analytics-core.read-thread-count", "8")
                .put("gcs.client.decryption-key", BASE64_KEY)
                .buildOrThrow();

        GcsFileSystemConfig expected = new GcsFileSystemConfig()
                .setReadBlockSize(DataSize.of(51, MEGABYTE))
                .setWriteBlockSize(DataSize.of(52, MEGABYTE))
                .setPageSize(10)
                .setBatchSize(11)
                .setProjectId("project")
                .setUserProject(Optional.of("billing-project"))
                .setEndpoint(Optional.of("http://custom.dns.org:8000"))
                .setClientLibToken(Optional.of("client-lib-token"))
                .setAuthType(AuthType.ACCESS_TOKEN)
                .setMaxRetries(10)
                .setBackoffScaleFactor(4.0)
                .setMaxRetryTime(new Duration(10, SECONDS))
                .setMinBackoffDelay(new Duration(20, MILLISECONDS))
                .setMaxBackoffDelay(new Duration(20, MILLISECONDS))
                .setApplicationId("application id")
                .setAnalyticsCoreEnabled(true)
                .setAnalyticsCoreFileAccessPattern(FileAccessPattern.RANDOM)
                .setAnalyticsCoreFooterPrefetchEnabled(false)
                .setAnalyticsCoreReadThreadCount(8)
                .setDecryptionKey(Optional.of(BASE64_KEY));
        assertFullMapping(properties, expected);
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

        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setDecryptionKey(Optional.of("not-base64")),
                "decryptionKeyValid",
                "gcs.client.decryption-key must be base64 encoded",
                AssertTrue.class);
    }
}
