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
import jakarta.validation.constraints.AssertFalse;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestGcsFileSystemConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(GcsFileSystemConfig.class)
                .setReadBlockSize(DataSize.of(2, MEGABYTE))
                .setWriteBlockSize(DataSize.of(16, MEGABYTE))
                .setPageSize(100)
                .setBatchSize(100)
                .setUseGcsAccessToken(false)
                .setProjectId(null)
                .setEndpoint(Optional.empty())
                .setAuthType(AuthType.SERVICE_ACCOUNT)
                .setMaxRetries(20)
                .setBackoffScaleFactor(3.0)
                .setMaxRetryTime(new Duration(25, SECONDS))
                .setMinBackoffDelay(new Duration(10, MILLISECONDS))
                .setMaxBackoffDelay(new Duration(2000, MILLISECONDS))
                .setApplicationId("Trino"));
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
                .setApplicationId("application id");
        assertFullMapping(properties, expected, Set.of("gcs.json-key", "gcs.json-key-file-path", "gcs.use-access-token"));
    }

    // backwards compatibility test, remove if use-access-token is removed
    @Test
    void testExplicitPropertyMappingsWithDeprecatedUseAccessToken()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("gcs.read-block-size", "51MB")
                .put("gcs.write-block-size", "52MB")
                .put("gcs.page-size", "10")
                .put("gcs.batch-size", "11")
                .put("gcs.project-id", "project")
                .put("gcs.endpoint", "http://custom.dns.org:8000")
                .put("gcs.use-access-token", "true")
                .put("gcs.client.max-retries", "10")
                .put("gcs.client.backoff-scale-factor", "4.0")
                .put("gcs.client.max-retry-time", "10s")
                .put("gcs.client.min-backoff-delay", "20ms")
                .put("gcs.client.max-backoff-delay", "20ms")
                .put("gcs.application-id", "application id")
                .buildOrThrow();

        GcsFileSystemConfig expected = new GcsFileSystemConfig()
                .setReadBlockSize(DataSize.of(51, MEGABYTE))
                .setWriteBlockSize(DataSize.of(52, MEGABYTE))
                .setPageSize(10)
                .setBatchSize(11)
                .setProjectId("project")
                .setEndpoint(Optional.of("http://custom.dns.org:8000"))
                .setUseGcsAccessToken(true)
                .setMaxRetries(10)
                .setBackoffScaleFactor(4.0)
                .setMaxRetryTime(new Duration(10, SECONDS))
                .setMinBackoffDelay(new Duration(20, MILLISECONDS))
                .setMaxBackoffDelay(new Duration(20, MILLISECONDS))
                .setApplicationId("application id");
        assertFullMapping(properties, expected, Set.of("gcs.json-key", "gcs.json-key-file-path", "gcs.auth-type"));
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
                        .setAuthType(AuthType.ACCESS_TOKEN)
                        .setUseGcsAccessToken(true),
                "authTypeAndGcsAccessTokenConfigured",
                "Cannot set both gcs.use-access-token and gcs.auth-type",
                AssertFalse.class);

        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setAuthType(AuthType.ACCESS_TOKEN)
                        .setUseGcsAccessToken(false),
                "authTypeAndGcsAccessTokenConfigured",
                "Cannot set both gcs.use-access-token and gcs.auth-type",
                AssertFalse.class);

        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setUseGcsAccessToken(true)
                        .setAuthType(AuthType.SERVICE_ACCOUNT),
                "authTypeAndGcsAccessTokenConfigured",
                "Cannot set both gcs.use-access-token and gcs.auth-type",
                AssertFalse.class);

        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setUseGcsAccessToken(false)
                        .setAuthType(AuthType.SERVICE_ACCOUNT),
                "authTypeAndGcsAccessTokenConfigured",
                "Cannot set both gcs.use-access-token and gcs.auth-type",
                AssertFalse.class);

        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setUseGcsAccessToken(true)
                        .setAuthType(AuthType.APPLICATION_DEFAULT),
                "authTypeAndGcsAccessTokenConfigured",
                "Cannot set both gcs.use-access-token and gcs.auth-type",
                AssertFalse.class);

        assertFailsValidation(
                new GcsFileSystemConfig()
                        .setUseGcsAccessToken(false)
                        .setAuthType(AuthType.APPLICATION_DEFAULT),
                "authTypeAndGcsAccessTokenConfigured",
                "Cannot set both gcs.use-access-token and gcs.auth-type",
                AssertFalse.class);
    }
}
