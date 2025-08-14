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
package io.trino.plugin.httpquery;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static org.assertj.core.api.Assertions.assertThat;

final class TestHttpEventListenerConfig
{
    @Test
    void testValidateHeaderConfigRedundant()
            throws IOException
    {
        HttpEventListenerConfig config = new HttpEventListenerConfig();
        // Neither set: valid
        assertThat(config.validateHeaderConfigRedundant()).isTrue();

        // Only httpHeaders set: valid
        config.setHttpHeaders(List.of("Authorization: Trust Me"));
        assertThat(config.validateHeaderConfigRedundant()).isTrue();

        // Only httpHeadersConfigFile set: valid
        config = new HttpEventListenerConfig();
        config.setHttpHeadersConfigFile(Files.createTempFile(null, null).toFile());
        assertThat(config.validateHeaderConfigRedundant()).isTrue();

        // Both set: invalid
        config.setHttpHeaders(List.of("Authorization: Trust Me"));
        assertThat(config.validateHeaderConfigRedundant()).isFalse();
    }

    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HttpEventListenerConfig.class)
                .setHttpHeaders(List.of())
                .setHttpHeadersConfigFile(null)
                .setIngestUri(null)
                .setRetryCount(0)
                .setRetryDelay(Duration.succinctDuration(1, TimeUnit.SECONDS))
                .setMaxDelay(Duration.succinctDuration(1, TimeUnit.MINUTES))
                .setBackoffBase(2.0)
                .setHttpMethod(HttpEventListenerHttpMethod.POST)
                .setLogCompleted(false)
                .setLogCreated(false)
                .setLogSplit(false));
    }

    @Test
    void testExplicitPropertyMappingsSkippingConnectHttpHeaders()
            throws IOException
    {
        Path httpHeadersConfigFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("http-event-listener.connect-http-headers.config-file", httpHeadersConfigFile.toString())
                .put("http-event-listener.log-created", "true")
                .put("http-event-listener.log-completed", "true")
                .put("http-event-listener.log-split", "true")
                .put("http-event-listener.connect-ingest-uri", "http://example.com:8080/api")
                .put("http-event-listener.connect-retry-count", "2")
                .put("http-event-listener.connect-http-method", "PUT")
                .put("http-event-listener.connect-retry-delay", "101s")
                .put("http-event-listener.connect-backoff-base", "1.5")
                .put("http-event-listener.connect-max-delay", "10m")
                .buildOrThrow();

        HttpEventListenerConfig expected = new HttpEventListenerConfig()
                .setHttpHeadersConfigFile(httpHeadersConfigFile.toFile())
                .setLogCompleted(true)
                .setLogCreated(true)
                .setLogSplit(true)
                .setIngestUri("http://example.com:8080/api")
                .setRetryCount(2)
                .setHttpMethod(HttpEventListenerHttpMethod.PUT)
                .setRetryDelay(Duration.succinctDuration(101, TimeUnit.SECONDS))
                .setBackoffBase(1.5)
                .setMaxDelay(Duration.succinctDuration(10, TimeUnit.MINUTES));

        assertFullMapping(properties, expected, Set.of("http-event-listener.connect-http-headers"));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("http-event-listener.connect-http-headers", "Authorization: Trust Me, Cache-Control: no-cache")
                .put("http-event-listener.log-created", "true")
                .put("http-event-listener.log-completed", "true")
                .put("http-event-listener.log-split", "true")
                .put("http-event-listener.connect-ingest-uri", "http://example.com:8080/api")
                .put("http-event-listener.connect-retry-count", "2")
                .put("http-event-listener.connect-http-method", "PUT")
                .put("http-event-listener.connect-retry-delay", "101s")
                .put("http-event-listener.connect-backoff-base", "1.5")
                .put("http-event-listener.connect-max-delay", "10m")
                .buildOrThrow();

        HttpEventListenerConfig expected = new HttpEventListenerConfig()
                .setHttpHeaders(List.of("Authorization: Trust Me", "Cache-Control: no-cache"))
                .setLogCompleted(true)
                .setLogCreated(true)
                .setLogSplit(true)
                .setIngestUri("http://example.com:8080/api")
                .setRetryCount(2)
                .setHttpMethod(HttpEventListenerHttpMethod.PUT)
                .setRetryDelay(Duration.succinctDuration(101, TimeUnit.SECONDS))
                .setBackoffBase(1.5)
                .setMaxDelay(Duration.succinctDuration(10, TimeUnit.MINUTES));

        assertFullMapping(properties, expected, Set.of("http-event-listener.connect-http-headers.config-file"));
    }

    @Test
    void testConfigFileDoesNotExist()
    {
        File file = new File("/doesNotExist-" + UUID.randomUUID());
        assertFailsValidation(
                new HttpEventListenerConfig()
                        .setHttpHeadersConfigFile(file),
                "httpHeadersConfigFile",
                "file does not exist: " + file,
                FileExists.class);
    }
}
