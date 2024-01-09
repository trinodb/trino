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
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.opensearch.OpenSearchConfig.Security.AWS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestOpenSearchConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OpenSearchConfig.class)
                .setHosts(null)
                .setPort(9200)
                .setDefaultSchema("default")
                .setScrollSize(1000)
                .setScrollTimeout(new Duration(1, MINUTES))
                .setRequestTimeout(new Duration(10, SECONDS))
                .setConnectTimeout(new Duration(1, SECONDS))
                .setBackoffInitDelay(new Duration(500, MILLISECONDS))
                .setBackoffMaxDelay(new Duration(20, SECONDS))
                .setMaxRetryTime(new Duration(30, SECONDS))
                .setNodeRefreshInterval(new Duration(1, MINUTES))
                .setMaxHttpConnections(25)
                .setHttpThreadCount(Runtime.getRuntime().availableProcessors())
                .setTlsEnabled(false)
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTrustStorePath(null)
                .setTruststorePassword(null)
                .setVerifyHostnames(true)
                .setIgnorePublishAddress(false)
                .setSecurity(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path keystoreFile = Files.createTempFile(null, null);
        Path truststoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("opensearch.host", "example.com")
                .put("opensearch.port", "9999")
                .put("opensearch.default-schema-name", "test")
                .put("opensearch.scroll-size", "4000")
                .put("opensearch.scroll-timeout", "20s")
                .put("opensearch.request-timeout", "1s")
                .put("opensearch.connect-timeout", "10s")
                .put("opensearch.backoff-init-delay", "100ms")
                .put("opensearch.backoff-max-delay", "15s")
                .put("opensearch.max-retry-time", "10s")
                .put("opensearch.node-refresh-interval", "10m")
                .put("opensearch.max-http-connections", "100")
                .put("opensearch.http-thread-count", "30")
                .put("opensearch.tls.enabled", "true")
                .put("opensearch.tls.keystore-path", keystoreFile.toString())
                .put("opensearch.tls.keystore-password", "keystore-password")
                .put("opensearch.tls.truststore-path", truststoreFile.toString())
                .put("opensearch.tls.truststore-password", "truststore-password")
                .put("opensearch.tls.verify-hostnames", "false")
                .put("opensearch.ignore-publish-address", "true")
                .put("opensearch.security", "AWS")
                .buildOrThrow();

        OpenSearchConfig expected = new OpenSearchConfig()
                .setHosts(Arrays.asList("example.com"))
                .setPort(9999)
                .setDefaultSchema("test")
                .setScrollSize(4000)
                .setScrollTimeout(new Duration(20, SECONDS))
                .setRequestTimeout(new Duration(1, SECONDS))
                .setConnectTimeout(new Duration(10, SECONDS))
                .setBackoffInitDelay(new Duration(100, MILLISECONDS))
                .setBackoffMaxDelay(new Duration(15, SECONDS))
                .setMaxRetryTime(new Duration(10, SECONDS))
                .setNodeRefreshInterval(new Duration(10, MINUTES))
                .setMaxHttpConnections(100)
                .setHttpThreadCount(30)
                .setTlsEnabled(true)
                .setKeystorePath(keystoreFile.toFile())
                .setKeystorePassword("keystore-password")
                .setTrustStorePath(truststoreFile.toFile())
                .setTruststorePassword("truststore-password")
                .setVerifyHostnames(false)
                .setIgnorePublishAddress(true)
                .setSecurity(AWS);

        assertFullMapping(properties, expected);
    }
}
