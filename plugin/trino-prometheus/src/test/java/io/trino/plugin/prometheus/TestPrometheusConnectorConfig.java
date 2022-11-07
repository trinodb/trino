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
package io.trino.plugin.prometheus;

import com.google.common.collect.ImmutableMap;
import com.google.inject.ConfigurationException;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPrometheusConnectorConfig
{
    @Test
    public void testDefaults()
            throws URISyntaxException
    {
        assertRecordedDefaults(recordDefaults(PrometheusConnectorConfig.class)
                .setPrometheusURI(new URI("http://localhost:9090"))
                .setQueryChunkSizeDuration(new Duration(1, DAYS))
                .setMaxQueryRangeDuration(new Duration(21, DAYS))
                .setCacheDuration(new Duration(30, SECONDS))
                .setBearerTokenFile(null)
                .setUser(null)
                .setPassword(null)
                .setReadTimeout(new Duration(10, SECONDS))
                .setCaseInsensitiveNameMatching(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("prometheus.uri", "file://test.json")
                .put("prometheus.query.chunk.size.duration", "365d")
                .put("prometheus.max.query.range.duration", "1095d")
                .put("prometheus.cache.ttl", "60s")
                .put("prometheus.bearer.token.file", "/tmp/bearer_token.txt")
                .put("prometheus.auth.user", "admin")
                .put("prometheus.auth.password", "password")
                .put("prometheus.read-timeout", "30s")
                .put("prometheus.case-insensitive-name-matching", "true")
                .buildOrThrow();

        URI uri = URI.create("file://test.json");
        PrometheusConnectorConfig expected = new PrometheusConnectorConfig();
        expected.setPrometheusURI(uri);
        expected.setQueryChunkSizeDuration(new Duration(365, DAYS));
        expected.setMaxQueryRangeDuration(new Duration(1095, DAYS));
        expected.setCacheDuration(new Duration(60, SECONDS));
        expected.setBearerTokenFile(new File("/tmp/bearer_token.txt"));
        expected.setUser("admin");
        expected.setPassword("password");
        expected.setReadTimeout(new Duration(30, SECONDS));
        expected.setCaseInsensitiveNameMatching(true);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testFailOnDurationLessThanQueryChunkConfig()
            throws Exception
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(new URI("http://doesnotmatter.com"));
        config.setQueryChunkSizeDuration(new Duration(21, DAYS));
        config.setMaxQueryRangeDuration(new Duration(1, DAYS));
        config.setCacheDuration(new Duration(30, SECONDS));
        assertThatThrownBy(config::checkConfig)
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("prometheus.max.query.range.duration must be greater than prometheus.query.chunk.size.duration");
    }

    @Test
    public void testInvalidAuth()
    {
        assertThatThrownBy(new PrometheusConnectorConfig().setBearerTokenFile(new File("/tmp/bearer_token.txt")).setUser("test")::checkConfig)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Either on of bearer token file or basic authentication should be used");

        assertThatThrownBy(new PrometheusConnectorConfig().setBearerTokenFile(new File("/tmp/bearer_token.txt")).setPassword("test")::checkConfig)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Either on of bearer token file or basic authentication should be used");

        assertThatThrownBy(new PrometheusConnectorConfig().setUser("test")::checkConfig)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Both username and password must be set when using basic authentication");

        assertThatThrownBy(new PrometheusConnectorConfig().setPassword("test")::checkConfig)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Both username and password must be set when using basic authentication");
    }
}
