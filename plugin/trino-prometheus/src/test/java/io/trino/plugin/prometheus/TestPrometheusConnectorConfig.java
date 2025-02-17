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
import com.google.common.net.HttpHeaders;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPrometheusConnectorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(PrometheusConnectorConfig.class)
                .setPrometheusURI(URI.create("http://localhost:9090"))
                .setQueryChunkSizeDuration(new Duration(1, DAYS))
                .setMaxQueryRangeDuration(new Duration(21, DAYS))
                .setCacheDuration(new Duration(30, SECONDS))
                .setBearerTokenFile(null)
                .setHttpAuthHeaderName(HttpHeaders.AUTHORIZATION)
                .setUser(null)
                .setPassword(null)
                .setReadTimeout(new Duration(10, SECONDS))
                .setCaseInsensitiveNameMatching(false)
                .setAdditionalHeaders(null));
    }

    @Test
    public void testExplicitPropertyMappingsWithBearerTokenFile()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("prometheus.uri", "file://test.json")
                .put("prometheus.query.chunk.size.duration", "365d")
                .put("prometheus.max.query.range.duration", "1095d")
                .put("prometheus.cache.ttl", "60s")
                .put("prometheus.auth.http.header.name", "X-team-auth")
                .put("prometheus.bearer.token.file", "/tmp/bearer_token.txt")
                .put("prometheus.read-timeout", "30s")
                .put("prometheus.case-insensitive-name-matching", "true")
                .put("prometheus.http.additional-headers", "key\\:1:value\\,1, key\\,2:value\\:2")
                .buildOrThrow();

        ConfigurationFactory configurationFactory = new ConfigurationFactory(properties);
        PrometheusConnectorConfig config = configurationFactory.build(PrometheusConnectorConfig.class);

        URI uri = URI.create("file://test.json");
        assertThat(config.getPrometheusURI()).isEqualTo(uri);
        assertThat(config.getQueryChunkSizeDuration()).isEqualTo(new Duration(365, DAYS));
        assertThat(config.getMaxQueryRangeDuration()).isEqualTo(new Duration(1095, DAYS));
        assertThat(config.getCacheDuration()).isEqualTo(new Duration(60, SECONDS));
        assertThat(config.getHttpAuthHeaderName()).isEqualTo("X-team-auth");
        assertThat(config.getBearerTokenFile()).contains(new File("/tmp/bearer_token.txt"));
        assertThat(config.getUser()).isEmpty();
        assertThat(config.getPassword()).isEmpty();
        assertThat(config.getReadTimeout()).isEqualTo(new Duration(30, SECONDS));
        assertThat(config.isCaseInsensitiveNameMatching()).isTrue();
        assertThat(config.getAdditionalHeaders()).isEqualTo(ImmutableMap.of("key\\:1", "value\\,1", "key\\,2", "value\\:2"));
    }

    @Test
    public void testFailOnDurationLessThanQueryChunkConfig()
    {
        PrometheusConnectorConfig config = new PrometheusConnectorConfig();
        config.setPrometheusURI(URI.create("http://doesnotmatter.com"));
        config.setQueryChunkSizeDuration(new Duration(21, DAYS));
        config.setMaxQueryRangeDuration(new Duration(1, DAYS));
        config.setCacheDuration(new Duration(30, SECONDS));

        assertFailsValidation(
                config,
                "maxQueryRangeDurationValid",
                "prometheus.max.query.range.duration must be greater than prometheus.query.chunk.size.duration",
                AssertTrue.class);
    }

    @Test
    public void testInvalidAuth()
    {
        assertFailsValidation(
                new PrometheusConnectorConfig().setBearerTokenFile(new File("/tmp/bearer_token.txt")).setUser("test"),
                "authConfigValid",
                "Either one of bearer token file or basic authentication should be used",
                AssertTrue.class);

        assertFailsValidation(
                new PrometheusConnectorConfig().setBearerTokenFile(new File("/tmp/bearer_token.txt")).setPassword("test"),
                "authConfigValid",
                "Either one of bearer token file or basic authentication should be used",
                AssertTrue.class);

        assertFailsValidation(
                new PrometheusConnectorConfig().setUser("test"),
                "basicAuthConfigValid",
                "Both username and password must be set when using basic authentication",
                AssertTrue.class);

        assertFailsValidation(
                new PrometheusConnectorConfig().setPassword("test"),
                "basicAuthConfigValid",
                "Both username and password must be set when using basic authentication",
                AssertTrue.class);

        assertFailsValidation(
                new PrometheusConnectorConfig().setAdditionalHeaders("Authorization: test").setHttpAuthHeaderName("Authorization"),
                "additionalHeadersValid",
                "Additional headers can not include authorization header",
                AssertTrue.class);
    }
}
