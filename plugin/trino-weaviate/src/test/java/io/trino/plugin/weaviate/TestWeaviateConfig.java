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
package io.trino.plugin.weaviate;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.Duration;
import io.weaviate.client6.v1.api.collections.query.ConsistencyLevel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWeaviateConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(WeaviateConfig.class)
                .setScheme("http")
                .setHttpHost("localhost")
                .setHttpPort(8080)
                .setGrpcHost("localhost")
                .setGrpcPort(50051)
                .setTimeout(null)
                .setConsistencyLevel(null)
                .setApiKey(null)
                .setAccessToken(null)
                .setRefreshToken(null)
                .setAccessTokenLifetime(null)
                .setUsername(null)
                .setPassword(null)
                .setClientSecret(null)
                .setScopes(emptyList())
                .setPageSize(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("weaviate.scheme", "https")
                .put("weaviate.http-host", "192.0.0.1")
                .put("weaviate.http-port", "7070")
                .put("weaviate.grpc-host", "192.0.0.2")
                .put("weaviate.grpc-port", "60061")
                .put("weaviate.timeout", "30s")
                .put("weaviate.consistency-level", "ONE")
                .put("weaviate.auth.api-key", "api-key")
                .put("weaviate.auth.access-token", "access-token")
                .put("weaviate.auth.refresh-token", "refresh-token")
                .put("weaviate.auth.access-token-lifetime", "900s")
                .put("weaviate.oidc.username", "john_doe")
                .put("weaviate.oidc.password", "qwerty")
                .put("weaviate.oidc.client-secret", "XXX-XXX")
                .put("weaviate.oidc.scopes", "a,b")
                .put("weaviate.page-size", "5000")
                .buildOrThrow();

        try (ConfigurationFactory configurationFactory = new ConfigurationFactory(properties)) {
            WeaviateConfig config = configurationFactory.build(WeaviateConfig.class);

            assertThat(config).returns("https", WeaviateConfig::getScheme);
            assertThat(config).returns("192.0.0.1", WeaviateConfig::getHttpHost);
            assertThat(config).returns(7070, WeaviateConfig::getHttpPort);
            assertThat(config).returns("192.0.0.2", WeaviateConfig::getGrpcHost);
            assertThat(config).returns(60061, WeaviateConfig::getGrpcPort);
            assertThat(config).returns(Optional.of(Duration.valueOf("30s")), WeaviateConfig::getTimeout);
            assertThat(config).returns(ConsistencyLevel.ONE, WeaviateConfig::getConsistencyLevel);
            assertThat(config).returns(Optional.of("api-key"), WeaviateConfig::getApiKey);
            assertThat(config).returns(Optional.of("access-token"), WeaviateConfig::getAccessToken);
            assertThat(config).returns(Optional.of("refresh-token"), WeaviateConfig::getRefreshToken);
            assertThat(config).returns(Optional.of(Duration.valueOf("900s")), WeaviateConfig::getAccessTokenLifetime);
            assertThat(config).returns(Optional.of("john_doe"), WeaviateConfig::getUsername);
            assertThat(config).returns(Optional.of("qwerty"), WeaviateConfig::getPassword);
            assertThat(config).returns(Optional.of("XXX-XXX"), WeaviateConfig::getClientSecret);
            assertThat(config).returns(List.of("a", "b"), WeaviateConfig::getScopes);
            assertThat(config).returns(OptionalInt.of(5_000), WeaviateConfig::getPageSize);
        }
    }
}
