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

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
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
                .setConsistencyLevel(null));
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
                .buildOrThrow();

        try (ConfigurationFactory configurationFactory = new ConfigurationFactory(properties)) {
            WeaviateConfig config = configurationFactory.build(WeaviateConfig.class);

            assertThat(config).returns("https", WeaviateConfig::getScheme);
            assertThat(config).returns("192.0.0.1", WeaviateConfig::getHttpHost);
            assertThat(config).returns(7070, WeaviateConfig::getHttpPort);
            assertThat(config).returns("192.0.0.2", WeaviateConfig::getGrpcHost);
            assertThat(config).returns(60061, WeaviateConfig::getGrpcPort);
            assertThat(config).returns(Duration.valueOf("30s"), WeaviateConfig::getTimeout);
            assertThat(config).returns(ConsistencyLevel.ONE, WeaviateConfig::getConsistencyLevel);
        }
    }
}
