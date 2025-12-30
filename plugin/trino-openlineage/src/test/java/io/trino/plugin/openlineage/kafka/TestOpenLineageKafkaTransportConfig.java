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
package io.trino.plugin.openlineage.kafka;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.plugin.openlineage.transport.kafka.OpenLineageKafkaTransportConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestOpenLineageKafkaTransportConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(OpenLineageKafkaTransportConfig.class)
                .setBrokerEndpoints(null)
                .setTopicName(null)
                .setClientId(null)
                .setRequestTimeout(Duration.valueOf("10s"))
                .setResourceConfigFiles(List.of())
                .setMessageKey(null));
    }

    @Test
    void testExplicitPropertyMappings()
            throws IOException
    {
        Path resource1 = Files.createTempFile(null, null);
        Path resource2 = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("openlineage-event-listener.kafka-transport.broker-endpoints", "kafka1:9092,kafka2:9092")
                .put("openlineage-event-listener.kafka-transport.topic-name", "openlineage-events")
                .put("openlineage-event-listener.kafka-transport.client-id", "trino-test")
                .put("openlineage-event-listener.kafka-transport.request-timeout", "30s")
                .put("openlineage-event-listener.kafka-transport.config.resources", resource1.toString() + "," + resource2.toString())
                .put("openlineage-event-listener.kafka-transport.message-key", "custom-key")
                .buildOrThrow();

        OpenLineageKafkaTransportConfig expected = new OpenLineageKafkaTransportConfig()
                .setBrokerEndpoints("kafka1:9092,kafka2:9092")
                .setTopicName("openlineage-events")
                .setClientId("trino-test")
                .setRequestTimeout(Duration.valueOf("30s"))
                .setResourceConfigFiles(ImmutableList.of(resource1.toString(), resource2.toString()))
                .setMessageKey("custom-key");

        assertFullMapping(properties, expected);
    }
}
