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
package io.prestosql.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestConfluentSchemaRegistryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ConfluentSchemaRegistryConfig.class)
                .setConfluentSchemaRegistryUrl(null)
                .setConfluentSchemaRegistryClientCacheSize(1000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("kafka.confluent-schema-registry-url", "http://schema-registry:8081")
                .put("kafka.confluent-schema-registry-client-cache-size", "1500")
                .build();

        ConfluentSchemaRegistryConfig expected = new ConfluentSchemaRegistryConfig()
                .setConfluentSchemaRegistryUrl("http://schema-registry:8081")
                .setConfluentSchemaRegistryClientCacheSize(1500);

        assertFullMapping(properties, expected);
    }
}
