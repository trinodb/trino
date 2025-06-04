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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.IGNORE;
import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.MARK;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestConfluentSchemaRegistryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ConfluentSchemaRegistryConfig.class)
                .setConfluentSchemaRegistryUrls(ImmutableSet.of())
                .setConfluentSchemaRegistryClientCacheSize(1000)
                .setEmptyFieldStrategy(IGNORE)
                .setConfluentSubjectsCacheRefreshInterval(new Duration(1, SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("kafka.confluent-schema-registry-url", "http://schema-registry-a:8081, http://schema-registry-b:8081")
                .put("kafka.confluent-schema-registry-client-cache-size", "1500")
                .put("kafka.empty-field-strategy", "MARK")
                .put("kafka.confluent-subjects-cache-refresh-interval", "2s")
                .buildOrThrow();

        ConfluentSchemaRegistryConfig expected = new ConfluentSchemaRegistryConfig()
                .setConfluentSchemaRegistryUrls(ImmutableSet.of("http://schema-registry-a:8081", "http://schema-registry-b:8081"))
                .setConfluentSchemaRegistryClientCacheSize(1500)
                .setEmptyFieldStrategy(MARK)
                .setConfluentSubjectsCacheRefreshInterval(new Duration(2, SECONDS));

        assertFullMapping(properties, expected);
    }
}
