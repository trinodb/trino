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
package io.trino.plugin.accumulo.conf;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

class TestAccumuloConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(AccumuloConfig.class)
                .setInstance(null)
                .setZooKeepers(null)
                .setUsername(null)
                .setPassword(null)
                .setZkMetadataRoot("/trino-accumulo")
                .setCardinalityCacheSize(100_000)
                .setCardinalityCacheExpiration(new Duration(5, MINUTES)));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("accumulo.instance", "accumulo")
                .put("accumulo.zookeepers", "zookeeper.example.com:2181")
                .put("accumulo.username", "user")
                .put("accumulo.password", "password")
                .put("accumulo.zookeeper.metadata.root", "/trino-accumulo-metadata")
                .put("accumulo.cardinality.cache.size", "999")
                .put("accumulo.cardinality.cache.expire.duration", "1h")
                .buildOrThrow();

        AccumuloConfig expected = new AccumuloConfig()
                .setInstance("accumulo")
                .setZooKeepers("zookeeper.example.com:2181")
                .setUsername("user")
                .setPassword("password")
                .setZkMetadataRoot("/trino-accumulo-metadata")
                .setCardinalityCacheSize(999)
                .setCardinalityCacheExpiration(new Duration(1, HOURS));

        assertFullMapping(properties, expected);
    }
}
