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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

final class TestMemoryConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(MemoryConfig.class)
                .setSplitsPerNode(Runtime.getRuntime().availableProcessors())
                .setMaxDataPerNode(DataSize.of(128, MEGABYTE))
                .setEnableLazyDynamicFiltering(true));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("memory.splits-per-node", "100")
                .put("memory.max-data-per-node", "1GB")
                .put("memory.enable-lazy-dynamic-filtering", "false")
                .buildOrThrow();

        MemoryConfig expected = new MemoryConfig()
                .setSplitsPerNode(100)
                .setMaxDataPerNode(DataSize.of(1, GIGABYTE))
                .setEnableLazyDynamicFiltering(false);

        assertFullMapping(properties, expected);
    }
}
