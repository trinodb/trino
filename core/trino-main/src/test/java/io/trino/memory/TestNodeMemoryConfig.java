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
package io.trino.memory;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.memory.NodeMemoryConfig.AVAILABLE_HEAP_MEMORY;

public class TestNodeMemoryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(NodeMemoryConfig.class)
                .setMaxQueryMemoryPerNode(DataSize.ofBytes(Math.round(AVAILABLE_HEAP_MEMORY * 0.1)))
                .setMaxQueryTotalMemoryPerNode(DataSize.ofBytes(Math.round(AVAILABLE_HEAP_MEMORY * 0.3)))
                .setMaxQueryTotalMemoryPerTask(null)
                .setHeapHeadroom(DataSize.ofBytes(Math.round(AVAILABLE_HEAP_MEMORY * 0.3)))
                .setReservedPoolDisabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("query.max-memory-per-node", "1GB")
                .put("query.max-total-memory-per-node", "3GB")
                .put("query.max-total-memory-per-task", "200MB")
                .put("memory.heap-headroom-per-node", "1GB")
                .put("experimental.reserved-pool-disabled", "false")
                .buildOrThrow();

        NodeMemoryConfig expected = new NodeMemoryConfig()
                .setMaxQueryMemoryPerNode(DataSize.of(1, GIGABYTE))
                .setMaxQueryTotalMemoryPerNode(DataSize.of(3, GIGABYTE))
                .setMaxQueryTotalMemoryPerTask(DataSize.of(200, MEGABYTE))
                .setHeapHeadroom(DataSize.of(1, GIGABYTE))
                .setReservedPoolDisabled(false);

        assertFullMapping(properties, expected);
    }
}
