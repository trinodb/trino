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
package io.trino.node;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.trino.node.NodeInventoryConfig.NodeInventoryType.AIRLIFT_DISCOVERY;
import static io.trino.node.NodeInventoryConfig.NodeInventoryType.ANNOUNCE;

class TestNodeInventoryConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(NodeInventoryConfig.class)
                .setType(ANNOUNCE));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("discovery.type", "airlift-discovery")
                .buildOrThrow();

        NodeInventoryConfig expected = new NodeInventoryConfig()
                .setType(AIRLIFT_DISCOVERY);

        assertFullMapping(properties, expected);
    }
}
