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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.trino.execution.scheduler.TopologyAwareNodeSelectorConfig.TopologyType;
import org.testng.annotations.Test;

import java.util.Map;

public class TestTopologyAwareNodeSelectorConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(TopologyAwareNodeSelectorConfig.class)
                .setType(TopologyType.FLAT)
                .setLocationSegmentNames("machine"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("node-scheduler.network-topology.type", "FILE")
                .put("node-scheduler.network-topology.segments", "rack,machine")
                .buildOrThrow();

        TopologyAwareNodeSelectorConfig expected = new TopologyAwareNodeSelectorConfig()
                .setType(TopologyType.FILE)
                .setLocationSegmentNames(ImmutableList.of("rack", "machine"));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
