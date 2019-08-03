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
package io.prestosql.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestTopologyAwareNodeSelectorConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(TopologyAwareNodeSelectorConfig.class)
                .setLocationSegmentNames("machine")
                .setNetworkTopologyFile("")
                .setRefreshPeriod(new Duration(12, HOURS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("node-scheduler.network-topology-segments", "rack,machine")
                .put("node-scheduler.network-topology-file", "/etc/topology.txt")
                .put("node-scheduler.refresh-period", "5m")
                .build();

        TopologyAwareNodeSelectorConfig expected = new TopologyAwareNodeSelectorConfig()
                .setLocationSegmentNames(ImmutableList.of("rack", "machine"))
                .setNetworkTopologyFile("/etc/topology.txt")
                .setRefreshPeriod(new Duration(5, MINUTES));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
