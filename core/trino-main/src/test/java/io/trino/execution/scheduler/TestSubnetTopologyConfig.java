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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static io.trino.execution.scheduler.SubnetBasedTopology.AddressProtocol.IPv4;
import static io.trino.execution.scheduler.SubnetBasedTopology.AddressProtocol.IPv6;

public class TestSubnetTopologyConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(SubnetTopologyConfig.class)
                .setCidrPrefixLengths("")
                .setAddressProtocol(IPv4));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("node-scheduler.network-topology.subnet.cidr-prefix-lengths", "24,26")
                .put("node-scheduler.network-topology.subnet.ip-address-protocol", "IPv6")
                .buildOrThrow();

        SubnetTopologyConfig expected = new SubnetTopologyConfig()
                .setCidrPrefixLengths("24,26")
                .setAddressProtocol(IPv6);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
