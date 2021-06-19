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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.scheduler.SubnetBasedTopology.AddressProtocol.IPv4;

public class SubnetTopologyConfig
{
    private SubnetBasedTopology.AddressProtocol addressProtocol = IPv4;
    private List<Integer> cidrPrefixLengths = ImmutableList.of();

    public List<Integer> getCidrPrefixLengths()
    {
        return cidrPrefixLengths;
    }

    @Config("node-scheduler.network-topology.subnet.cidr-prefix-lengths")
    public SubnetTopologyConfig setCidrPrefixLengths(String commaSeparatedLengths)
    {
        this.cidrPrefixLengths = Splitter.on(',').omitEmptyStrings().splitToList(commaSeparatedLengths).stream()
                .map(Integer::parseInt)
                .collect(toImmutableList());
        return this;
    }

    public SubnetBasedTopology.AddressProtocol getAddressProtocol()
    {
        return addressProtocol;
    }

    @Config("node-scheduler.network-topology.subnet.ip-address-protocol")
    public SubnetTopologyConfig setAddressProtocol(SubnetBasedTopology.AddressProtocol addressProtocol)
    {
        this.addressProtocol = addressProtocol;
        return this;
    }
}
