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
import io.trino.spi.HostAddress;
import org.testng.annotations.Test;

import static io.trino.execution.scheduler.NetworkLocation.ROOT_LOCATION;
import static io.trino.execution.scheduler.SubnetBasedTopology.AddressProtocol.IPv4;
import static io.trino.execution.scheduler.SubnetBasedTopology.AddressProtocol.IPv6;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSubnetTopology
{
    @Test
    public void testSubnetTopologyIpv4()
    {
        SubnetBasedTopology topology = new SubnetBasedTopology(ImmutableList.of(24, 25, 27), IPv4);
        assertThat(topology.locate(HostAddress.fromString("192.168.0.172"))).isEqualTo(new NetworkLocation("192.168.0.0", "192.168.0.128", "192.168.0.160", "192.168.0.172"));

        SubnetBasedTopology noTopology = new SubnetBasedTopology(ImmutableList.of(), IPv4);
        assertThat(noTopology.locate(HostAddress.fromString("192.168.0.172"))).isEqualTo(new NetworkLocation("192.168.0.172"));

        // root location returned for IPv6 address
        assertThat(topology.locate(HostAddress.fromString("2001:db8:0:0:1:0:0:1"))).isEqualTo(ROOT_LOCATION);
        assertThat(noTopology.locate(HostAddress.fromString("2001:db8:0:0:1:0:0:1"))).isEqualTo(ROOT_LOCATION);
    }

    @Test
    public void testSubnetTopologyIpv6()
    {
        SubnetBasedTopology topology = new SubnetBasedTopology(ImmutableList.of(96, 110, 112, 120), IPv6);
        assertThat(topology.locate(HostAddress.fromString("2001:db8::ff00:42:8329"))).isEqualTo(new NetworkLocation(
                        "2001:db8::ff00:0:0",
                        "2001:db8::ff00:40:0",
                        "2001:db8::ff00:42:0",
                        "2001:db8::ff00:42:8300",
                        "2001:db8::ff00:42:8329"));

        SubnetBasedTopology noTopology = new SubnetBasedTopology(ImmutableList.of(), IPv6);
        assertThat(noTopology.locate(HostAddress.fromString("2001:db8::ff00:42:8329"))).isEqualTo(new NetworkLocation("2001:db8::ff00:42:8329"));

        // root location returned for IPv4 address
        assertThat(topology.locate(HostAddress.fromString("192.168.0.1"))).isEqualTo(ROOT_LOCATION);
        assertThat(noTopology.locate(HostAddress.fromString("192.168.0.1"))).isEqualTo(ROOT_LOCATION);
    }

    @Test
    public void testBadInitialization()
    {
        assertThatThrownBy(() -> new SubnetBasedTopology(ImmutableList.of(25, 34), IPv4)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubnetBasedTopology(ImmutableList.of(-3, 5), IPv4)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubnetBasedTopology(ImmutableList.of(25, 22), IPv4)).isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> new SubnetBasedTopology(ImmutableList.of(95, 130), IPv6)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubnetBasedTopology(ImmutableList.of(-3, 100), IPv6)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SubnetBasedTopology(ImmutableList.of(122, 120), IPv6)).isInstanceOf(IllegalArgumentException.class);
    }
}
