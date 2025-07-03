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

import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import io.trino.node.DnsNodeInventory.HostResolver;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class TestDnsNodeInventory
{
    private static final HostResolver RESOLVER = host -> switch (host) {
        case "example.com" -> ImmutableSet.of(InetAddresses.forString("10.0.0.1"), InetAddresses.forString("10.0.0.2"));
        case "other.example.org" -> ImmutableSet.of(InetAddresses.forString("fd00::1:2:3:4"), InetAddresses.forString("fd00::1:2:3:5"));
        default -> {
            if (InetAddresses.isInetAddress(host)) {
                yield ImmutableSet.of(InetAddresses.forString(host));
            }
            throw new IllegalArgumentException("Unknown host: " + host);
        }
    };

    @Test
    void simple()
    {
        assertThat(new DnsNodeInventory(true, 7777, Set.of("example.com", "other.example.org"), RESOLVER).getNodes())
                .containsExactlyInAnyOrder(
                        URI.create("https://10.0.0.1:7777"),
                        URI.create("https://10.0.0.2:7777"),
                        URI.create("https://[fd00::1:2:3:4]:7777"),
                        URI.create("https://[fd00::1:2:3:5]:7777"));
        assertThat(new DnsNodeInventory(true, 7777, Set.of("", "other.example.org"), RESOLVER).getNodes())
                .containsExactlyInAnyOrder(
                        URI.create("https://[fd00::1:2:3:4]:7777"),
                        URI.create("https://[fd00::1:2:3:5]:7777"));
        assertThat(new DnsNodeInventory(true, 7777, Set.of("10.0.0.9", "fd00::1:2:3:9"), RESOLVER).getNodes())
                .containsExactlyInAnyOrder(
                        URI.create("https://10.0.0.9:7777"),
                        URI.create("https://[fd00::1:2:3:9]:7777"));
    }
}
