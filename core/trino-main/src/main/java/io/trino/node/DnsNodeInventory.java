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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.InetAddresses;
import com.google.inject.Inject;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

public class DnsNodeInventory
        implements NodeInventory
{
    private final boolean https;
    private final int port;
    private final Set<String> hosts;
    private final HostResolver hostResolver;

    @Inject
    public DnsNodeInventory(InternalNode currentNode, DnsNodeInventoryConfig config)
    {
        this(currentNode.getInternalUri().getScheme().equals("https"),
                currentNode.getInternalUri().getPort(),
                config.getHosts(),
                DnsNodeInventory::getAllByName);
    }

    @VisibleForTesting
    DnsNodeInventory(boolean https, int port, Set<String> hosts, HostResolver hostResolver)
    {
        this.https = https;
        this.port = port;
        checkArgument(port > 0 && port < 65536, "invalid port: %s", port);
        this.hosts = hosts.stream()
                .filter(not(Strings::isNullOrEmpty))
                .collect(toImmutableSet());
        this.hostResolver = requireNonNull(hostResolver, "hostResolver is null");
    }

    @Override
    public Set<URI> getNodes()
    {
        return hosts.stream()
                .map(hostResolver::getAllByName)
                .flatMap(Set::stream)
                .map(address -> {
                    try {
                        return new URI(https ? "https" : "http", null, InetAddresses.toUriString(address), port, null, null, null);
                    }
                    catch (URISyntaxException e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toImmutableSet());
    }

    interface HostResolver
    {
        Set<InetAddress> getAllByName(String dnsName);
    }

    private static Set<InetAddress> getAllByName(String dnsName)
    {
        try {
            return ImmutableSet.copyOf(InetAddress.getAllByName(dnsName));
        }
        catch (RuntimeException | UnknownHostException e) {
            return ImmutableSet.of();
        }
    }
}
