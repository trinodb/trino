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
package io.trino.tests.product.utils;

import com.google.common.collect.ImmutableMap;
import io.trino.client.DnsResolver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class HostMappingDnsResolver
        implements DnsResolver
{
    private final Map<String, InetAddress> hostMapping;

    public HostMappingDnsResolver(String context)
            throws UnknownHostException
    {
        requireNonNull(context, "context is null");

        ImmutableMap.Builder<String, InetAddress> builder = ImmutableMap.builder();
        for (String entry : context.split(";")) {
            if (entry.isBlank()) {
                continue;
            }

            String[] parts = entry.split("=", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid DNS resolver mapping: " + entry);
            }
            builder.put(parts[0], InetAddress.getByName(parts[1]));
        }
        hostMapping = builder.buildOrThrow();
    }

    @Override
    public List<InetAddress> lookup(String hostname)
            throws UnknownHostException
    {
        InetAddress address = hostMapping.get(hostname);
        if (address != null) {
            return List.of(address);
        }
        return List.of(InetAddress.getAllByName(hostname));
    }
}
