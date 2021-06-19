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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.net.HostAndPort;
import org.apache.thrift.transport.TTransportException;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class MockThriftMetastoreClientFactory
        implements ThriftMetastoreClientFactory
{
    private final Map<HostAndPort, Optional<ThriftMetastoreClient>> clients;

    public MockThriftMetastoreClientFactory(Map<String, Optional<ThriftMetastoreClient>> clients)
    {
        this.clients = clients.entrySet().stream()
                .collect(toImmutableMap(entry -> createHostAndPort(entry.getKey()), Map.Entry::getValue));
    }

    @Override
    public ThriftMetastoreClient create(HostAndPort address, Optional<String> delegationToken)
            throws TTransportException
    {
        checkArgument(delegationToken.isEmpty(), "delegation token is not supported");
        return clients.getOrDefault(address, Optional.empty())
                .orElseThrow(() -> new TTransportException(TTransportException.TIMED_OUT));
    }

    private static HostAndPort createHostAndPort(String url)
    {
        URI uri = URI.create(url);
        checkArgument("thrift".equals(uri.getScheme()), "Invalid URL: %s", url);
        return HostAndPort.fromParts(uri.getHost(), uri.getPort());
    }
}
