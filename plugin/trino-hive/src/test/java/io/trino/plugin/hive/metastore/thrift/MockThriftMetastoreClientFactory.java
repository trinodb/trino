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
package io.prestosql.plugin.hive.metastore.thrift;

import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import org.apache.thrift.transport.TTransportException;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class MockThriftMetastoreClientFactory
        extends DefaultThriftMetastoreClientFactory
{
    private Map<HostAndPort, Optional<ThriftMetastoreClient>> clients;

    public MockThriftMetastoreClientFactory(Optional<HostAndPort> socksProxy, Duration timeout, Map<String, Optional<ThriftMetastoreClient>> clients)
    {
        super(Optional.empty(), socksProxy, timeout, new NoHiveMetastoreAuthentication(), "localhost");
        this.clients = clients.entrySet().stream().collect(Collectors.toMap(entry -> createHostAndPort(entry.getKey()), Map.Entry::getValue));
    }

    @Override
    public ThriftMetastoreClient create(HostAndPort address, Optional<String> delegationToken)
            throws TTransportException
    {
        checkArgument(delegationToken.isEmpty(), "delegation token is not supported");
        Optional<ThriftMetastoreClient> client = clients.getOrDefault(address, Optional.empty());
        if (client.isEmpty()) {
            throw new TTransportException(TTransportException.TIMED_OUT);
        }
        return client.get();
    }

    private static HostAndPort createHostAndPort(String str)
    {
        URI uri = URI.create(str);
        return HostAndPort.fromParts(uri.getHost(), uri.getPort());
    }
}
