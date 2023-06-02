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

import org.apache.thrift.transport.TTransportException;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class MockThriftMetastoreClientFactory
        implements ThriftMetastoreClientFactory
{
    private final Map<URI, Optional<ThriftMetastoreClient>> clients;

    public MockThriftMetastoreClientFactory(Map<String, Optional<ThriftMetastoreClient>> clients)
    {
        this.clients = clients.entrySet().stream()
                .collect(toImmutableMap(entry -> URI.create(entry.getKey()), Map.Entry::getValue));
    }

    @Override
    public ThriftMetastoreClient create(URI uri, Optional<String> delegationToken)
            throws TTransportException
    {
        checkArgument(delegationToken.isEmpty(), "delegation token is not supported");
        return clients.getOrDefault(uri, Optional.empty())
                .orElseThrow(() -> new TTransportException(TTransportException.TIMED_OUT));
    }
}
