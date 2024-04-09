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
import io.airlift.units.Duration;
import org.apache.thrift.TException;

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestingTokenAwareMetastoreClientFactory
        implements TokenAwareMetastoreClientFactory
{
    private static final HiveMetastoreAuthentication AUTHENTICATION = new NoHiveMetastoreAuthentication();
    public static final Duration TIMEOUT = new Duration(20, SECONDS);

    private final DefaultThriftMetastoreClientFactory factory;
    private final URI address;

    private final MetastoreClientAdapterProvider metastoreClientAdapterProvider;

    public TestingTokenAwareMetastoreClientFactory(Optional<HostAndPort> socksProxy, URI uri)
    {
        this(socksProxy, uri, TIMEOUT, delegate -> delegate);
    }

    public TestingTokenAwareMetastoreClientFactory(Optional<HostAndPort> socksProxy, URI address, Duration timeout)
    {
        this(socksProxy, address, timeout, delegate -> delegate);
    }

    public TestingTokenAwareMetastoreClientFactory(Optional<HostAndPort> socksProxy, URI uri, Duration timeout, MetastoreClientAdapterProvider metastoreClientAdapterProvider)
    {
        this.factory = new DefaultThriftMetastoreClientFactory(Optional.empty(), socksProxy, timeout, timeout, AUTHENTICATION, "localhost");
        this.address = requireNonNull(uri, "uri is null");
        this.metastoreClientAdapterProvider = requireNonNull(metastoreClientAdapterProvider, "metastoreClientAdapterProvider is null");
    }

    @Override
    public ThriftMetastoreClient createMetastoreClient(Optional<String> delegationToken)
            throws TException
    {
        return metastoreClientAdapterProvider.createThriftMetastoreClientAdapter(factory.create(address, delegationToken));
    }
}
