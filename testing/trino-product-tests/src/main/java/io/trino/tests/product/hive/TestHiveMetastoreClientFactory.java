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
package io.trino.tests.product.hive;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.airlift.units.Duration;
import io.trino.plugin.hive.metastore.thrift.DefaultThriftMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.NoHiveMetastoreAuthentication;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClientFactory;
import org.apache.thrift.TException;

import java.net.URI;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class TestHiveMetastoreClientFactory
{
    private final ThriftMetastoreClientFactory thriftMetastoreClientFactory = new DefaultThriftMetastoreClientFactory(
            Optional.empty(),
            Optional.empty(),
            new Duration(10, SECONDS),
            new NoHiveMetastoreAuthentication(),
            "localhost");

    @Inject
    @Named("databases.hive.metastore.host")
    private String metastoreHost;

    @Inject
    @Named("databases.hive.metastore.port")
    private int metastorePort;

    public ThriftMetastoreClient createMetastoreClient()
            throws TException
    {
        URI metastore = URI.create("thrift://" + metastoreHost + ":" + metastorePort);
        return thriftMetastoreClientFactory.create(
                HostAndPort.fromParts(metastore.getHost(), metastore.getPort()),
                Optional.empty());
    }
}
