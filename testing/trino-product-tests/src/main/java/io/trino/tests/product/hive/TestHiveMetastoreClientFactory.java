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
import io.trino.plugin.hive.metastore.thrift.NoHiveMetastoreAuthentication;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient;
import io.trino.plugin.hive.metastore.thrift.Transport;
import org.apache.thrift.TException;

import java.net.URI;
import java.util.Optional;

public final class TestHiveMetastoreClientFactory
{
    private static final String LOCALHOST = "localhost";

    @Inject
    @Named("databases.hive.metastore.host")
    private String metastoreHost;

    @Inject
    @Named("databases.hive.metastore.port")
    private int metastorePort;

    ThriftHiveMetastoreClient createMetastoreClient()
            throws TException
    {
        URI metastore = URI.create("thrift://" + metastoreHost + ":" + metastorePort);
        return new ThriftHiveMetastoreClient(
                Transport.create(
                        HostAndPort.fromParts(metastore.getHost(), metastore.getPort()),
                        Optional.empty(),
                        Optional.empty(),
                        10000,
                        new NoHiveMetastoreAuthentication(),
                        Optional.empty()), LOCALHOST);
    }
}
