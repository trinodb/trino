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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class TestingKuduServer
        implements Closeable
{
    private static final Integer KUDU_MASTER_PORT = 7051;
    private static final Integer KUDU_TSERVER_PORT = 7050;
    private static final Integer NUMBER_OF_REPLICA = 3;
    private static final String KUDU_VERSION = "1.15.0";

    private final GenericContainer<?> master;
    private final List<GenericContainer<?>> tServers;

    public TestingKuduServer()
    {
        Network network = Network.newNetwork();
        ImmutableList.Builder<GenericContainer<?>> tServersBuilder = ImmutableList.builder();
        this.master = new GenericContainer<>("apache/kudu:" + KUDU_VERSION)
                .withExposedPorts(KUDU_MASTER_PORT)
                .withCommand("master")
                .withNetwork(network)
                .withNetworkAliases("kudu-master");

        for (int instance = 0; instance < NUMBER_OF_REPLICA; instance++) {
            String instanceName = "kudu-tserver-" + instance;
            GenericContainer<?> tableServer = new GenericContainer<>("apache/kudu:" + KUDU_VERSION)
                    .withExposedPorts(KUDU_TSERVER_PORT)
                    .withCommand("tserver")
                    .withEnv("KUDU_MASTERS", "kudu-master:" + KUDU_MASTER_PORT)
                    .withNetwork(network)
                    .withNetworkAliases("kudu-tserver-" + instance)
                    .dependsOn(master)
                    .withEnv("TSERVER_ARGS", "--fs_wal_dir=/var/lib/kudu/tserver --use_hybrid_clock=false --rpc_advertised_addresses=" + instanceName);
            tServersBuilder.add(tableServer);
        }
        this.tServers = tServersBuilder.build();
        master.start();
        tServers.forEach(GenericContainer::start);
    }

    public HostAndPort getMasterAddress()
    {
        return HostAndPort.fromParts(master.getContainerIpAddress(), master.getMappedPort(KUDU_MASTER_PORT));
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            closer.register(master::stop);
            tServers.forEach(tabletServer -> closer.register(tabletServer::stop));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
