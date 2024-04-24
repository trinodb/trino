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

import com.google.common.io.Closer;
import com.google.common.net.HostAndPort;
import io.trino.testing.ResourcePresence;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static java.lang.String.format;

public class TestingKuduServer
        implements Closeable
{
    private static final String KUDU_IMAGE = "apache/kudu";
    public static final String EARLIEST_TAG = "1.13.0";
    public static final String LATEST_TAG = "1.17";

    private static final Integer KUDU_MASTER_PORT = 7051;
    private static final Integer KUDU_TSERVER_PORT = 7050;

    private static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.4.0";
    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";

    private final Network network;
    private final ToxiproxyContainer toxiProxy;
    private final GenericContainer<?> master;
    private final GenericContainer<?> tabletServer;

    private boolean stopped;

    public TestingKuduServer()
    {
        this(LATEST_TAG);
    }

    /**
     * Kudu tablets needs to know the host/mapped port it will be bound to in order to configure --rpc_advertised_addresses
     * However when using non-fixed ports in testcontainers, we only know the mapped port after the container starts up
     * In order to workaround this, create a proxy to forward traffic from the host to the underlying tablets
     * Since the ToxiProxy container starts up *before* kudu, we know the mapped port when configuring the kudu tablets
     */
    public TestingKuduServer(String kuduVersion)
    {
        network = Network.newNetwork();
        String masterContainerAlias = "kudu-master";
        this.master = new GenericContainer<>(format("%s:%s", KUDU_IMAGE, kuduVersion))
                .withExposedPorts(KUDU_MASTER_PORT)
                .withCommand("master")
                .withEnv("MASTER_ARGS", "--default_num_replicas=1 --unlock_unsafe_flags --use_hybrid_clock=false")
                .withNetwork(network)
                .withNetworkAliases(masterContainerAlias);

        toxiProxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
        toxiProxy.start();

        String instanceName = "kudu-tserver";
        ToxiproxyContainer.ContainerProxy proxy = toxiProxy.getProxy(instanceName, KUDU_TSERVER_PORT);
        tabletServer = new GenericContainer<>(format("%s:%s", KUDU_IMAGE, kuduVersion))
                .withExposedPorts(KUDU_TSERVER_PORT)
                .withCommand("tserver")
                .withEnv("KUDU_MASTERS", format("%s:%s", masterContainerAlias, KUDU_MASTER_PORT))
                .withEnv("TSERVER_ARGS", format("--fs_wal_dir=/var/lib/kudu/tserver --logtostderr --use_hybrid_clock=false --unlock_unsafe_flags --rpc_bind_addresses=%s:%s --rpc_advertised_addresses=%s:%s", instanceName, KUDU_TSERVER_PORT, TOXIPROXY_NETWORK_ALIAS, proxy.getOriginalProxyPort()))
                .withNetwork(network)
                .withNetworkAliases(instanceName)
                .waitingFor(new KuduTabletWaitStrategy(master))
                .dependsOn(master);

        master.start();
        tabletServer.start();
    }

    public HostAndPort getMasterAddress()
    {
        // Do not use master.getHost(), it returns "localhost" which the kudu client resolves to:
        // localhost/127.0.0.1, localhost/0:0:0:0:0:0:0:1
        // Instead explicitly list only the ipv4 loopback address 127.0.0.1
        return HostAndPort.fromParts("127.0.0.1", master.getMappedPort(KUDU_MASTER_PORT));
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            closer.register(master::stop);
            closer.register(tabletServer::stop);
            closer.register(toxiProxy::stop);
            closer.register(network::close);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        stopped = true;
    }

    @ResourcePresence
    public boolean isNotStopped()
    {
        return !stopped;
    }

    private static String getHostIPAddress()
    {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
