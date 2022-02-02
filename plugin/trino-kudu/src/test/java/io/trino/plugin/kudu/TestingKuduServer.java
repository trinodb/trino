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
import org.testcontainers.containers.ToxiproxyContainer;

import java.io.Closeable;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;

import static java.lang.String.format;

public class TestingKuduServer
        implements Closeable
{
    private static final String KUDU_IMAGE = "apache/kudu";
    private static final Integer KUDU_MASTER_PORT = 7051;
    private static final Integer KUDU_TSERVER_PORT = 7050;
    private static final Integer NUMBER_OF_REPLICA = 3;

    private static final String TOXIPROXY_IMAGE = "shopify/toxiproxy:2.1.0";
    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";

    private final Network network;
    private final ToxiproxyContainer toxiProxy;
    private final GenericContainer<?> master;
    private final List<GenericContainer<?>> tServers;

    public TestingKuduServer()
    {
        // This version should match the kudu client version
        this("1.15.0");
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
        ImmutableList.Builder<GenericContainer<?>> tServersBuilder = ImmutableList.builder();

        String hostIP = getHostIPAddress();

        String masterContainerAlias = "kudu-master";
        this.master = new GenericContainer<>(format("%s:%s", KUDU_IMAGE, kuduVersion))
                .withExposedPorts(KUDU_MASTER_PORT)
                .withCommand("master")
                .withNetwork(network)
                .withNetworkAliases(masterContainerAlias);

        toxiProxy = new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
        toxiProxy.start();

        for (int instance = 0; instance < NUMBER_OF_REPLICA; instance++) {
            String instanceName = "kudu-tserver-" + instance;
            ToxiproxyContainer.ContainerProxy proxy = toxiProxy.getProxy(instanceName, KUDU_TSERVER_PORT);
            GenericContainer<?> tableServer = new GenericContainer<>(format("%s:%s", KUDU_IMAGE, kuduVersion))
                    .withExposedPorts(KUDU_TSERVER_PORT)
                    .withCommand("tserver")
                    .withEnv("KUDU_MASTERS", format("%s:%s", masterContainerAlias, KUDU_MASTER_PORT))
                    .withEnv("TSERVER_ARGS", format("--fs_wal_dir=/var/lib/kudu/tserver --logtostderr --use_hybrid_clock=false --rpc_bind_addresses=%s:%s --rpc_advertised_addresses=%s:%s", instanceName, KUDU_TSERVER_PORT, hostIP, proxy.getProxyPort()))
                    .withNetwork(network)
                    .withNetworkAliases(instanceName)
                    .dependsOn(master);

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
            closer.register(toxiProxy::stop);
            closer.register(network::close);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getHostIPAddress()
    {
        // Binding kudu's `rpc_advertised_addresses` to 127.0.0.1 inside the container will not bind to the host's loopback address
        // As a workaround, use a site local ipv4 address from the host
        // This is roughly equivalent to setting the KUDU_QUICKSTART_IP defined here: https://kudu.apache.org/docs/quickstart.html#_set_kudu_quickstart_ip
        try {
            Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaceEnumeration.hasMoreElements()) {
                for (InterfaceAddress interfaceAddress : networkInterfaceEnumeration.nextElement().getInterfaceAddresses()) {
                    if (interfaceAddress.getAddress().isSiteLocalAddress() && interfaceAddress.getAddress() instanceof Inet4Address) {
                        return interfaceAddress.getAddress().getHostAddress();
                    }
                }
            }
        }
        catch (SocketException e) {
            throw new RuntimeException(e);
        }
        throw new IllegalStateException("Could not find site local ipv4 address, failed to launch kudu");
    }
}
