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
package io.trino.tests.product.tls;

import io.trino.testing.containers.TrinoTestImages;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.tests.product.utils.HostMappingDnsResolver;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Three-node TLS environment used by SuiteTls.
 */
public class TlsEnvironment
        extends ProductTestEnvironment
{
    protected static final String COORDINATOR_FQDN = "trino-coordinator.docker.cluster";

    private static final int HTTP_PORT = 8080;
    private static final int HTTPS_PORT = 7778;
    private static final int EXPECTED_NODE_COUNT = 3;
    private static final String SHARED_SECRET = "internal-shared-secret";
    private static final String KEYSTORE_CONTAINER_PATH = "/etc/trino/trino-cluster.jks";
    private static final String KEYSTORE_RESOURCE_PATH = "testing/trino-product-tests/src/test/resources/tls/cert/trino-cluster.jks";
    private static final String TRUSTSTORE_PASSWORD = "123456";

    private Network network;
    private GenericContainer<?> coordinator;
    private List<GenericContainer<?>> workers = List.of();

    @Override
    public void start()
    {
        if (isRunning()) {
            return;
        }

        network = Network.newNetwork();
        try {
            startDependencies(network);

            coordinator = createTrinoContainer("trino-coordinator", true);
            workers = List.of(
                    createTrinoContainer("trino-worker-1", false),
                    createTrinoContainer("trino-worker-2", false));

            coordinator.start();
            for (GenericContainer<?> worker : workers) {
                worker.start();
            }

            waitUntilClusterReady();
        }
        catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    protected void startDependencies(Network network) {}

    protected String authenticationConfig()
    {
        return "";
    }

    protected void configureContainer(GenericContainer<?> container) {}

    protected void configureConnectionProperties(Properties properties) {}

    protected void closeDependencies() {}

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return createTrinoConnection("hive");
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("SSL", "true");
        properties.setProperty("SSLVerification", "FULL");
        properties.setProperty("SSLTrustStorePath", locateKeystore().toString());
        properties.setProperty("SSLTrustStorePassword", TRUSTSTORE_PASSWORD);
        properties.setProperty("dnsResolver", HostMappingDnsResolver.class.getName());
        properties.setProperty("dnsResolverContext", COORDINATOR_FQDN + "=" + coordinator.getHost());
        configureConnectionProperties(properties);
        return DriverManager.getConnection(
                "jdbc:trino://%s:%d".formatted(COORDINATOR_FQDN, coordinator.getMappedPort(HTTPS_PORT)),
                properties);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return coordinator == null
                ? null
                : "jdbc:trino://%s:%d".formatted(COORDINATOR_FQDN, coordinator.getMappedPort(HTTPS_PORT));
    }

    @Override
    public boolean isRunning()
    {
        return coordinator != null && coordinator.isRunning() && workers.stream().allMatch(GenericContainer::isRunning);
    }

    public List<NodeEndpoint> getNodes()
    {
        List<NodeEndpoint> nodes = new ArrayList<>(EXPECTED_NODE_COUNT);
        nodes.add(nodeEndpoint("trino-coordinator", coordinator));
        for (int index = 0; index < workers.size(); index++) {
            nodes.add(nodeEndpoint("trino-worker-" + (index + 1), workers.get(index)));
        }
        return List.copyOf(nodes);
    }

    @Override
    protected void doClose()
    {
        for (GenericContainer<?> worker : workers.reversed()) {
            worker.close();
        }
        workers = List.of();

        if (coordinator != null) {
            coordinator.close();
            coordinator = null;
        }

        closeDependencies();

        if (network != null) {
            network.close();
            network = null;
        }
    }

    private GenericContainer<?> createTrinoContainer(String hostName, boolean coordinatorNode)
    {
        String fqdn = hostName + ".docker.cluster";
        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(TrinoTestImages.getDefaultTrinoImage()))
                .withNetwork(network)
                .withNetworkAliases(hostName, fqdn)
                .withCreateContainerCmdModifier(command -> command.withHostName(hostName))
                .withCreateContainerCmdModifier(command -> command.withDomainName("docker.cluster"))
                .withCopyToContainer(Transferable.of(configProperties(hostName, coordinatorNode)), "/etc/trino/config.properties")
                .withCopyFileToContainer(MountableFile.forHostPath(locateKeystore()), KEYSTORE_CONTAINER_PATH)
                .withCopyToContainer(Transferable.of("connector.name=tpch\n"), "/etc/trino/catalog/tpch.properties")
                .waitingFor(Wait.forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(2)));
        container.addExposedPort(HTTP_PORT);
        container.addExposedPort(HTTPS_PORT);
        configureContainer(container);
        return container;
    }

    private String configProperties(String nodeId, boolean coordinatorNode)
    {
        return """
               node.id=%s
               node.environment=test
               node.internal-address-source=FQDN
               coordinator=%s
               %sdiscovery.uri=https://%s:7778
               query.max-memory=1GB
               query.max-memory-per-node=1GB
               http-server.http.enabled=false
               http-server.https.enabled=true
               http-server.https.port=7778
               http-server.https.keystore.path=%s
               http-server.https.keystore.key=%s
               %sinternal-communication.https.required=true
               internal-communication.shared-secret=%s
               internal-communication.https.keystore.path=%s
               internal-communication.https.keystore.key=%s
               http-server.log.enabled=false
               catalog.management=dynamic
               query.min-expire-age=1m
               task.info.max-age=1m
               """.formatted(
                nodeId,
                coordinatorNode,
                coordinatorNode ? "node-scheduler.include-coordinator=false\n" : "",
                COORDINATOR_FQDN,
                KEYSTORE_CONTAINER_PATH,
                TRUSTSTORE_PASSWORD,
                authenticationConfig(),
                SHARED_SECRET,
                KEYSTORE_CONTAINER_PATH,
                TRUSTSTORE_PASSWORD);
    }

    private void waitUntilClusterReady()
    {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
        SQLException lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = createTrinoConnection();
                    Statement statement = connection.createStatement();
                    ResultSet nodes = statement.executeQuery("SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'")) {
                if (nodes.next() && nodes.getInt(1) == EXPECTED_NODE_COUNT) {
                    try (ResultSet result = statement.executeQuery("SELECT count(*) FROM tpch.tiny.lineitem")) {
                        if (result.next() && result.getLong(1) == 60_175) {
                            return;
                        }
                    }
                }
            }
            catch (SQLException e) {
                // The coordinator may be reachable before the workers are registered.
                lastFailure = e;
            }

            try {
                Thread.sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for TLS Trino cluster", e);
            }
        }
        throw new IllegalStateException("TLS Trino cluster did not become ready with three active nodes", lastFailure);
    }

    private static NodeEndpoint nodeEndpoint(String nodeId, GenericContainer<?> container)
    {
        return new NodeEndpoint(nodeId, container.getHost(), container.getMappedPort(HTTP_PORT), container.getMappedPort(HTTPS_PORT));
    }

    private static Path locateKeystore()
    {
        Path current = Path.of("").toAbsolutePath();
        while (current != null) {
            Path candidate = current.resolve(KEYSTORE_RESOURCE_PATH);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("Unable to locate TLS keystore at " + KEYSTORE_RESOURCE_PATH);
    }

    public record NodeEndpoint(String nodeId, String host, int httpPort, int httpsPort) {}
}
