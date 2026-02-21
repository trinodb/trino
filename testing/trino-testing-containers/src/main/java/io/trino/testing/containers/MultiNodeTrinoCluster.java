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
package io.trino.testing.containers;

import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * Orchestrates a multinode Trino cluster with one coordinator and multiple workers.
 * <p>
 * Example usage:
 * <pre>{@code
 * MultiNodeTrinoCluster cluster = MultiNodeTrinoCluster.builder()
 *         .withNetwork(network)
 *         .withWorkerCount(2)
 *         .withCatalog("hive", Map.of(
 *                 "connector.name", "hive",
 *                 "hive.metastore.uri", "thrift://metastore:9083"))
 *         .build();
 * cluster.start();
 *
 * try (Connection conn = cluster.createConnection()) {
 *     // Run queries...
 * }
 * }</pre>
 * <p>
 * The cluster ensures:
 * <ul>
 *   <li>Coordinator starts first and is ready for discovery</li>
 *   <li>Workers connect to coordinator via discovery.uri</li>
 *   <li>All nodes have identical catalog configurations</li>
 *   <li>{@link #waitForClusterReady()} blocks until all nodes are registered</li>
 * </ul>
 *
 * @see TrinoWorkerContainer
 */
public class MultiNodeTrinoCluster
        implements AutoCloseable
{
    private static final String DEFAULT_IMAGE = "trinodb/trino";
    private static final String COORDINATOR_ALIAS = "trino-coordinator";

    private final TrinoContainer coordinator;
    private final List<TrinoWorkerContainer> workers;
    private final int expectedNodeCount;

    private MultiNodeTrinoCluster(TrinoContainer coordinator, List<TrinoWorkerContainer> workers)
    {
        this.coordinator = requireNonNull(coordinator, "coordinator is null");
        this.workers = List.copyOf(requireNonNull(workers, "workers is null"));
        this.expectedNodeCount = 1 + workers.size(); // coordinator + workers
    }

    /**
     * Starts the cluster: coordinator first, then workers.
     */
    public void start()
    {
        // Start coordinator first
        coordinator.start();

        // Start all workers
        for (TrinoWorkerContainer worker : workers) {
            worker.start();
        }
    }

    /**
     * Waits for all nodes to register with the cluster.
     * <p>
     * This method blocks until the expected number of active nodes are visible
     * in system.runtime.nodes, or throws an exception on timeout.
     */
    public void waitForClusterReady()
            throws SQLException, InterruptedException
    {
        waitForClusterReady(Duration.ofSeconds(60));
    }

    /**
     * Waits for all nodes to register with the cluster with a custom timeout.
     */
    public void waitForClusterReady(Duration timeout)
            throws SQLException, InterruptedException
    {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection conn = createConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'")) {
                if (rs.next() && rs.getInt(1) >= expectedNodeCount) {
                    // Verify we can run a distributed query
                    try (ResultSet verify = stmt.executeQuery("SELECT count(*) FROM tpch.tiny.nation")) {
                        if (verify.next()) {
                            return; // Cluster is ready
                        }
                    }
                }
            }
            catch (SQLException e) {
                // Ignore and retry
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException(
                "Trino cluster not ready within timeout. Expected %d nodes.".formatted(expectedNodeCount));
    }

    /**
     * Creates a JDBC connection to the coordinator using the default user "test".
     */
    public Connection createConnection()
            throws SQLException
    {
        return DriverManager.getConnection(coordinator.getJdbcUrl(), "test", null);
    }

    /**
     * Creates a JDBC connection to the coordinator with the specified user.
     */
    public Connection createConnection(String user)
            throws SQLException
    {
        return DriverManager.getConnection(coordinator.getJdbcUrl(), user, null);
    }

    /**
     * Creates a JDBC connection to the coordinator with the specified user and default catalog/schema.
     * <p>
     * This restores legacy product-test behavior that used jdbc:trino://.../hive/default in Tempto config.
     */
    public Connection createConnection(String user, String catalog, String schema)
            throws SQLException
    {
        Connection connection = createConnection(user);
        try {
            if (catalog != null) {
                connection.setCatalog(catalog);
            }
            if (schema != null) {
                connection.setSchema(schema);
            }
            return connection;
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
    }

    /**
     * Creates a JDBC connection to the coordinator with the specified properties.
     */
    public Connection createConnection(Properties properties)
            throws SQLException
    {
        return DriverManager.getConnection(coordinator.getJdbcUrl(), properties);
    }

    /**
     * Returns the JDBC URL for the coordinator.
     */
    public String getJdbcUrl()
    {
        return coordinator.getJdbcUrl();
    }

    /**
     * Returns the coordinator container.
     */
    public TrinoContainer getCoordinator()
    {
        return coordinator;
    }

    /**
     * Returns an unmodifiable list of worker containers.
     */
    public List<TrinoWorkerContainer> getWorkers()
    {
        return workers;
    }

    /**
     * Returns the expected number of nodes in the cluster (coordinator + workers).
     */
    public int getExpectedNodeCount()
    {
        return expectedNodeCount;
    }

    @Override
    public void close()
    {
        // Close workers first
        for (TrinoWorkerContainer worker : workers) {
            try {
                worker.close();
            }
            catch (Exception e) {
                // Continue closing other containers
            }
        }
        // Close coordinator
        try {
            coordinator.close();
        }
        catch (Exception e) {
            // Ignore
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String imageName = DEFAULT_IMAGE;
        private final Map<String, Map<String, String>> catalogs = new HashMap<>();
        private final Map<String, String> configProperties = new HashMap<>();
        private Map<String, String> exchangeManagerProperties;
        private Network network;
        private int workerCount = 1;
        private String hdfsSiteXml;
        private Consumer<TrinoContainer> coordinatorCustomizer;
        private Consumer<TrinoWorkerContainer> workerCustomizer;

        private Builder() {}

        /**
         * Sets the Docker image to use for Trino.
         */
        public Builder withImage(String imageName)
        {
            this.imageName = requireNonNull(imageName, "imageName is null");
            return this;
        }

        /**
         * Sets the Trino version to use (uses trinodb/trino:{version}).
         */
        public Builder withVersion(String version)
        {
            this.imageName = DEFAULT_IMAGE + ":" + requireNonNull(version, "version is null");
            return this;
        }

        /**
         * Adds a catalog configuration that will be applied to all nodes.
         */
        public Builder withCatalog(String catalogName, Map<String, String> properties)
        {
            requireNonNull(catalogName, "catalogName is null");
            requireNonNull(properties, "properties is null");
            catalogs.put(catalogName, new HashMap<>(properties));
            return this;
        }

        /**
         * Sets the Docker network for the cluster.
         */
        public Builder withNetwork(Network network)
        {
            this.network = requireNonNull(network, "network is null");
            return this;
        }

        /**
         * Sets the number of worker nodes (default: 1).
         */
        public Builder withWorkerCount(int workerCount)
        {
            if (workerCount < 0) {
                throw new IllegalArgumentException("workerCount must be non-negative");
            }
            this.workerCount = workerCount;
            return this;
        }

        /**
         * Configures HDFS client settings for all nodes.
         */
        public Builder withHdfsConfiguration(String hdfsSiteXml)
        {
            this.hdfsSiteXml = requireNonNull(hdfsSiteXml, "hdfsSiteXml is null");
            return this;
        }

        /**
         * Adds a config property that will be applied to all nodes.
         * These properties are added to config.properties on both coordinator and workers.
         *
         * @param key the property key
         * @param value the property value
         */
        public Builder withConfigProperty(String key, String value)
        {
            requireNonNull(key, "key is null");
            requireNonNull(value, "value is null");
            configProperties.put(key, value);
            return this;
        }

        /**
         * Adds multiple config properties that will be applied to all nodes.
         *
         * @param properties the properties to add
         */
        public Builder withConfigProperties(Map<String, String> properties)
        {
            requireNonNull(properties, "properties is null");
            configProperties.putAll(properties);
            return this;
        }

        /**
         * Configures the exchange manager for fault-tolerant execution.
         * The exchange manager configuration is applied to all nodes in the cluster.
         *
         * @param properties the exchange manager properties
         */
        public Builder withExchangeManager(Map<String, String> properties)
        {
            requireNonNull(properties, "properties is null");
            this.exchangeManagerProperties = new HashMap<>(properties);
            return this;
        }

        /**
         * Adds a customizer that will be applied to the coordinator container before start.
         * <p>
         * Use this for coordinator-specific configuration like tmpfs mounts.
         */
        public Builder withCoordinatorCustomizer(Consumer<TrinoContainer> customizer)
        {
            this.coordinatorCustomizer = requireNonNull(customizer, "customizer is null");
            return this;
        }

        /**
         * Adds a customizer that will be applied to each worker container before start.
         * <p>
         * Use this for worker-specific configuration like tmpfs mounts.
         */
        public Builder withWorkerCustomizer(Consumer<TrinoWorkerContainer> customizer)
        {
            this.workerCustomizer = requireNonNull(customizer, "customizer is null");
            return this;
        }

        /**
         * Builds and returns a configured MultiNodeTrinoCluster.
         * <p>
         * Note: The cluster is not started automatically. Call {@link #start()} after building.
         */
        public MultiNodeTrinoCluster build()
        {
            if (network == null) {
                throw new IllegalStateException("network must be set for multinode cluster");
            }

            // Build coordinator
            TrinoContainer coordinator = new TrinoContainer(DockerImageName.parse(imageName));
            coordinator.withNetwork(network);
            coordinator.withNetworkAliases(COORDINATOR_ALIAS);

            // Add catalog configurations to coordinator
            for (Map.Entry<String, Map<String, String>> catalog : catalogs.entrySet()) {
                String catalogName = catalog.getKey();
                Map<String, String> properties = catalog.getValue();
                String propertiesContent = formatProperties(properties);
                coordinator.withCopyToContainer(
                        Transferable.of(propertiesContent),
                        "/etc/trino/catalog/" + catalogName + ".properties");
            }

            // Add HDFS configuration if provided
            if (hdfsSiteXml != null) {
                coordinator.withCopyToContainer(
                        Transferable.of(hdfsSiteXml),
                        "/etc/trino/hdfs-site.xml");
            }

            // Add config properties to coordinator
            if (!configProperties.isEmpty()) {
                coordinator.withCopyToContainer(
                        Transferable.of(formatProperties(configProperties)),
                        "/etc/trino/config.properties");
            }

            // Add exchange manager configuration to coordinator
            if (exchangeManagerProperties != null) {
                coordinator.withCopyToContainer(
                        Transferable.of(formatProperties(exchangeManagerProperties)),
                        "/etc/trino/exchange-manager.properties");
            }

            // Apply coordinator customizer
            if (coordinatorCustomizer != null) {
                coordinatorCustomizer.accept(coordinator);
            }

            // Add a robust wait strategy
            coordinator.waitingFor(
                    Wait.forHttp("/v1/info")
                            .forPort(8080)
                            .forStatusCode(200)
                            .withStartupTimeout(Duration.ofSeconds(90)));

            // Build workers
            List<TrinoWorkerContainer> workers = new ArrayList<>();
            String discoveryUri = "http://" + COORDINATOR_ALIAS + ":8080";

            for (int i = 0; i < workerCount; i++) {
                TrinoWorkerContainer worker = new TrinoWorkerContainer(imageName);
                worker.withNetwork(network);
                worker.withNetworkAliases("trino-worker-" + i);
                worker.withDiscoveryUri(discoveryUri);

                // Add catalog configurations to worker
                for (Map.Entry<String, Map<String, String>> catalog : catalogs.entrySet()) {
                    worker.withCatalog(catalog.getKey(), catalog.getValue());
                }

                // Add HDFS configuration if provided
                if (hdfsSiteXml != null) {
                    worker.withHdfsConfiguration(hdfsSiteXml);
                }

                // Add config properties to worker
                if (!configProperties.isEmpty()) {
                    worker.withConfigProperties(configProperties);
                }

                // Add exchange manager configuration to worker
                if (exchangeManagerProperties != null) {
                    worker.withExchangeManager(exchangeManagerProperties);
                }

                // Apply worker customizer
                if (workerCustomizer != null) {
                    workerCustomizer.accept(worker);
                }

                workers.add(worker);
            }

            return new MultiNodeTrinoCluster(coordinator, workers);
        }

        private static String formatProperties(Map<String, String> properties)
        {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                sb.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
            }
            return sb.toString();
        }
    }
}
