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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * Builder for creating Trino containers with custom catalog configurations.
 * <p>
 * Example usage:
 * <pre>
 * {@code
 * @Container
 * static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0");
 *
 * @Container
 * static TrinoContainer trino = TrinoProductTestContainer.builder()
 *         .withCatalog("mysql", Map.of(
 *                 "connector.name", "mysql",
 *                 "connection-url", mysql.getJdbcUrl(),
 *                 "connection-user", mysql.getUsername(),
 *                 "connection-password", mysql.getPassword()))
 *         .build();
 * }
 * </pre>
 */
public final class TrinoProductTestContainer
{
    private static final String DEFAULT_IMAGE = "trinodb/trino";

    private TrinoProductTestContainer() {}

    /**
     * Creates a JDBC connection to the given Trino container using the default user "hive".
     */
    public static Connection createConnection(TrinoContainer container)
            throws SQLException
    {
        return createConnection(container, "hive");
    }

    /**
     * Creates a JDBC connection to the given Trino container with the specified user.
     */
    public static Connection createConnection(TrinoContainer container, String user)
            throws SQLException
    {
        return DriverManager.getConnection(container.getJdbcUrl(), user, null);
    }

    /**
     * Creates a JDBC connection to the given Trino container with the specified user and default catalog/schema.
     * <p>
     * This restores legacy product-test behavior that used jdbc:trino://.../hive/default in Tempto config.
     */
    public static Connection createConnection(TrinoContainer container, String user, String catalog, String schema)
            throws SQLException
    {
        Connection connection = createConnection(container, user);
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
     * Creates a JDBC connection to the given Trino container with the specified properties.
     */
    public static Connection createConnection(TrinoContainer container, Properties properties)
            throws SQLException
    {
        return DriverManager.getConnection(container.getJdbcUrl(), properties);
    }

    /**
     * Waits for the Trino cluster to be fully ready for queries.
     * The standard TrinoContainer wait strategy checks basic connectivity, but there's
     * a race condition where the node may not be fully registered for distributed queries.
     * This method ensures at least one active node is available.
     */
    public static void waitForClusterReady(TrinoContainer container)
            throws SQLException, InterruptedException
    {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection conn = createConnection(container);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'")) {
                if (rs.next() && rs.getInt(1) > 0) {
                    // Verify we can actually run a distributed query
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
        throw new IllegalStateException("Trino cluster not ready within timeout");
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String imageName = TrinoTestImages.getDefaultTrinoImage();
        private final Map<String, Map<String, String>> catalogs = new HashMap<>();
        private Network network;
        private String networkAlias;
        private String hdfsSiteXml;

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
         * Adds a catalog configuration. The properties map should include
         * at minimum "connector.name".
         * <p>
         * Example:
         * <pre>
         * withCatalog("mysql", Map.of(
         *     "connector.name", "mysql",
         *     "connection-url", "jdbc:mysql://mysql:3306",
         *     "connection-user", "root",
         *     "connection-password", "secret"))
         * </pre>
         */
        public Builder withCatalog(String catalogName, Map<String, String> properties)
        {
            requireNonNull(catalogName, "catalogName is null");
            requireNonNull(properties, "properties is null");
            catalogs.put(catalogName, new HashMap<>(properties));
            return this;
        }

        /**
         * Sets the Docker network for this container.
         */
        public Builder withNetwork(Network network)
        {
            this.network = requireNonNull(network, "network is null");
            return this;
        }

        /**
         * Sets the network alias for this container.
         */
        public Builder withNetworkAlias(String networkAlias)
        {
            this.networkAlias = requireNonNull(networkAlias, "networkAlias is null");
            return this;
        }

        /**
         * Configures the container with HDFS client settings.
         * <p>
         * The XML should be obtained from {@link HadoopContainer#getHdfsClientSiteXml()}
         * to ensure consistent HDFS configuration across containers.
         * <p>
         * Example:
         * <pre>
         * .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
         * </pre>
         *
         * @param hdfsSiteXml HDFS configuration XML from HadoopContainer
         */
        public Builder withHdfsConfiguration(String hdfsSiteXml)
        {
            this.hdfsSiteXml = requireNonNull(hdfsSiteXml, "hdfsSiteXml is null");
            return this;
        }

        /**
         * Builds and returns a configured TrinoContainer.
         */
        public TrinoContainer build()
        {
            TrinoContainer container = new TrinoContainer(DockerImageName.parse(imageName));

            // Legacy product tests always ran Trino with Asia/Kathmandu timezone.
            // Keep that behavior for JUnit parity.
            String timezoneInitScript = """
                    #!/bin/bash
                    if ! grep -qx -- '-Duser.timezone=Asia/Kathmandu' /etc/trino/jvm.config; then
                        echo '-Duser.timezone=Asia/Kathmandu' >> /etc/trino/jvm.config
                    fi
                    """;
            container.withCopyToContainer(
                    Transferable.of(timezoneInitScript, 0755),
                    "/docker/trino-init.d/00-set-timezone.sh");

            // Add catalog configurations
            for (Map.Entry<String, Map<String, String>> catalog : catalogs.entrySet()) {
                String catalogName = catalog.getKey();
                Map<String, String> properties = catalog.getValue();
                String propertiesContent = formatProperties(properties);
                container.withCopyToContainer(
                        Transferable.of(propertiesContent),
                        "/etc/trino/catalog/" + catalogName + ".properties");
            }

            // Add HDFS configuration if provided
            if (hdfsSiteXml != null) {
                container.withCopyToContainer(
                        Transferable.of(hdfsSiteXml),
                        "/etc/trino/hdfs-site.xml");
            }

            // Apply network if specified
            if (network != null) {
                container.withNetwork(network);
                if (networkAlias != null) {
                    container.withNetworkAliases(networkAlias);
                }
            }

            // Add a more robust wait strategy that ensures nodes are available
            container.waitingFor(
                    Wait.forHttp("/v1/info")
                            .forPort(8080)
                            .forStatusCode(200)
                            .withStartupTimeout(Duration.ofSeconds(90)));

            return container;
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
