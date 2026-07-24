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
package io.trino.tests.product.compatibility;

import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.trino.TrinoContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;

import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hadoopMetastoreUri;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;

/**
 * Runs current and compatibility Trino servers against one shared Hive Metastore and HDFS deployment.
 */
public class CompatibilityEnvironment
        extends ProductTestEnvironment
{
    public static final String COMPATIBILITY_IMAGE_PROPERTY = "compatibility.testDockerImage";
    public static final String COMPATIBILITY_VERSION_PROPERTY = "compatibility.testVersion";

    private static final int TRINO_HTTP_PORT = 8080;
    private static final int LEGACY_HIVE_CONNECTOR_RENAME_VERSION = 359;

    private Network network;
    private HadoopContainer hadoop;
    private TrinoContainer trino;
    private GenericContainer<?> compatibilityTrino;
    private int compatibilityVersion;

    static {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Hive JDBC driver", e);
        }
    }

    @Override
    public void start()
    {
        if (isRunning()) {
            return;
        }

        String image = requireNonNull(System.getProperty(COMPATIBILITY_IMAGE_PROPERTY), COMPATIBILITY_IMAGE_PROPERTY + " is not set");
        compatibilityVersion = parseInt(requireNonNull(System.getProperty(COMPATIBILITY_VERSION_PROPERTY), COMPATIBILITY_VERSION_PROPERTY + " is not set"));

        network = Network.newNetwork();
        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        hadoop.start();

        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                .withCatalog("hive", Map.of(
                        "connector.name", "hive",
                        "hive.metastore.uri", hadoopMetastoreUri(),
                        "fs.hadoop.enabled", "true",
                        "hive.config.resources", "/etc/trino/hdfs-site.xml",
                        "hive.hive-views.enabled", "true"))
                .withCatalog("iceberg", Map.of(
                        "connector.name", "iceberg",
                        "hive.metastore.uri", hadoopMetastoreUri(),
                        "fs.hadoop.enabled", "true",
                        "hive.config.resources", "/etc/trino/hdfs-site.xml"))
                .build();
        TrinoProductTestContainer.startAndWait(trino);

        compatibilityTrino = createCompatibilityContainer(image, compatibilityVersion);
        compatibilityTrino.start();
        waitForCompatibilityServerReady();
    }

    public QueryResult executeCompatibilityTrino(String sql)
    {
        try (Connection connection = createCompatibilityTrinoConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            return QueryResult.forResultSet(resultSet);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute compatibility Trino query: " + sql, e);
        }
    }

    public int executeCompatibilityTrinoUpdate(String sql)
    {
        try (Connection connection = createCompatibilityTrinoConnection();
                Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute compatibility Trino update: " + sql, e);
        }
    }

    public int executeHiveUpdate(String sql)
    {
        String jdbcUrl = format(
                "jdbc:hive2://%s:%s/default",
                hadoop.getHost(),
                hadoop.getMappedPort(HadoopContainer.HIVESERVER2_PORT));
        try (Connection connection = DriverManager.getConnection(jdbcUrl, "hive", "");
                Statement statement = connection.createStatement()) {
            return statement.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Hive update: " + sql, e);
        }
    }

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, "hive", "hive", "default");
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user, "hive", "default");
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino.getJdbcUrl();
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning() && compatibilityTrino != null && compatibilityTrino.isRunning();
    }

    @Override
    protected void doClose()
    {
        if (compatibilityTrino != null) {
            compatibilityTrino.close();
            compatibilityTrino = null;
        }
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (hadoop != null) {
            hadoop.close();
            hadoop = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    private Connection createCompatibilityTrinoConnection()
            throws SQLException
    {
        String protocol = compatibilityVersion <= 350 ? "presto" : "trino";
        return DriverManager.getConnection(
                format("jdbc:%s://%s:%s/hive/default", protocol, compatibilityTrino.getHost(), compatibilityTrino.getMappedPort(TRINO_HTTP_PORT)),
                "hive",
                null);
    }

    private GenericContainer<?> createCompatibilityContainer(String image, int version)
    {
        String configurationDirectory = getConfigurationDirectory(version);
        return new GenericContainer<>(DockerImageName.parse(image))
                .withNetwork(network)
                .withNetworkAliases("compatibility-test-server", "compatibility-test-coordinator")
                .withExposedPorts(TRINO_HTTP_PORT)
                .withCopyToContainer(
                        Transferable.of(createHiveCatalogProperties(version), 0644),
                        configurationDirectory + "catalog/hive.properties")
                .withCopyToContainer(
                        Transferable.of(createIcebergCatalogProperties(version), 0644),
                        configurationDirectory + "catalog/iceberg.properties")
                .withCopyToContainer(
                        Transferable.of(hadoop.getHdfsClientSiteXml(), 0644),
                        "/etc/trino/hdfs-site.xml")
                .waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(5)));
    }

    private static String createHiveCatalogProperties(int version)
    {
        String connectorName = version < LEGACY_HIVE_CONNECTOR_RENAME_VERSION ? "hive-hadoop2" : "hive";
        StringBuilder properties = new StringBuilder()
                .append("connector.name=").append(connectorName).append('\n')
                .append("hive.metastore.uri=thrift://").append(HadoopContainer.HOST_NAME).append(':').append(HadoopContainer.HIVE_METASTORE_PORT).append('\n')
                .append("hive.parquet.time-zone=UTC\n")
                .append("hive.rcfile.time-zone=UTC\n");
        if (version >= LEGACY_HIVE_CONNECTOR_RENAME_VERSION) {
            properties.append("fs.hadoop.enabled=true\n");
            properties.append("hive.config.resources=/etc/trino/hdfs-site.xml\n");
            properties.append("hive.hive-views.enabled=true\n");
        }
        return properties.toString();
    }

    private static String createIcebergCatalogProperties(int version)
    {
        StringBuilder properties = new StringBuilder()
                .append("connector.name=iceberg\n")
                .append("hive.metastore.uri=thrift://").append(HadoopContainer.HOST_NAME).append(':').append(HadoopContainer.HIVE_METASTORE_PORT).append('\n');
        if (version >= LEGACY_HIVE_CONNECTOR_RENAME_VERSION) {
            properties.append("fs.hadoop.enabled=true\n");
            properties.append("hive.config.resources=/etc/trino/hdfs-site.xml\n");
        }
        return properties.toString();
    }

    private static String getConfigurationDirectory(int version)
    {
        if (version <= 350) {
            return "/usr/lib/presto/default/etc/";
        }
        if (version == 351) {
            return "/usr/lib/trino/default/etc/";
        }
        return "/etc/trino/";
    }

    private void waitForCompatibilityServerReady()
    {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection connection = createCompatibilityTrinoConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT 1")) {
                if (resultSet.next()) {
                    return;
                }
            }
            catch (SQLException ignored) {
                // Retry until timeout.
            }

            try {
                sleep(500);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for compatibility server readiness", e);
            }
        }
        throw new IllegalStateException("Compatibility server was not ready for queries within timeout");
    }
}
