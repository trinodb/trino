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

import io.trino.testing.TestingProperties;
import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.SparkIcebergContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hiveCatalog;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.containers.wait.strategy.Wait.forLogMessage;

/**
 * Hive/Spark product test environment for interoperability testing.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with HDFS, Hive Metastore, and HiveServer2</li>
 *   <li>Spark container with Thrift Server for Spark SQL access</li>
 *   <li>Trino container with Hive connector</li>
 * </ul>
 * <p>
 * Catalog configuration:
 * <ul>
 *   <li>Trino uses "hive" catalog</li>
 *   <li>Spark and Hive both share the same Hive Metastore</li>
 *   <li>All three engines share the same metastore, enabling cross-engine table access</li>
 * </ul>
 * <p>
 * This environment supports Hive/Spark interoperability testing, allowing tests
 * to create tables in one engine and read/write them from another.
 * <p>
 * Note: Uses SparkIcebergContainer (not SparkDeltaContainer) because it has
 * full Hive support for creating and accessing native Hive tables.
 */
public class HiveSparkEnvironment
        extends ProductTestEnvironment
{
    private static final int TRINO_HTTP_PORT = 8080;
    private static final int LEGACY_HIVE_CONNECTOR_RENAME_VERSION = 359;
    private static final int FIRST_TRINO_VERSION = 351;
    private static final String COMPATIBILITY_IMAGE_PROPERTY = "compatibility.testDockerImage";
    private static final String COMPATIBILITY_VERSION_PROPERTY = "compatibility.testVersion";
    private static final Pattern PROJECT_VERSION_PATTERN = Pattern.compile("(\\d+)(?:-SNAPSHOT)?");
    private static final Pattern NUMERIC_PREFIX_PATTERN = Pattern.compile("(\\d+).*");

    static {
        // Ensure the Hive JDBC driver is loaded for Spark Thrift Server and HiveServer2 connections
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Hive JDBC driver. " +
                    "Ensure hive-apache-jdbc dependency is on the classpath.", e);
        }
    }

    private Network network;
    private HadoopContainer hadoop;
    private SparkIcebergContainer spark;
    private TrinoContainer trino;
    private TrinoContainer compatibilityTrino;
    private int compatibilityVersion;

    @Override
    public void start()
    {
        if (isRunning()) {
            return; // Already started
        }

        if (trino == null || !trino.isRunning()) {
            network = Network.newNetwork();

            // Start Hadoop first (provides HDFS, HMS, and HiveServer2)
            hadoop = new HadoopContainer()
                    .withNetwork(network)
                    .withNetworkAliases(HadoopContainer.HOST_NAME);
            hadoop.start();

            // Start Spark (depends on Hadoop for HMS)
            // Note: Using SparkIcebergContainer because it has Hive support
            spark = new SparkIcebergContainer()
                    .withNetwork(network)
                    .withNetworkAliases(SparkIcebergContainer.HOST_NAME);
            spark.dependsOn(hadoop);
            spark.start();

            // Start Trino with Hive connector
            trino = TrinoProductTestContainer.builder()
                    .withNetwork(network)
                    .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                    .withCatalog("hive", hiveCatalog(HiveCatalogPropertiesBuilder.hadoopMetastoreUri())
                            .withHadoopFileSystem()
                            .withCommonProperties()
                            .withPartitionProcedures()
                            .build())
                    .withCatalog("tpch", Map.of("connector.name", "tpch"))
                    .build();
            trino.start();

            try {
                TrinoProductTestContainer.waitForClusterReady(trino);
            }
            catch (SQLException | InterruptedException e) {
                throw new RuntimeException("Failed to wait for Trino cluster", e);
            }
        }

        if (compatibilityTrino == null || !compatibilityTrino.isRunning()) {
            String image = System.getProperty(COMPATIBILITY_IMAGE_PROPERTY, defaultCompatibilityImage());
            compatibilityVersion = detectCompatibilityVersion(image);
            if (compatibilityVersion < FIRST_TRINO_VERSION) {
                throw new IllegalArgumentException(format(
                        "Hive view compatibility requires Trino image version >= %s, got %s (%s)",
                        FIRST_TRINO_VERSION,
                        compatibilityVersion,
                        image));
            }

            compatibilityTrino = createCompatibilityContainer(image, compatibilityVersion);
            compatibilityTrino.start();
            waitForCompatibilityServerReady();
        }
    }

    public Connection createCompatibilityTrinoConnection()
            throws SQLException
    {
        return createCompatibilityTrinoConnection("hive");
    }

    public Connection createCompatibilityTrinoConnection(String user)
            throws SQLException
    {
        return DriverManager.getConnection(getCompatibilityTrinoJdbcUrl(), requireNonNull(user, "user is null"), null);
    }

    public QueryResult executeCompatibilityTrino(String sql)
    {
        try (Connection conn = createCompatibilityTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return QueryResult.forResultSet(rs);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute compatibility Trino query: " + sql, e);
        }
    }

    public int executeCompatibilityTrinoUpdate(String sql)
    {
        try (Connection conn = createCompatibilityTrinoConnection();
                Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute compatibility Trino update: " + sql, e);
        }
    }

    public String getCompatibilityTrinoJdbcUrl()
    {
        if (compatibilityTrino == null) {
            return null;
        }
        return format(
                "jdbc:trino://%s:%s/hive/default",
                compatibilityTrino.getHost(),
                compatibilityTrino.getMappedPort(TRINO_HTTP_PORT));
    }

    // Spark JDBC methods

    /**
     * Creates a JDBC connection to the Spark Thrift Server.
     */
    public Connection createSparkConnection()
            throws SQLException
    {
        return DriverManager.getConnection(spark.getJdbcUrl(), "hive", "");
    }

    /**
     * Executes a SQL query against Spark and returns the result.
     *
     * @param sql the SQL query to execute
     * @return the query result
     */
    public QueryResult executeSpark(String sql)
    {
        try (Connection conn = createSparkConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return QueryResult.forResultSet(rs);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Spark query: " + sql, e);
        }
    }

    /**
     * Executes a DDL or DML statement against Spark.
     *
     * @param sql the SQL statement to execute
     * @return the number of affected rows, or 0 for DDL statements
     */
    public int executeSparkUpdate(String sql)
    {
        try (Connection conn = createSparkConnection();
                Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Spark update: " + sql, e);
        }
    }

    // Hive JDBC methods (connects to HiveServer2 in Hadoop container)

    /**
     * Creates a JDBC connection to HiveServer2 in the Hadoop container.
     */
    public Connection createHiveConnection()
            throws SQLException
    {
        String jdbcUrl = "jdbc:hive2://" + hadoop.getHost() + ":" + hadoop.getMappedPort(HadoopContainer.HIVESERVER2_PORT) + "/default";
        return DriverManager.getConnection(jdbcUrl, "hive", "");
    }

    /**
     * Executes a SQL query against Hive and returns the result.
     *
     * @param sql the SQL query to execute
     * @return the query result
     */
    public QueryResult executeHive(String sql)
    {
        try (Connection conn = createHiveConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return QueryResult.forResultSet(rs);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Hive query: " + sql, e);
        }
    }

    /**
     * Executes a DDL or DML statement against Hive.
     *
     * @param sql the SQL statement to execute
     * @return the number of affected rows, or 0 for DDL statements
     */
    public int executeHiveUpdate(String sql)
    {
        try (Connection conn = createHiveConnection();
                Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Hive update: " + sql, e);
        }
    }

    // HDFS access

    /**
     * Creates an HdfsClient for interacting with HDFS.
     * <p>
     * The client uses custom DNS resolution to handle redirects to DataNodes
     * using Docker-internal hostnames, enabling both read and write operations.
     */
    public HdfsClient createHdfsClient()
    {
        return hadoop.createHdfsClient();
    }

    /**
     * Returns the default warehouse directory path in HDFS.
     */
    public String getWarehouseDirectory()
    {
        return hadoop.getWarehouseDirectory();
    }

    // Standard ProductTestEnvironment methods

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
        if (spark != null) {
            spark.close();
            spark = null;
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

    private TrinoContainer createCompatibilityContainer(String image, int version)
    {
        return new TrinoContainer(DockerImageName.parse(image))
                .withNetwork(network)
                .withNetworkAliases("compatibility-test-server", "compatibility-test-coordinator")
                .withExposedPorts(TRINO_HTTP_PORT)
                .withCopyToContainer(
                        Transferable.of(createCompatibilityHiveCatalogProperties(version), 0644),
                        getConfigurationDirectory(version) + "catalog/hive.properties")
                .withCopyToContainer(
                        Transferable.of(hadoop.getHdfsClientSiteXml(), 0644),
                        "/etc/trino/hdfs-site.xml")
                .waitingFor(forLogMessage(".*SERVER STARTED.*", 1).withStartupTimeout(Duration.ofMinutes(5)));
    }

    private static String createCompatibilityHiveCatalogProperties(int version)
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

    private static String getConfigurationDirectory(int version)
    {
        if (version == 351) {
            return "/usr/lib/trino/default/etc/";
        }
        return "/etc/trino/";
    }

    private static int detectCompatibilityVersion(String image)
    {
        String configuredVersion = System.getProperty(COMPATIBILITY_VERSION_PROPERTY);
        if (configuredVersion != null && !configuredVersion.isBlank()) {
            return parseInt(configuredVersion);
        }

        Matcher matcher = NUMERIC_PREFIX_PATTERN.matcher(DockerImageName.parse(image).getVersionPart());
        if (matcher.matches()) {
            return parseInt(matcher.group(1));
        }

        throw new IllegalArgumentException(format(
                "Cannot detect compatibility version from image '%s'; set -D%s=<version>",
                image,
                COMPATIBILITY_VERSION_PROPERTY));
    }

    private static String defaultCompatibilityImage()
    {
        Matcher matcher = PROJECT_VERSION_PATTERN.matcher(TestingProperties.getProjectVersion());
        if (!matcher.matches()) {
            throw new IllegalStateException("Unexpected project version: " + TestingProperties.getProjectVersion());
        }
        return "trinodb/trino:" + (parseInt(matcher.group(1)) - 1);
    }

    private void waitForCompatibilityServerReady()
    {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline) {
            try (Connection conn = createCompatibilityTrinoConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1")) {
                if (rs.next()) {
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
