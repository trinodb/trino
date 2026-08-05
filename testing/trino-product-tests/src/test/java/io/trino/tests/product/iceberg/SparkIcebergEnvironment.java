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
package io.trino.tests.product.iceberg;

import io.trino.testing.containers.Floci;
import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.SparkIcebergContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static io.trino.tests.product.iceberg.IcebergCatalogPropertiesBuilder.icebergCatalog;
import static java.util.Objects.requireNonNull;

/**
 * Spark/Iceberg product test environment for interoperability testing.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with HDFS and Hive Metastore</li>
 *   <li>Spark container with Iceberg support and Thrift Server</li>
 *   <li>Trino container with Iceberg connector</li>
 * </ul>
 * <p>
 * Catalog configuration:
 * <ul>
 *   <li>Trino uses "iceberg" catalog</li>
 *   <li>Spark uses "iceberg_test" catalog</li>
 *   <li>Both share the same Hive Metastore, enabling cross-engine table access</li>
 * </ul>
 * <p>
 * This environment supports approximately 100 Spark/Iceberg compatibility tests.
 */
public class SparkIcebergEnvironment
        extends ProductTestEnvironment
{
    private static final String FLOCI_HOST_NAME = "floci";
    private static final String FLOCI_ENDPOINT = "http://" + FLOCI_HOST_NAME + ":" + Floci.FLOCI_PORT;
    // Must be kept in sync with dep.iceberg.version in the root pom.xml.
    private static final String ICEBERG_VERSION = "1.11.0";

    static {
        // Ensure the Hive JDBC driver is loaded for Spark Thrift Server connections
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
    private Floci floci;
    private SparkIcebergContainer spark;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        // Start Hadoop first (provides HDFS and HMS)
        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        hadoop.start();

        floci = new Floci()
                .withNetwork(network)
                .withNetworkAliases(FLOCI_HOST_NAME);
        floci.start();

        // Start Spark (depends on Hadoop for HMS)
        spark = new SparkIcebergContainer()
                .withNetwork(network)
                .withNetworkAliases(SparkIcebergContainer.HOST_NAME)
                .withIcebergSparkRuntime(findIcebergSparkRuntimeJar(), ICEBERG_VERSION)
                .withAwsKmsEncryption(FLOCI_ENDPOINT);
        spark.dependsOn(hadoop, floci);
        spark.start();

        // Start Trino with Iceberg and Hive connectors
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                .withCatalog("iceberg", icebergCatalog("thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT)
                        .withHadoopFileSystem()
                        .put("iceberg.register-table-procedure.enabled", "true")
                        .put("iceberg.allowed-extra-properties", "custom.table-property")
                        .put("iceberg.encryption.kms-type", "AWS")
                        .put("aws.kms.region", Floci.FLOCI_REGION)
                        .put("aws.kms.endpoint", FLOCI_ENDPOINT)
                        .put("aws.kms.access-key", Floci.FLOCI_ACCESS_KEY)
                        .put("aws.kms.secret-key", Floci.FLOCI_SECRET_KEY)
                        .build())
                .withCatalog("hive", Map.of(
                        "connector.name", "hive",
                        "hive.metastore.uri", "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT,
                        "fs.hadoop.enabled", "true",
                        "hive.config.resources", "/etc/trino/hdfs-site.xml"))
                .build();
        TrinoProductTestContainer.startAndWait(trino);
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
            if (stmt.execute(sql)) {
                try (ResultSet ignored = stmt.getResultSet()) {
                    // intentionally ignored
                }
                return -1;
            }
            return stmt.getUpdateCount();
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
        String jdbcUrl = "jdbc:hive2://" + hadoop.getHost() + ":" + hadoop.getMappedPort(HadoopContainer.HIVESERVER2_PORT);
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

    // Standard ProductTestEnvironment methods

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino);
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, user);
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trino.getJdbcUrl();
    }

    @Override
    public boolean isRunning()
    {
        return trino != null && trino.isRunning();
    }

    protected final Network getNetwork()
    {
        return requireNonNull(network, "Environment is not started: network is null");
    }

    protected final HadoopContainer getHadoopContainer()
    {
        return requireNonNull(hadoop, "Environment is not started: hadoop is null");
    }

    @Override
    protected void doClose()
    {
        if (trino != null) {
            trino.close();
            trino = null;
        }
        if (spark != null) {
            spark.close();
            spark = null;
        }
        if (floci != null) {
            floci.close();
            floci = null;
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

    public URI getFlociEndpoint()
    {
        return floci.endpoint();
    }

    /**
     * Returns the warehouse directory path in HDFS.
     */
    public String getWarehouseDirectory()
    {
        return hadoop.getWarehouseDirectory();
    }

    // Iceberg utility methods

    /**
     * Gets the HDFS location of an Iceberg table by querying its $path metadata.
     *
     * @param tableName fully qualified table name (e.g., "iceberg.schema.table")
     * @return the table's HDFS location
     */
    public String getTableLocation(String tableName)
    {
        return (String) executeTrino(
                "SELECT DISTINCT regexp_replace(\"$path\", '/[^/]*/[^/]*$', '') FROM " + tableName)
                .getOnlyValue();
    }

    /**
     * Gets the filename of the latest metadata file for an Iceberg table.
     *
     * @param catalog the catalog name
     * @param schema the schema name
     * @param tableName the table name
     * @return the latest metadata filename
     */
    public String getLatestMetadataFilename(String catalog, String schema, String tableName)
    {
        return (String) executeTrino(
                ("SELECT substring(file, strpos(file, '/', -1) + 1) " +
                        "FROM %s.%s.\"%s$metadata_log_entries\" " +
                        "ORDER BY timestamp DESC LIMIT 1").formatted(catalog, schema, tableName))
                .getOnlyValue();
    }

    private static Path findIcebergSparkRuntimeJar()
    {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            Path jar = current.resolve("testing/trino-product-tests/target/iceberg-spark-runtime.jar");
            if (Files.isRegularFile(jar)) {
                return jar;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("iceberg-spark-runtime.jar was not generated");
    }

    /**
     * Strips the namenode URI from an HDFS path, returning just the path component.
     *
     * @param location the full HDFS URI (e.g., "hdfs://namenode:9000/path/to/table")
     * @return just the path (e.g., "/path/to/table")
     */
    public static String stripNamenodeURI(String location)
    {
        return URI.create(location).getPath();
    }
}
