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
package io.trino.tests.product.deltalake;

import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.SparkDeltaContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * Delta Lake OSS product test environment for interoperability testing.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with HDFS and Hive Metastore</li>
 *   <li>Spark container with Delta Lake support and Thrift Server</li>
 *   <li>Trino container with Delta Lake connector</li>
 * </ul>
 * <p>
 * Catalog configuration:
 * <ul>
 *   <li>Trino uses "delta_lake" catalog for Delta Lake tables</li>
 *   <li>Trino uses "hive" catalog for Hive interoperability</li>
 *   <li>Spark uses "spark_catalog" with DeltaCatalog</li>
 *   <li>All share the same Hive Metastore, enabling cross-engine table access</li>
 * </ul>
 * <p>
 * This environment supports Delta Lake interoperability tests between Spark and Trino.
 */
public class DeltaLakeOssEnvironment
        extends ProductTestEnvironment
{
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
    private SparkDeltaContainer spark;
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

        // Start Spark with Delta Lake (depends on Hadoop for HMS)
        spark = new SparkDeltaContainer()
                .withNetwork(network)
                .withNetworkAliases(SparkDeltaContainer.HOST_NAME)
                .build();
        spark.dependsOn(hadoop);
        spark.start();

        // Start Trino with Delta Lake and Hive connectors
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                .withCatalog("delta_lake", Map.of(
                        "connector.name", "delta_lake",
                        "hive.metastore.uri", "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT,
                        "fs.hadoop.enabled", "true",
                        "delta.register-table-procedure.enabled", "true",
                        "hive.config.resources", "/etc/trino/hdfs-site.xml"))
                .withCatalog("hive", Map.of(
                        "connector.name", "hive",
                        "hive.metastore.uri", "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT,
                        "fs.hadoop.enabled", "true",
                        "hive.config.resources", "/etc/trino/hdfs-site.xml"))
                .build();
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
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

    /**
     * Returns the warehouse directory path in HDFS.
     */
    public String getWarehouseDirectory()
    {
        return hadoop.getWarehouseDirectory();
    }

    static void main(String[] args)
    {
        try (DeltaLakeOssEnvironment environment = new DeltaLakeOssEnvironment()) {
            environment.start();
            System.out.println("DeltaLakeOssEnvironment started. Press Ctrl+C to stop.");
            Thread.sleep(Long.MAX_VALUE);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
