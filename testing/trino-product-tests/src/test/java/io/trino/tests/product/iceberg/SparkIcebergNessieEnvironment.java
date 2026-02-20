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

import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.NessieContainer;
import io.trino.testing.containers.SparkIcebergNessieContainer;
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
 * Spark/Iceberg with Nessie catalog product test environment.
 * <p>
 * This environment extends the Spark/Iceberg pattern by adding a Nessie server
 * for version-controlled catalog operations. Nessie provides Git-like versioning
 * for data lakes, including branches, commits, and multi-table transactions.
 * <p>
 * Components:
 * <ul>
 *   <li>Hadoop container with HDFS for data storage</li>
 *   <li>Nessie container for version-controlled catalog metadata</li>
 *   <li>Spark container with Iceberg using NessieCatalog</li>
 *   <li>Trino container with Iceberg connector using Nessie catalog type</li>
 * </ul>
 * <p>
 * Catalog configuration:
 * <ul>
 *   <li>Trino uses "iceberg" catalog with iceberg.catalog.type=nessie</li>
 *   <li>Spark uses "iceberg_test" catalog with NessieCatalog implementation</li>
 *   <li>Both connect to the same Nessie server, enabling cross-engine table access</li>
 *   <li>HDFS is used for underlying data storage</li>
 * </ul>
 */
public class SparkIcebergNessieEnvironment
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
    private NessieContainer nessie;
    private SparkIcebergNessieContainer spark;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        // Start Hadoop first (provides HDFS for data storage)
        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        hadoop.start();

        // Start Nessie (provides catalog metadata storage)
        nessie = new NessieContainer()
                .withNetwork(network)
                .withNetworkAliases(NessieContainer.HOST_NAME);
        nessie.start();

        // Start Spark (depends on Hadoop for HDFS and Nessie for catalog)
        spark = new SparkIcebergNessieContainer()
                .withNetwork(network)
                .withNetworkAliases(SparkIcebergNessieContainer.HOST_NAME);
        spark.dependsOn(hadoop, nessie);
        spark.start();

        // Start Trino with Iceberg connector using Nessie catalog
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("iceberg", Map.of(
                        "connector.name", "iceberg",
                        "iceberg.catalog.type", "nessie",
                        "iceberg.nessie-catalog.uri", "http://" + NessieContainer.HOST_NAME + ":" + NessieContainer.NESSIE_PORT + "/api/v2",
                        "iceberg.nessie-catalog.default-warehouse-dir", "hdfs://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HDFS_NAMENODE_PORT + "/user/hive/warehouse",
                        "fs.hadoop.enabled", "true"))
                .build();
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    // Nessie access methods

    /**
     * Returns the external Nessie URI for connecting from the host.
     */
    public String getExternalNessieUri()
    {
        return nessie.getExternalNessieUri();
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
        if (nessie != null) {
            nessie.close();
            nessie = null;
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
}
