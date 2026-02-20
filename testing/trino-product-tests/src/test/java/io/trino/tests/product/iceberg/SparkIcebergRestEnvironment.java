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

import io.trino.testing.TestingProperties;
import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.IcebergRestCatalogContainer;
import io.trino.testing.containers.SparkIcebergContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import org.testcontainers.containers.GenericContainer;
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
import java.util.Map;

/**
 * Spark/Iceberg with REST Catalog product test environment for interoperability testing.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with HDFS (for data storage)</li>
 *   <li>Iceberg REST Catalog server (for metadata)</li>
 *   <li>Spark container with Iceberg support configured for REST catalog</li>
 *   <li>Trino container with Iceberg connector configured for REST catalog</li>
 * </ul>
 * <p>
 * Unlike the {@link SparkIcebergEnvironment} which uses Hive Metastore for metadata,
 * this environment uses an Iceberg REST Catalog server. Both Spark and Trino connect
 * to the REST server for metadata operations while still using HDFS for data storage.
 * <p>
 * Catalog configuration:
 * <ul>
 *   <li>Trino uses "iceberg" catalog pointing to REST server</li>
 *   <li>Spark uses "iceberg_test" catalog pointing to REST server</li>
 *   <li>Both share the same REST catalog, enabling cross-engine table access</li>
 * </ul>
 */
public class SparkIcebergRestEnvironment
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
    private IcebergRestCatalogContainer restCatalog;
    private GenericContainer<?> spark;
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

        // Start REST Catalog server (depends on HDFS being available)
        restCatalog = new IcebergRestCatalogContainer()
                .withNetwork(network)
                .withNetworkAliases(IcebergRestCatalogContainer.HOST_NAME);
        restCatalog.dependsOn(hadoop);
        restCatalog.start();

        // Start Spark with REST catalog configuration
        spark = createSparkRestContainer()
                .withNetwork(network)
                .withNetworkAliases(SparkIcebergContainer.HOST_NAME);
        spark.dependsOn(restCatalog);
        spark.start();

        // Start Trino with Iceberg REST catalog connector
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("iceberg", Map.of(
                        "connector.name", "iceberg",
                        "iceberg.catalog.type", "rest",
                        "iceberg.rest-catalog.uri", restCatalog.getRestUri(),
                        "fs.hadoop.enabled", "true",
                        "iceberg.allowed-extra-properties", "custom.table-property"))
                .build();
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    /**
     * Creates a Spark container configured for REST catalog instead of HMS.
     */
    private GenericContainer<?> createSparkRestContainer()
    {
        String sparkDefaultsConf = """
                spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
                spark.sql.defaultCatalog=iceberg_test
                spark.sql.catalog.iceberg_test=org.apache.iceberg.spark.SparkCatalog
                spark.sql.catalog.iceberg_test.catalog-impl=org.apache.iceberg.rest.RESTCatalog
                spark.sql.catalog.iceberg_test.uri=http://iceberg-with-rest:8181/
                spark.sql.catalog.iceberg_test.cache-enabled=false
                spark.sql.catalog.iceberg_test.warehouse=hdfs://hadoop-master:9000/user/hive/warehouse
                spark.hadoop.fs.defaultFS=hdfs://hadoop-master:9000
                """;

        String log4j2Properties = """
                rootLogger.level = WARN
                rootLogger.appenderRef.console.ref = console
                appender.console.type = Console
                appender.console.name = console
                appender.console.target = SYSTEM_ERR
                appender.console.layout.type = PatternLayout
                appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n%ex
                """;

        String imageName = "ghcr.io/trinodb/testing/spark4-iceberg:" + TestingProperties.getDockerImagesVersion();

        return new GenericContainer<>(DockerImageName.parse(imageName))
                .withExposedPorts(SparkIcebergContainer.SPARK_THRIFT_PORT)
                .withEnv("HADOOP_USER_NAME", "hive")
                .withCopyToContainer(
                        Transferable.of(sparkDefaultsConf),
                        "/spark/conf/spark-defaults.conf")
                .withCopyToContainer(
                        Transferable.of(log4j2Properties),
                        "/spark/conf/log4j2.properties")
                .withCommand(
                        "spark-submit",
                        "--master", "local[*]",
                        "--class", "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2",
                        "--name", "Thrift JDBC/ODBC Server",
                        "--packages", "org.apache.spark:spark-avro_2.12:3.2.1",
                        "--conf", "spark.hive.server2.thrift.port=" + SparkIcebergContainer.SPARK_THRIFT_PORT,
                        "spark-internal")
                .waitingFor(Wait.forListeningPort()
                        .withStartupTimeout(Duration.ofMinutes(3)));
    }

    // Spark JDBC methods

    /**
     * Creates a JDBC connection to the Spark Thrift Server.
     */
    public Connection createSparkConnection()
            throws SQLException
    {
        String jdbcUrl = "jdbc:hive2://" + spark.getHost() + ":" + spark.getMappedPort(SparkIcebergContainer.SPARK_THRIFT_PORT);
        return DriverManager.getConnection(jdbcUrl, "hive", "");
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

    // REST catalog methods

    /**
     * Returns the internal REST catalog URI (for containers on the same network).
     */
    public String getRestCatalogUri()
    {
        return restCatalog.getRestUri();
    }

    /**
     * Returns the external REST catalog URI (for connecting from the host).
     */
    public String getExternalRestCatalogUri()
    {
        return restCatalog.getExternalRestUri();
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
        if (restCatalog != null) {
            restCatalog.close();
            restCatalog = null;
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
