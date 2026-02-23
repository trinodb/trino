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
import io.trino.testing.containers.Minio;
import io.trino.testing.containers.SparkDeltaContainer;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.minio.MinioClient;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Delta Lake product test environment with S3-compatible storage (Minio).
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Minio container providing S3-compatible object storage</li>
 *   <li>Hadoop container for Hive Metastore only (HDFS is not used by Trino)</li>
 *   <li>Spark container with Delta Lake support and Thrift Server</li>
 *   <li>Trino container with Delta Lake connector configured for S3 (Minio)</li>
 * </ul>
 * <p>
 * Catalog configuration:
 * <ul>
 *   <li>Trino uses "delta_lake" catalog for Delta Lake tables stored in Minio (S3)</li>
 *   <li>Spark uses "spark_catalog" with DeltaCatalog</li>
 *   <li>Both share the same Hive Metastore for table metadata</li>
 * </ul>
 * <p>
 * <b>Note:</b> The SparkDeltaContainer is currently configured with hardcoded HDFS settings,
 * so Spark writes to HDFS while Trino writes to S3 (Minio). This environment is primarily
 * designed for testing Trino's Delta Lake connector with S3-compatible storage. For full
 * Spark-Trino interoperability testing, use {@link DeltaLakeOssEnvironment} instead.
 */
public class DeltaLakeMinioEnvironment
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

    private static final String BUCKET_NAME = "delta-test-bucket";

    private Network network;
    private Minio minio;
    private HadoopContainer hadoop;
    private SparkDeltaContainer spark;
    private TrinoContainer trino;
    private final List<MinioClient> openMinioClients = new CopyOnWriteArrayList<>();

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        // Start Minio first (provides S3-compatible storage)
        minio = Minio.builder()
                .withNetwork(network)
                .build();
        minio.start();

        // Create the test bucket
        minio.createBucket(BUCKET_NAME);

        // Start Hadoop (provides HMS only - HDFS is not used by Trino in this environment)
        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        hadoop.start();

        // Start Spark with Delta Lake (depends on Hadoop for HMS)
        // Note: Spark writes to HDFS due to hardcoded configuration in SparkDeltaContainer
        spark = new SparkDeltaContainer()
                .withNetwork(network)
                .withNetworkAliases(SparkDeltaContainer.HOST_NAME);
        spark.dependsOn(hadoop);
        spark.start();

        // Start Trino with Delta Lake connector configured for S3 (Minio)
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("delta_lake", Map.of(
                        "connector.name", "delta_lake",
                        "hive.metastore.uri", "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT,
                        "fs.native-s3.enabled", "true",
                        "s3.endpoint", "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT,
                        "s3.aws-access-key", Minio.MINIO_ROOT_USER,
                        "s3.aws-secret-key", Minio.MINIO_ROOT_PASSWORD,
                        "s3.path-style-access", "true",
                        "s3.region", Minio.MINIO_REGION))
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

    // Minio access

    /**
     * Creates a MinioClient for direct access to the Minio (S3-compatible) storage.
     * <p>
     * The client should be closed when no longer needed to release resources.
     *
     * @return a new MinioClient
     */
    public MinioClient createMinioClient()
    {
        MinioClient client = minio.createMinioClient();
        openMinioClients.add(client);
        return client;
    }

    /**
     * Returns the name of the test bucket created in Minio.
     *
     * @return the bucket name
     */
    public String getBucketName()
    {
        return BUCKET_NAME;
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
    protected void afterEachTest()
    {
        closeOpenMinioClients();
    }

    private void closeOpenMinioClients()
    {
        for (MinioClient client : openMinioClients) {
            try {
                client.close();
            }
            catch (Exception ignored) {
            }
        }
        openMinioClients.clear();
    }

    @Override
    protected void doClose()
    {
        closeOpenMinioClients();
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
        if (minio != null) {
            minio.close();
            minio = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }
}
