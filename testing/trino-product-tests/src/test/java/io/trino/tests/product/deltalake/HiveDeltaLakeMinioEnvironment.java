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
import java.util.Map;

/**
 * Hive and Delta Lake product test environment with S3-compatible storage (Minio)
 * and table redirections enabled.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Minio container providing S3-compatible object storage</li>
 *   <li>Hadoop container for Hive Metastore only (HDFS is not used)</li>
 *   <li>Spark container with Delta Lake support and Thrift Server configured for S3</li>
 *   <li>Trino container with both Hive and Delta Lake connectors configured for S3 (Minio)</li>
 * </ul>
 * <p>
 * Catalog configuration:
 * <ul>
 *   <li>Trino uses "delta" catalog for Delta Lake tables stored in Minio (S3)</li>
 *   <li>Trino uses "hive" catalog for Hive tables, with delta_lake_catalog_name=delta for redirections</li>
 *   <li>Spark uses "spark_catalog" with DeltaCatalog, also configured for S3 (Minio)</li>
 *   <li>All share the same Hive Metastore for table metadata and the same S3 storage</li>
 * </ul>
 * <p>
 * This environment enables testing of Hive-to-Delta Lake table redirections, where Delta Lake
 * tables accessed through the Hive catalog are automatically redirected to the Delta Lake
 * catalog for proper handling.
 */
public class HiveDeltaLakeMinioEnvironment
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

    private static final String BUCKET_NAME = "delta-hive-redirect-bucket";

    private Network network;
    private Minio minio;
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

        // Start Minio first (provides S3-compatible storage)
        minio = Minio.builder()
                .withNetwork(network)
                .build();
        minio.start();

        // Create the test bucket
        minio.createBucket(BUCKET_NAME);

        // Start Hadoop (provides HMS only - HDFS is not used by Trino in this environment)
        // Configure S3 so HMS can validate s3:// table locations
        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME)
                .withS3Config(
                        "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT,
                        Minio.MINIO_ROOT_USER,
                        Minio.MINIO_ROOT_PASSWORD);
        hadoop.start();

        // Start Spark with Delta Lake configured for S3 (Minio)
        spark = new SparkDeltaContainer()
                .withNetwork(network)
                .withNetworkAliases(SparkDeltaContainer.HOST_NAME)
                .withS3Config(
                        "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT,
                        Minio.MINIO_ROOT_USER,
                        Minio.MINIO_ROOT_PASSWORD,
                        Minio.MINIO_REGION)
                .withWarehouseDir("s3a://" + BUCKET_NAME + "/warehouse")
                .build();
        spark.dependsOn(hadoop);
        spark.start();

        String metastoreUri = "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT;

        // Start Trino with both Hive and Delta Lake connectors configured for S3 (Minio)
        // Bi-directional redirections: Hive redirects Delta Lake tables to delta catalog,
        // and Delta Lake redirects Hive tables to hive catalog
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("delta", Map.of(
                        "connector.name", "delta_lake",
                        "hive.metastore.uri", metastoreUri,
                        "fs.native-s3.enabled", "true",
                        "s3.endpoint", "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT,
                        "s3.aws-access-key", Minio.MINIO_ROOT_USER,
                        "s3.aws-secret-key", Minio.MINIO_ROOT_PASSWORD,
                        "s3.path-style-access", "true",
                        "s3.region", Minio.MINIO_REGION,
                        "delta.hive-catalog-name", "hive"))
                .withCatalog("hive", Map.ofEntries(
                        Map.entry("connector.name", "hive"),
                        Map.entry("hive.metastore.uri", metastoreUri),
                        Map.entry("hive.non-managed-table-writes-enabled", "true"),
                        Map.entry("fs.native-s3.enabled", "true"),
                        Map.entry("s3.endpoint", "http://" + Minio.DEFAULT_HOST_NAME + ":" + Minio.MINIO_API_PORT),
                        Map.entry("s3.aws-access-key", Minio.MINIO_ROOT_USER),
                        Map.entry("s3.aws-secret-key", Minio.MINIO_ROOT_PASSWORD),
                        Map.entry("s3.path-style-access", "true"),
                        Map.entry("s3.region", Minio.MINIO_REGION),
                        Map.entry("hive.hive-views.enabled", "true"),
                        Map.entry("hive.delta-lake-catalog-name", "delta")))
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
        return minio.createMinioClient();
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
        return trino != null ? trino.getJdbcUrl() : null;
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
