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

import io.trino.testing.containers.Hive4HiveServerContainer;
import io.trino.testing.containers.Hive4MetastoreContainer;
import io.trino.testing.containers.Minio;
import io.trino.testing.containers.SparkHudiContainer;
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

import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hiveCatalog;

/**
 * Hive/Hudi product test environment with table redirections using S3 (MinIO) storage.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>MinIO container providing S3-compatible object storage</li>
 *   <li>Hive 4 Metastore container (standalone metastore service)</li>
 *   <li>Hive 4 HiveServer2 container (connects to remote metastore)</li>
 *   <li>Spark container with Hudi support configured for S3 storage</li>
 *   <li>Trino container with both Hive and Hudi connectors</li>
 *   <li>Hive-to-Hudi table redirections enabled</li>
 * </ul>
 * <p>
 * The "hive" catalog is configured with {@code hive.hudi-catalog-name=hudi}
 * to enable automatic redirection of Hudi tables accessed through the Hive
 * catalog to the Hudi catalog for proper Hudi-native handling.
 * <p>
 * This enables testing of Trino's table redirection feature for Hudi tables,
 * allowing users to transparently access Hudi tables through the Hive catalog
 * while getting full Hudi functionality like time-travel queries.
 */
public class HiveHudiRedirectionsEnvironment
        extends ProductTestEnvironment
{
    static {
        // Ensure the Hive JDBC driver is loaded for HiveServer2 and Spark Thrift Server connections
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Hive JDBC driver. " +
                    "Ensure hive-apache-jdbc dependency is on the classpath.", e);
        }
    }

    private static final String BUCKET_NAME = "hudi-test-bucket";

    private Network network;
    private Minio minio;
    private Hive4MetastoreContainer metastore;
    private Hive4HiveServerContainer hiveServer;
    private SparkHudiContainer spark;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        // Start MinIO first (provides S3-compatible storage)
        minio = Minio.builder()
                .withNetwork(network)
                .build();
        minio.start();
        minio.createBucket(BUCKET_NAME);

        // Configure warehouse path to use S3 storage
        String warehouseDir = "s3a://" + BUCKET_NAME + "/warehouse";
        String metastoreUri = Hive4MetastoreContainer.getInternalMetastoreUri();

        // Start Hive 4 Metastore (standalone metastore service)
        metastore = new Hive4MetastoreContainer()
                .withNetwork(network)
                .withNetworkAliases(Hive4MetastoreContainer.HOST_NAME)
                .withWarehouseDir(warehouseDir);
        metastore.start();

        // Start Hive 4 HiveServer2 (connects to remote metastore)
        hiveServer = new Hive4HiveServerContainer()
                .withNetwork(network)
                .withNetworkAliases(Hive4HiveServerContainer.HOST_NAME)
                .withMetastoreUri(metastoreUri)
                .withWarehouseDir(warehouseDir);
        hiveServer.start();

        // Start Spark with Hudi support configured for S3 storage
        spark = new SparkHudiContainer()
                .withNetwork(network)
                .withNetworkAliases(SparkHudiContainer.HOST_NAME)
                .withS3Config(
                        Minio.MINIO_ROOT_USER,
                        Minio.MINIO_ROOT_PASSWORD,
                        Minio.DEFAULT_HOST_NAME,
                        Minio.MINIO_API_PORT,
                        metastoreUri,
                        warehouseDir);
        spark.start();

        // Start Trino with both Hive and Hudi connectors
        // Hive catalog configured to redirect Hudi tables to the hudi catalog
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withCatalog("hive", hiveCatalog(metastoreUri)
                        .withMinioS3()
                        .withCommonProperties()
                        .withPartitionProcedures()
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .put("hive.hudi-catalog-name", "hudi")
                        .build())
                .withCatalog("hudi", HiveCatalogPropertiesBuilder.hudiCatalog(metastoreUri)
                        .withMinioS3()
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

    // Spark JDBC methods (for Hudi table management)

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

    // Hive JDBC methods (connects to HiveServer2)

    /**
     * Creates a JDBC connection to HiveServer2.
     */
    public Connection createHiveConnection()
            throws SQLException
    {
        return DriverManager.getConnection(hiveServer.getJdbcUrl(), "hive", "");
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

    // MinIO access

    /**
     * Creates a MinioClient for direct access to the MinIO (S3-compatible) storage.
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
     * Returns the name of the test bucket created in MinIO.
     *
     * @return the bucket name
     */
    public String getBucketName()
    {
        return BUCKET_NAME;
    }

    /**
     * Returns the S3 path for the warehouse directory.
     *
     * @return the S3 warehouse path (e.g., "s3a://hudi-test-bucket/warehouse")
     */
    public String getWarehousePath()
    {
        return "s3a://" + BUCKET_NAME + "/warehouse";
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
        if (hiveServer != null) {
            hiveServer.close();
            hiveServer = null;
        }
        if (metastore != null) {
            metastore.close();
            metastore = null;
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
