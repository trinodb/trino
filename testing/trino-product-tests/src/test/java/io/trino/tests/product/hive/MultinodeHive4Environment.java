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
import io.trino.testing.containers.MultiNodeTrinoCluster;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.minio.MinioClient;
import org.testcontainers.containers.Network;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hiveCatalog;

/**
 * Multinode Hive 4 product test environment with S3-compatible storage (MinIO).
 * <p>
 * This environment provides:
 * <ul>
 *   <li>MinIO container providing S3-compatible object storage</li>
 *   <li>Hive 4 Metastore container (standalone metastore service)</li>
 *   <li>Hive 4 HiveServer2 container (connects to remote metastore)</li>
 *   <li>Multinode Trino cluster (1 coordinator + 1 worker) with Hive connector configured for S3</li>
 * </ul>
 * <p>
 * <b>Architecture:</b>
 * <pre>
 * +-------------------------------------------------------------+
 * |                      Docker Network                          |
 * |                                                              |
 * |  +-----------+                                               |
 * |  |   MinIO   |  S3-compatible storage                        |
 * |  |  (minio)  |                                               |
 * |  |   :4566   |                                               |
 * |  +-----+-----+                                               |
 * |        |                                                     |
 * |  +-----+-----+    +---------------+                          |
 * |  | Hive4     |<---| Hive4         |                          |
 * |  | Metastore |    | HiveServer2   |                          |
 * |  | :9083     |    | :10000        |                          |
 * |  +-----+-----+    +---------------+                          |
 * |        |                                                     |
 * |  +-----+-----+    +---------------+                          |
 * |  |   Trino   |<---|   Trino       |                          |
 * |  | Coordinator|   |   Worker      |                          |
 * |  |   :8080   |    |   :8080       |                          |
 * |  +-----------+    +---------------+                          |
 * +-------------------------------------------------------------+
 * </pre>
 * <p>
 * <b>Hive catalog configuration:</b>
 * <ul>
 *   <li>connector.name=hive</li>
 *   <li>fs.native-s3.enabled=true with MinIO endpoint</li>
 *   <li>Path-style S3 access for MinIO compatibility</li>
 * </ul>
 */
public class MultinodeHive4Environment
        extends ProductTestEnvironment
{
    static {
        // Ensure the Hive JDBC driver is loaded for HiveServer2 connections
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load Hive JDBC driver. " +
                    "Ensure hive-apache-jdbc dependency is on the classpath.", e);
        }
    }

    private static final String BUCKET_NAME = "multinode-hive4-test-bucket";

    private Network network;
    private Minio minio;
    private Hive4MetastoreContainer metastore;
    private Hive4HiveServerContainer hiveServer;
    private MultiNodeTrinoCluster trinoCluster;

    @Override
    public void start()
    {
        if (trinoCluster != null) {
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
                .withMetastoreUri(metastore.getInternalHiveMetastoreUri())
                .withWarehouseDir(warehouseDir);
        hiveServer.start();

        // Build Hive catalog configuration for S3 (MinIO)
        Map<String, String> hiveCatalog = hiveCatalog(metastore.getInternalHiveMetastoreUri())
                .withMinioS3()
                .withCommonProperties()
                .withPartitionProcedures()
                .withHadoopFileSystemDisabled()
                .put("hive.non-managed-table-writes-enabled", "true")
                .build();

        // Start multinode Trino cluster
        trinoCluster = MultiNodeTrinoCluster.builder()
                .withNetwork(network)
                .withWorkerCount(1)
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withCatalog("hive", hiveCatalog)
                .build();
        trinoCluster.start();

        try {
            trinoCluster.waitForClusterReady();
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    // Hive JDBC methods (connects to Hive 4 HiveServer2)

    /**
     * Creates a JDBC connection to Hive 4 HiveServer2.
     */
    public Connection createHiveConnection()
            throws SQLException
    {
        return DriverManager.getConnection(hiveServer.getJdbcUrl(), "hive", "");
    }

    /**
     * Executes a SQL query against Hive 4 and returns the result.
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
     * Executes a DDL or DML statement against Hive 4.
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

    /**
     * Executes a command in the HiveServer2 container using beeline.
     *
     * @param sql the SQL command to execute
     * @return the command output
     */
    public String runOnHive(String sql)
    {
        return hiveServer.runOnHive(sql);
    }

    // MinIO access

    /**
     * Creates a MinioClient for direct access to the MinIO (S3-compatible) storage.
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
     * @return the S3 warehouse path (e.g., "s3a://multinode-hive4-test-bucket/")
     */
    public String getWarehousePath()
    {
        return "s3a://" + BUCKET_NAME + "/";
    }

    // Standard ProductTestEnvironment methods

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return trinoCluster.createConnection("test", "hive", "default");
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return trinoCluster.createConnection(user, "hive", "default");
    }

    @Override
    public String getTrinoJdbcUrl()
    {
        return trinoCluster != null ? trinoCluster.getJdbcUrl() : null;
    }

    @Override
    public boolean isRunning()
    {
        return trinoCluster != null && trinoCluster.getCoordinator().isRunning();
    }

    @Override
    protected void doClose()
    {
        if (trinoCluster != null) {
            trinoCluster.close();
            trinoCluster = null;
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

    /**
     * Returns the Hive 4 Metastore container.
     */
    public Hive4MetastoreContainer getMetastore()
    {
        return metastore;
    }

    /**
     * Returns the Hive 4 HiveServer2 container.
     */
    public Hive4HiveServerContainer getHiveServer()
    {
        return hiveServer;
    }

    /**
     * Returns the MinIO container.
     */
    public Minio getMinio()
    {
        return minio;
    }

    /**
     * Returns the multinode Trino cluster.
     */
    public MultiNodeTrinoCluster getTrinoCluster()
    {
        return trinoCluster;
    }

    // Utility methods for distributed query testing

    /**
     * Returns the expected number of nodes in the cluster (coordinator + workers).
     *
     * @return the total number of nodes (2 in this environment: 1 coordinator + 1 worker)
     */
    public int getExpectedNodeCount()
    {
        return 2; // 1 coordinator + 1 worker
    }

    /**
     * Verifies that the cluster has all expected nodes active.
     *
     * @return true if all nodes are active, false otherwise
     */
    public boolean isClusterFullyActive()
    {
        try {
            QueryResult result = executeTrino("SELECT count(*) FROM system.runtime.nodes WHERE state = 'active'");
            return result.rows().size() == 1 &&
                    result.rows().getFirst().size() == 1 &&
                    ((Number) result.rows().getFirst().getFirst()).intValue() == getExpectedNodeCount();
        }
        catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Returns the number of active worker nodes in the cluster.
     *
     * @return the number of workers (1 in this environment)
     */
    public int getWorkerCount()
    {
        return 1;
    }
}
