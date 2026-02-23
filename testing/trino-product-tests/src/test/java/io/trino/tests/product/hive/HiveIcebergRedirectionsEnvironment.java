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

import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.HdfsClient;
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

import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hiveCatalog;

/**
 * Hive/Iceberg product test environment with table redirections.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with HDFS, Hive Metastore, and HiveServer2</li>
 *   <li>Trino container with both Hive and Iceberg connectors</li>
 *   <li>Hive-to-Iceberg table redirections enabled</li>
 * </ul>
 * <p>
 * The "hive" catalog is configured with {@code hive.iceberg-catalog-name=iceberg}
 * to enable automatic redirection of Iceberg tables accessed through the Hive
 * catalog to the Iceberg catalog for proper Iceberg-native handling.
 * <p>
 * This enables testing of Trino's table redirection feature, which allows
 * users to transparently access Iceberg tables through the Hive catalog
 * while still getting full Iceberg functionality.
 */
public class HiveIcebergRedirectionsEnvironment
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

    private Network network;
    private HadoopContainer hadoop;
    private TrinoContainer trino;

    @Override
    public void start()
    {
        if (trino != null && trino.isRunning()) {
            return; // Already started
        }

        network = Network.newNetwork();

        // Start Hadoop (provides HDFS, Hive Metastore, and HiveServer2)
        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        hadoop.start();

        String metastoreUri = "thrift://" + HadoopContainer.HOST_NAME + ":" + HadoopContainer.HIVE_METASTORE_PORT;

        // Start Trino with both Hive and Iceberg connectors
        // Bi-directional redirections: Hive redirects Iceberg tables to iceberg catalog,
        // and Iceberg redirects Hive tables to hive catalog
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                .withCatalog("hive", hiveCatalog(metastoreUri)
                        .withHadoopFileSystem()
                        .withCommonProperties()
                        .put("hive.iceberg-catalog-name", "iceberg")
                        .build())
                .withCatalog("iceberg", Map.of(
                        "connector.name", "iceberg",
                        "iceberg.catalog.type", "hive_metastore",
                        "hive.metastore.uri", metastoreUri,
                        "fs.hadoop.enabled", "true",
                        "hive.config.resources", "/etc/trino/hdfs-site.xml",
                        "iceberg.hive-catalog-name", "hive"))
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .build();
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
            initializeHiveReferenceTables();
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    private void initializeHiveReferenceTables()
    {
        executeTrinoUpdate("""
                CREATE TABLE IF NOT EXISTS hive.default.region AS
                SELECT
                    CAST(regionkey AS BIGINT) AS r_regionkey,
                    CAST(name AS VARCHAR(25)) AS r_name,
                    CAST(comment AS VARCHAR(152)) AS r_comment
                FROM tpch.tiny.region
                """);
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
