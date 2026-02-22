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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hiveCatalog;

/**
 * Basic Hive product test environment.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with HDFS, Hive Metastore, and HiveServer2</li>
 *   <li>Trino container with the Hive connector</li>
 * </ul>
 * <p>
 * The "hive" catalog is configured to connect to the Hive Metastore
 * and use HDFS for storage.
 */
public class HiveBasicEnvironment
        extends ProductTestEnvironment
{
    private static final Pattern HIVE_VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+).*");
    private static final String HIVE_METASTORE_URI = HiveCatalogPropertiesBuilder.hadoopMetastoreUri();

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
    private Connection hiveSessionConnection;

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

        // Start Trino with Hive connector and TPCH catalog
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                .withCatalog("hive", partitionAwareHiveCatalogProperties())
                // Second catalog for testing external table writes
                .withCatalog("hive_with_external_writes", hiveCatalog(HIVE_METASTORE_URI)
                        .withHadoopFileSystem()
                        .withCommonProperties()
                        .withPartitionProcedures()
                        .put("hive.non-managed-table-writes-enabled", "true")
                        .build())
                // Catalog for testing nanosecond timestamp precision
                .withCatalog("hive_timestamp_nanos", hiveCatalog(HIVE_METASTORE_URI)
                        .withHadoopFileSystem()
                        .withCommonProperties()
                        .put("hive.timestamp-precision", "NANOSECONDS")
                        .build())
                // Catalog for testing run-as-invoker mode for views
                .withCatalog("hive_with_run_view_as_invoker", hiveCatalog(HIVE_METASTORE_URI)
                        .withHadoopFileSystem()
                        .withCommonProperties()
                        .put("hive.hive-views.run-as-invoker", "true")
                        .put("hive.security", "sql-standard")
                        .build())
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .build();
        trino.start();

        try {
            TrinoProductTestContainer.waitForClusterReady(trino);
            initializeHiveTpchTables();
        }
        catch (SQLException | InterruptedException e) {
            throw new RuntimeException("Failed to wait for Trino cluster", e);
        }
    }

    private static Map<String, String> partitionAwareHiveCatalogProperties()
    {
        return hiveCatalog(HIVE_METASTORE_URI)
                .withHadoopFileSystem()
                .withCommonProperties()
                .withPartitionProcedures()
                .put("hive.max-partitions-per-scan", "100")
                .build();
    }

    private void initializeHiveTpchTables()
    {
        executeTrinoUpdate("""
                CREATE TABLE IF NOT EXISTS hive.default.nation AS
                SELECT
                    CAST(nationkey AS BIGINT) AS n_nationkey,
                    CAST(name AS VARCHAR(25)) AS n_name,
                    CAST(regionkey AS BIGINT) AS n_regionkey,
                    CAST(comment AS VARCHAR(152)) AS n_comment
                FROM tpch.tiny.nation
                """);

        executeTrinoUpdate("""
                CREATE TABLE IF NOT EXISTS hive.default.region AS
                SELECT
                    CAST(regionkey AS BIGINT) AS r_regionkey,
                    CAST(name AS VARCHAR(25)) AS r_name,
                    CAST(comment AS VARCHAR(152)) AS r_comment
                FROM tpch.tiny.region
                """);

        executeTrinoUpdate("""
                CREATE TABLE IF NOT EXISTS hive.default.orders AS
                SELECT
                    CAST(orderkey AS BIGINT) AS o_orderkey,
                    CAST(custkey + 1000 AS BIGINT) AS o_custkey,
                    CAST(orderstatus AS VARCHAR(1)) AS o_orderstatus,
                    CAST(totalprice AS DOUBLE) AS o_totalprice,
                    CAST(orderdate AS DATE) AS o_orderdate,
                    CAST(orderpriority AS VARCHAR(15)) AS o_orderpriority,
                    CAST(clerk AS VARCHAR(15)) AS o_clerk,
                    CAST(shippriority AS INTEGER) AS o_shippriority,
                    CAST(comment AS VARCHAR(79)) AS o_comment
                FROM tpch.tiny.orders
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
        try (Statement stmt = getHiveSessionConnection().createStatement();
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
        try (Statement stmt = getHiveSessionConnection().createStatement()) {
            return stmt.executeUpdate(sql);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute Hive update: " + sql, e);
        }
    }

    private Connection getHiveSessionConnection()
            throws SQLException
    {
        if (hiveSessionConnection == null || hiveSessionConnection.isClosed()) {
            hiveSessionConnection = createHiveConnection();
            try (Statement statement = hiveSessionConnection.createStatement()) {
                statement.execute("set hive.stats.column.autogather=false");
                statement.execute("set hive.stats.autogather=false");
            }
        }
        return hiveSessionConnection;
    }

    private void closeHiveSessionConnection()
    {
        if (hiveSessionConnection == null) {
            return;
        }
        try {
            hiveSessionConnection.close();
        }
        catch (SQLException ignored) {
            // Best-effort cleanup of per-test Hive session connection
        }
        hiveSessionConnection = null;
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
     *
     * @return the HDFS warehouse path (e.g., "/user/hive/warehouse")
     */
    public String getWarehouseDirectory()
    {
        return hadoop.getWarehouseDirectory();
    }

    // Hive version detection

    /**
     * Detects if Hive has broken Avro timestamps.
     * In 3.1.0 timestamp semantics in hive changed in backward incompatible way,
     * which was fixed for Parquet and Avro in 3.1.2 (https://issues.apache.org/jira/browse/HIVE-21002)
     */
    public boolean isHiveWithBrokenAvroTimestamps()
    {
        int[] version = getHiveVersion();
        if (version == null) {
            return false;
        }
        return version[0] == 3 &&
                version[1] == 1 &&
                (version[2] == 0 || version[2] == 1);
    }

    /**
     * Returns the Hive version as an array [major, minor, patch], or null if version cannot be detected.
     */
    private int[] getHiveVersion()
    {
        try (Connection conn = createHiveConnection()) {
            String versionString = conn.getMetaData().getDatabaseProductVersion();
            Matcher matcher = HIVE_VERSION_PATTERN.matcher(versionString);
            if (!matcher.matches()) {
                return null;
            }
            return new int[] {
                    Integer.parseInt(matcher.group(1)),
                    Integer.parseInt(matcher.group(2)),
                    Integer.parseInt(matcher.group(3))
            };
        }
        catch (SQLException e) {
            return null;
        }
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

    /**
     * Creates a JDBC connection to Trino without a default catalog/schema.
     * <p>
     * This is useful for testing scenarios where queries must fully qualify table names.
     */
    public Connection createTrinoConnectionWithoutDefaultCatalog()
            throws SQLException
    {
        return TrinoProductTestContainer.createConnection(trino, "hive");
    }

    /**
     * Creates a JDBC connection to Trino as the specified user.
     * <p>
     * This is useful for testing user-specific behavior such as view invoker mode.
     *
     * @param user the user name to connect as
     */
    public Connection createTrinoConnectionAsUser(String user)
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
    protected void afterEachTest()
    {
        closeHiveSessionConnection();
    }

    @Override
    protected void doClose()
    {
        closeHiveSessionConnection();

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
