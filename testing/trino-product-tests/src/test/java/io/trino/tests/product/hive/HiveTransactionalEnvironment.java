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

import io.trino.plugin.hive.metastore.thrift.DefaultThriftMetastoreClientFactory;
import io.trino.plugin.hive.metastore.thrift.NoHiveMetastoreAuthentication;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClientFactory;
import io.trino.testing.containers.HadoopContainer;
import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.TrinoProductTestContainer;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import org.apache.thrift.TException;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.Network;
import org.testcontainers.trino.TrinoContainer;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hiveCatalog;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testcontainers.utility.MountableFile.forClasspathResource;

/**
 * Hive product test environment with ACID/transactional table support.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>Hadoop container with HDFS, Hive Metastore, and HiveServer2</li>
 *   <li>Trino container with Hive connector configured for transactional tables</li>
 * </ul>
 * <p>
 * The "hive" catalog is configured with ACID support, including:
 * <ul>
 *   <li>Insert existing partitions behavior set to APPEND</li>
 *   <li>Support for Hive transactional (ACID) tables</li>
 * </ul>
 * <p>
 * Use this environment for testing Hive transactional/ACID table operations
 * such as INSERT, UPDATE, DELETE, and MERGE on ORC transactional tables.
 */
public class HiveTransactionalEnvironment
        extends ProductTestEnvironment
{
    private static final String PARTITION_COMPACTION_REQUIRED_MESSAGE = "You must specify a partition to compact for partitioned tables";
    private static final String HIVE_ACID_RESOURCE_BASE = "docker/trino-product-tests/conf/environment/singlenode-hive-acid/";
    private static final String HIVE_ACID_INIT_SCRIPT_RESOURCE = HIVE_ACID_RESOURCE_BASE + "apply-hive-config.sh";
    private static final String HIVE_ACID_INIT_SCRIPT_PATH = "/etc/hadoop-init.d/10-apply-hive-acid-config.sh";

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
                .withNetworkAliases(HadoopContainer.HOST_NAME)
                .withCopyFileToContainer(
                        forClasspathResource(HIVE_ACID_INIT_SCRIPT_RESOURCE, 0755),
                        HIVE_ACID_INIT_SCRIPT_PATH);
        hadoop.start();

        // Start Trino with Hive connector configured for ACID/transactional tables
        trino = TrinoProductTestContainer.builder()
                .withNetwork(network)
                .withHdfsConfiguration(hadoop.getHdfsClientSiteXml())
                .withCatalog("hive", hiveCatalog(HiveCatalogPropertiesBuilder.hadoopMetastoreUri())
                        .withHadoopFileSystem()
                        .withCommonProperties()
                        .withPartitionProcedures()
                        .put("hive.insert-existing-partitions-behavior", "APPEND")
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

    // Hive JDBC methods (connects to HiveServer2 in Hadoop container)

    /**
     * Creates a JDBC connection to HiveServer2 in the Hadoop container.
     */
    public Connection createHiveConnection()
            throws SQLException
    {
        return createHiveConnection(false);
    }

    private Connection createHiveConnection(boolean forQuery)
            throws SQLException
    {
        String jdbcUrl = "jdbc:hive2://" + hadoop.getHost() + ":" + hadoop.getMappedPort(HadoopContainer.HIVESERVER2_PORT) + "/default";
        Connection connection = DriverManager.getConnection(jdbcUrl, "hive", "");
        try (Statement statement = connection.createStatement()) {
            // Hive 3.1 local MapReduce + vectorized reduce sink is unstable for ACID row-level writes.
            statement.execute("SET hive.vectorized.execution.enabled=false");
            statement.execute("SET hive.vectorized.execution.reduce.enabled=false");
            // Legacy tests rely on dynamic partition inserts for ACID table population.
            statement.execute("SET hive.exec.dynamic.partition.mode=nonstrict");
            // Avoid StatsTask failures on ACID-converted/original-file flows.
            statement.execute("SET hive.stats.autogather=false");
            // Hive ACID reads require HiveInputFormat in Apache Hive 3.1, but this breaks row-level UPDATE/DELETE.
            if (forQuery) {
                statement.execute("SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat");
            }
        }
        return connection;
    }

    /**
     * Executes a SQL query against Hive and returns the result.
     *
     * @param sql the SQL query to execute
     * @return the query result
     */
    public QueryResult executeHive(String sql)
    {
        try (Connection conn = createHiveConnection(true);
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
            throw new RuntimeException("Failed to execute Hive update: " + sql + "\n" + hiveLogTail(), e);
        }
    }

    /**
     * Verifies that HiveServer2 is accessible and responsive.
     *
     * @return true if HiveServer2 is accessible
     */
    public boolean verifyHiveServer2Connectivity()
    {
        try (Connection conn = createHiveConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            return rs.next() && rs.getInt(1) == 1;
        }
        catch (SQLException e) {
            return false;
        }
    }

    // Compaction helpers for transactional tables

    /**
     * Initiates compaction on a table and waits for it to complete.
     * <p>
     * This method executes the ALTER TABLE COMPACT command via HiveServer2
     * and polls SHOW COMPACTIONS until the compaction succeeds or times out.
     *
     * @param mode the compaction mode ('major' or 'minor')
     * @param tableName the fully qualified table name (e.g., 'default.my_table')
     * @param timeout the maximum time to wait for compaction to complete
     * @throws RuntimeException if compaction fails or times out
     */
    public void compactTableAndWait(String mode, String tableName, Duration timeout)
    {
        try {
            executeHiveUpdate("ALTER TABLE " + tableName + " COMPACT '" + mode + "'");
        }
        catch (RuntimeException e) {
            // Legacy tests compacted the whole table. On Apache Hive 3.1, partitioned tables require
            // explicit PARTITION(...) compaction, so we preserve legacy intent by compacting each partition.
            if (containsMessage(e, PARTITION_COMPACTION_REQUIRED_MESSAGE)) {
                compactEachPartition(mode, tableName);
            }
            else {
                throw e;
            }
        }

        // Wait for compaction to complete
        long startTime = System.currentTimeMillis();
        long timeoutMillis = timeout.toMillis();

        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            String state = getCompactionState(tableName);
            if (state == null) {
                // No compaction found, might have completed very quickly
                return;
            }
            if ("succeeded".equalsIgnoreCase(state)) {
                return;
            }
            if ("failed".equalsIgnoreCase(state)) {
                throw new RuntimeException("Compaction failed for table: " + tableName);
            }
            // Still in progress (initiated, working, ready for cleaning)
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for compaction", e);
            }
        }
        throw new RuntimeException("Compaction timed out after " + timeout + " for table: " + tableName);
    }

    /**
     * Gets the current compaction state for a table.
     *
     * @param tableName the fully qualified table name
     * @return the compaction state, or null if no compaction is found
     */
    private String getCompactionState(String tableName)
    {
        try (Connection conn = createHiveConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW COMPACTIONS")) {
            // SHOW COMPACTIONS returns columns by index (column names vary by Hive version):
            // 1: compactionId, 2: dbname, 3: tabname, 4: partname, 5: type, 6: state, ...
            while (rs.next()) {
                String dbName = rs.getString(2);
                String tblName = rs.getString(3);
                String fullName = dbName + "." + tblName;
                if (fullName.equalsIgnoreCase(tableName) || tblName.equalsIgnoreCase(tableName)) {
                    return rs.getString(6);
                }
            }
            return null;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get compaction state for table: " + tableName, e);
        }
    }

    private void compactEachPartition(String mode, String tableName)
    {
        List<String> partitions = getTablePartitions(tableName);
        if (partitions.isEmpty()) {
            throw new RuntimeException("Compaction requested for partitioned table but no partitions found: " + tableName);
        }
        for (String partition : partitions) {
            String partitionSpec = partition.replace("/", ", ");
            executeHiveUpdate("ALTER TABLE " + tableName + " PARTITION (" + partitionSpec + ") COMPACT '" + mode + "'");
        }
    }

    private List<String> getTablePartitions(String tableName)
    {
        try (Connection conn = createHiveConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SHOW PARTITIONS " + tableName)) {
            List<String> partitions = new ArrayList<>();
            while (rs.next()) {
                partitions.add(rs.getString(1));
            }
            return partitions;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to list partitions for table: " + tableName, e);
        }
    }

    private static boolean containsMessage(Throwable throwable, String expected)
    {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            String message = current.getMessage();
            if (message != null && message.contains(expected)) {
                return true;
            }
        }
        return false;
    }

    private String hiveLogTail()
    {
        if (hadoop == null || !hadoop.isRunning()) {
            return "Hive log tail unavailable (container not running)";
        }
        try {
            ExecResult result = hadoop.execInContainer("bash", "-lc", "tail -n 120 /tmp/root/hive.log 2>/dev/null || tail -n 120 /var/log/hive/hive-server2.log 2>/dev/null || true");
            String output = result.getStdout().isBlank() ? result.getStderr() : result.getStdout();
            if (output.isBlank()) {
                return "Hive log tail unavailable (no output)";
            }
            return "Hive log tail:\n" + output;
        }
        catch (Exception ignored) {
            return "Hive log tail unavailable (log collection failed)";
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

    // Thrift Metastore access

    private static final ThriftMetastoreClientFactory THRIFT_METASTORE_CLIENT_FACTORY = new DefaultThriftMetastoreClientFactory(
            Optional.empty(),
            Optional.empty(),
            new io.airlift.units.Duration(10, SECONDS),
            new io.airlift.units.Duration(10, SECONDS),
            new NoHiveMetastoreAuthentication(),
            "localhost",
            Optional.empty());

    /**
     * Creates a ThriftMetastoreClient for direct metastore operations.
     * <p>
     * This client can be used for low-level metastore operations such as
     * opening/aborting transactions, allocating write IDs, etc.
     *
     * @return a new ThriftMetastoreClient connected to the Hive metastore
     * @throws TException if the connection fails
     */
    public ThriftMetastoreClient createThriftMetastoreClient()
            throws TException
    {
        URI metastoreUri = URI.create("thrift://" + hadoop.getHost() + ":" + hadoop.getMappedPort(HadoopContainer.HIVE_METASTORE_PORT));
        return THRIFT_METASTORE_CLIENT_FACTORY.create(metastoreUri, Optional.empty());
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
