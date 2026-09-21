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
import io.trino.testing.containers.MultiNodeTrinoCluster;
import io.trino.testing.containers.environment.ProductTestEnvironment;
import io.trino.testing.containers.environment.QueryResult;
import org.intellij.lang.annotations.Language;
import org.testcontainers.containers.Network;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static io.trino.tests.product.hive.HiveCatalogPropertiesBuilder.hiveCatalog;

/**
 * Multinode Hive product test environment with HDFS storage and filesystem caching.
 * <p>
 * This environment provides:
 * <ul>
 *   <li>HadoopContainer with HDFS NameNode, Hive Metastore, and HiveServer2</li>
 *   <li>Multinode Trino cluster (1 coordinator + 1 worker) with:
 *       <ul>
 *         <li><b>hive</b> catalog - with filesystem caching enabled</li>
 *         <li><b>hivenoncached</b> catalog - without caching (for comparison)</li>
 *       </ul>
 *   </li>
 * </ul>
 * <p>
 * <b>Architecture:</b>
 * <pre>
 * +-------------------------------------------------------------+
 * |                      Docker Network                          |
 * |                                                              |
 * |  +-----------------------------------------------------+     |
 * |  |              HadoopContainer (hadoop-master)        |     |
 * |  |  HDFS NameNode :9000 | Metastore :9083 | HS2 :10000 |     |
 * |  +-----------------------------------------------------+     |
 * |                              |                               |
 * |         +--------------------+--------------------+          |
 * |         |                                         |          |
 * |  +------+------+                         +--------+-------+  |
 * |  |   Trino     |                         |    Trino       |  |
 * |  | Coordinator |                         |    Worker      |  |
 * |  |   :8080     |                         |    :8080       |  |
 * |  |             |                         |                |  |
 * |  | tmpfs: /tmp/cache (mode=777)          | tmpfs: /tmp/cache |
 * |  +-------------+                         +----------------+  |
 * +-------------------------------------------------------------+
 * </pre>
 * <p>
 * <b>Caching configuration:</b>
 * <ul>
 *   <li>Cache directory: /tmp/cache/hive (tmpfs mount)</li>
 *   <li>Max disk usage: 90%</li>
 *   <li>Both coordinator and worker have identical tmpfs mounts</li>
 * </ul>
 */
public class MultinodeHiveCachingEnvironment
        extends ProductTestEnvironment
{
    private static final String CACHE_DIRECTORY = "/tmp/cache/hive";
    private static final String CACHE_MANAGER_PATH = "/etc/trino/cache-manager-alluxio.properties";

    private Network network;
    private HadoopContainer hadoop;
    private MultiNodeTrinoCluster trinoCluster;

    @Override
    public void start()
            throws SQLException, InterruptedException
    {
        if (trinoCluster != null) {
            return; // Already started
        }

        network = Network.newNetwork();

        // Start Hadoop container (HDFS + Hive Metastore + HiveServer2)
        hadoop = new HadoopContainer()
                .withNetwork(network)
                .withNetworkAliases(HadoopContainer.HOST_NAME);
        hadoop.start();

        // Hive catalog with filesystem caching enabled
        Map<String, String> hiveCatalog = hiveCatalog(hadoop.getHiveMetastoreUri())
                .withHadoopFileSystem()
                .put("fs.cache.enabled", "true")
                .build();

        // Hive catalog without caching (for comparison/baseline tests)
        Map<String, String> hiveNonCachedCatalog = hiveCatalog(hadoop.getHiveMetastoreUri())
                .withHadoopFileSystem()
                .put("hive.metastore-cache-ttl", "0s")
                .build();

        // Build multinode Trino cluster with tmpfs mounts for cache
        // Mount at /tmp/cache (parent directory) with mode=777 for write permissions
        // The hive catalog will create the /tmp/cache/hive subdirectory
        trinoCluster = MultiNodeTrinoCluster.builder()
                .withNetwork(network)
                .withWorkerCount(1)
                .withCatalog("hive", hiveCatalog)
                .withCatalog("hivenoncached", hiveNonCachedCatalog)
                .withCatalog("tpch", Map.of("connector.name", "tpch"))
                .withCatalog("jmx", Map.of("connector.name", "jmx"))
                .withConfigProperty("cache-manager.config-files", "etc/cache-manager-alluxio.properties")
                .withFile(
                        CACHE_MANAGER_PATH,
                        """
                        cache-manager.name=alluxio
                        fs.cache.directories=%s
                        fs.cache.max-disk-usage-percentages=90
                        """.formatted(CACHE_DIRECTORY))
                .withFile("/etc/trino/hdfs-site.xml", hadoop.getHdfsClientSiteXml())
                .withCoordinatorCustomizer(coordinator ->
                        coordinator.withTmpFs(Map.of("/tmp/cache", "rw,mode=777")))
                .withWorkerCustomizer(worker ->
                        worker.withTmpFs(Map.of("/tmp/cache", "rw,mode=777")))
                .build();
        trinoCluster.start();
        trinoCluster.waitForClusterReady();
    }

    // Hive access methods

    /**
     * Executes a Hive query using beeline on the Hadoop container.
     *
     * @param sql the SQL query to execute
     * @return the command output
     */
    public String runOnHive(@Language("SQL") String sql)
    {
        return hadoop.runOnHive(sql);
    }

    // HDFS access

    /**
     * Creates an HdfsClient for interacting with HDFS via WebHDFS.
     *
     * @return a new HdfsClient
     */
    public HdfsClient createHdfsClient()
    {
        return hadoop.createHdfsClient();
    }

    /**
     * Returns the default warehouse directory path in HDFS.
     */
    public String getWarehouseDirectory()
    {
        return hadoop.getWarehouseDirectory();
    }

    // Standard ProductTestEnvironment methods

    @Override
    public Connection createTrinoConnection()
            throws SQLException
    {
        return trinoCluster.createConnection();
    }

    @Override
    public Connection createTrinoConnection(String user)
            throws SQLException
    {
        return trinoCluster.createConnection(user);
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
        if (hadoop != null) {
            hadoop.close();
            hadoop = null;
        }
        if (network != null) {
            network.close();
            network = null;
        }
    }

    /**
     * Returns the Hadoop container.
     */
    public HadoopContainer getHadoop()
    {
        return hadoop;
    }

    /**
     * Returns the multinode Trino cluster.
     */
    public MultiNodeTrinoCluster getTrinoCluster()
    {
        return trinoCluster;
    }

    /**
     * Executes a query using the cached Hive catalog.
     */
    public QueryResult executeHiveCached(@Language("SQL") String sql)
    {
        try (Connection conn = createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return QueryResult.forResultSet(rs);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute cached Hive query: " + sql, e);
        }
    }

    /**
     * Executes a query using the non-cached Hive catalog.
     */
    public QueryResult executeHiveNonCached(@Language("SQL") String sql)
    {
        try (Connection conn = createTrinoConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            return QueryResult.forResultSet(rs);
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to execute non-cached Hive query: " + sql, e);
        }
    }

    /**
     * Retrieves filesystem cache statistics via the JMX catalog.
     * <p>
     * Returns cache metrics including cache reads and external reads from the
     * AlluxioCacheStats MBean. The JMX table name follows the pattern:
     * io.trino.blob.cache.alluxio:name={name},type=alluxiocachestats
     *
     * @return the cache statistics as a QueryResult
     */
    public QueryResult getCacheStatistics()
    {
        String sql =
                """
                SELECT node, "cachereads.alltime.count" as cache_reads, "externalreads.alltime.count" as external_reads
                FROM jmx.current."io.trino.blob.cache.alluxio:name=hive,type=alluxiocachestats"
                """;
        return executeTrino(sql);
    }
}
