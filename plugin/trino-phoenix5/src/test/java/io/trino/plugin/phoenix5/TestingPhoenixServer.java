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
package io.trino.plugin.phoenix5;

import io.airlift.log.Logger;
import io.trino.testing.SharedResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.phoenix.query.HBaseFactoryProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.logging.Level;

import static java.lang.String.format;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.MASTER_INFO_PORT;
import static org.apache.hadoop.hbase.HConstants.REGIONSERVER_INFO_PORT;

public final class TestingPhoenixServer
        implements AutoCloseable
{
    private static final Logger LOG = Logger.get(TestingPhoenixServer.class);

    private static final SharedResource<TestingPhoenixServer> sharedResource = new SharedResource<>(TestingPhoenixServer::new);

    public static synchronized SharedResource.Lease<TestingPhoenixServer> getInstance()
            throws Exception
    {
        return sharedResource.getInstanceLease();
    }

    private HBaseTestingUtility hbaseTestingUtility;
    private final int port;
    private final Configuration conf = HBaseConfiguration.create();

    private final java.util.logging.Logger apacheLogger;

    private final java.util.logging.Logger zookeeperLogger;

    private final java.util.logging.Logger securityLogger;

    private TestingPhoenixServer()
    {
        // keep references to prevent GC from resetting the log levels
        apacheLogger = java.util.logging.Logger.getLogger("org.apache");
        apacheLogger.setLevel(Level.SEVERE);
        zookeeperLogger = java.util.logging.Logger.getLogger("org.apache.phoenix.shaded.org.apache.zookeeper.server.ZooKeeperServer");
        zookeeperLogger.setLevel(Level.OFF);
        securityLogger = java.util.logging.Logger.getLogger("SecurityLogger.org.apache");
        securityLogger.setLevel(Level.SEVERE);
        // to squelch the SecurityLogger,
        // instantiate logger with config above before config is overridden again in HBase test franework
        org.apache.commons.logging.LogFactory.getLog("SecurityLogger.org.apache.hadoop.hbase.server");
        this.conf.set("hbase.security.logger", "ERROR");
        this.conf.setInt(MASTER_INFO_PORT, -1);
        this.conf.setInt(REGIONSERVER_INFO_PORT, -1);
        this.conf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 15);
        this.conf.setBoolean("phoenix.schema.isNamespaceMappingEnabled", true);
        this.conf.set("hbase.regionserver.wal.codec", "org.apache.hadoop.hbase.regionserver.wal.IndexedWALEditCodec");
        this.hbaseTestingUtility = new HBaseTestingUtility(conf);

        try {
            MiniZooKeeperCluster zkCluster = this.hbaseTestingUtility.startMiniZKCluster();
            port = zkCluster.getClientPort();

            StartMiniClusterOption option = StartMiniClusterOption.builder().numMasters(1).numRegionServers(4).build();
            MiniHBaseCluster hbaseCluster = hbaseTestingUtility.startMiniHBaseCluster(option);
            hbaseCluster.waitForActiveAndReadyMaster();
            LOG.info("Phoenix server ready: %s", getJdbcUrl());
        }
        catch (Exception e) {
            throw new RuntimeException("Can't start phoenix server.", e);
        }
    }

    public Connection getConnection()
            throws IOException
    {
        return HBaseFactoryProvider.getHConnectionFactory().createConnection(this.conf);
    }

    @Override
    public void close()
    {
        if (hbaseTestingUtility == null) {
            return;
        }
        try {
            LOG.info("Shutting down HBase cluster.");
            hbaseTestingUtility.shutdownMiniHBaseCluster();
            hbaseTestingUtility.shutdownMiniZKCluster();
        }
        catch (IOException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException("Failed to shutdown HBaseTestingUtility instance", e);
        }
        hbaseTestingUtility = null;
    }

    public String getJdbcUrl()
    {
        return format("jdbc:phoenix:localhost:%d:/hbase;phoenix.schema.isNamespaceMappingEnabled=true", port);
    }
}
