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
package io.trino.plugin.phoenix;

import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.phoenix.shaded.org.apache.zookeeper.server.ZooKeeperServer;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static java.lang.String.format;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.MASTER_INFO_PORT;
import static org.apache.hadoop.hbase.HConstants.REGIONSERVER_INFO_PORT;

public final class TestingPhoenixServer
{
    private static final Logger LOG = Logger.get(TestingPhoenixServer.class);

    @GuardedBy("this")
    private static int referenceCount;
    @GuardedBy("this")
    private static TestingPhoenixServer instance;

    public static synchronized TestingPhoenixServer getInstance()
    {
        if (referenceCount == 0) {
            instance = new TestingPhoenixServer();
        }
        referenceCount++;
        return instance;
    }

    public static synchronized void shutDown()
    {
        referenceCount--;
        if (referenceCount == 0) {
            instance.shutdown();
            instance = null;
        }
    }

    private HBaseTestingUtility hbaseTestingUtility;
    private final int port;
    private final Configuration conf = HBaseConfiguration.create();
    private final AtomicBoolean tpchLoaded = new AtomicBoolean();
    private final CountDownLatch tpchLoadComplete = new CountDownLatch(1);

    private final java.util.logging.Logger apacheLogger;

    private final java.util.logging.Logger zookeeperLogger;

    private final java.util.logging.Logger securityLogger;

    private TestingPhoenixServer()
    {
        // keep references to prevent GC from resetting the log levels
        apacheLogger = java.util.logging.Logger.getLogger("org.apache");
        apacheLogger.setLevel(Level.SEVERE);
        zookeeperLogger = java.util.logging.Logger.getLogger(ZooKeeperServer.class.getName());
        zookeeperLogger.setLevel(Level.OFF);
        securityLogger = java.util.logging.Logger.getLogger("SecurityLogger.org.apache");
        securityLogger.setLevel(Level.SEVERE);
        // to squelch the SecurityLogger,
        // instantiate logger with config above before config is overriden again in HBase test franework
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

            MiniHBaseCluster hbaseCluster = hbaseTestingUtility.startMiniHBaseCluster(1, 4);
            hbaseCluster.waitForActiveAndReadyMaster();
            LOG.info("Phoenix server ready: %s", getJdbcUrl());
        }
        catch (Exception e) {
            throw new RuntimeException("Can't start phoenix server.", e);
        }
    }

    private void shutdown()
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

    public boolean isTpchLoaded()
    {
        return tpchLoaded.getAndSet(true);
    }

    public void setTpchLoaded()
    {
        tpchLoadComplete.countDown();
    }

    public void waitTpchLoaded()
            throws InterruptedException
    {
        tpchLoadComplete.await(2, TimeUnit.MINUTES);
    }
}
