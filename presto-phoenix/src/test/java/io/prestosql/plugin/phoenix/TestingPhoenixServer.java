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
package io.prestosql.plugin.phoenix;

import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.phoenix.shaded.org.apache.zookeeper.server.ZooKeeperServer;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static io.prestosql.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static java.lang.String.format;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.MASTER_INFO_PORT;
import static org.apache.hadoop.hbase.HConstants.REGIONSERVER_INFO_PORT;

public final class TestingPhoenixServer
{
    private static final Logger LOG = Logger.get(TestingPhoenixServer.class);
    private static final Object INSTANCE_LOCK = new Object();

    @GuardedBy("INSTANCE_LOCK")
    private static int referenceCount;
    @GuardedBy("INSTANCE_LOCK")
    private static TestingPhoenixServer instance;

    public static TestingPhoenixServer getInstance()
    {
        synchronized (INSTANCE_LOCK) {
            if (referenceCount == 0) {
                instance = new TestingPhoenixServer();
            }
            referenceCount++;
            return instance;
        }
    }

    public static void shutDown()
    {
        synchronized (INSTANCE_LOCK) {
            referenceCount--;
            if (referenceCount == 0) {
                instance.shutdown();
                instance = null;
            }
        }
    }

    private HBaseTestingUtility hbaseTestingUtility;
    private final int port;
    private final Configuration conf = HBaseConfiguration.create();
    private final AtomicBoolean tpchLoaded = new AtomicBoolean();
    private CountDownLatch tpchLoadComplete = new CountDownLatch(1);

    private TestingPhoenixServer()
    {
        java.util.logging.Logger.getLogger("org.apache").setLevel(Level.SEVERE);
        java.util.logging.Logger.getLogger(ZooKeeperServer.class.getName()).setLevel(Level.OFF);
        java.util.logging.Logger.getLogger("SecurityLogger.org.apache").setLevel(Level.SEVERE);
        // to squelch the SecurityLogger,
        // instantiate logger with config above before config is overriden again in HBase test franework
        org.apache.commons.logging.LogFactory.getLog("SecurityLogger.org.apache.hadoop.hbase.server");
        this.conf.set("hbase.security.logger", "ERROR");
        this.conf.setInt(MASTER_INFO_PORT, -1);
        this.conf.setInt(REGIONSERVER_INFO_PORT, -1);
        this.conf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 1);
        this.conf.setBoolean("phoenix.schema.isNamespaceMappingEnabled", true);
        this.hbaseTestingUtility = new HBaseTestingUtility(conf);

        try {
            MiniZooKeeperCluster zkCluster = this.hbaseTestingUtility.startMiniZKCluster();
            port = zkCluster.getClientPort();

            MiniHBaseCluster hbm = hbaseTestingUtility.startMiniHBaseCluster(1, 4);
            hbm.waitForActiveAndReadyMaster();
            LOG.info("Phoenix server ready: %s", getJdbcUrl());
        }
        catch (Exception e) {
            throw new IllegalStateException("Can't start phoenix server.", e);
        }
    }

    private void shutdown()
    {
        if (hbaseTestingUtility != null) {
            try {
                LOG.info("Shutting down HBase cluster.");
                hbaseTestingUtility.shutdownMiniHBaseCluster();
                hbaseTestingUtility.shutdownMiniZKCluster();
            }
            catch (IOException e) {
                Thread.currentThread().interrupt();
                throw new PrestoException(SERVER_SHUTTING_DOWN, "Failed to shutdown HBaseTestingUtility instance", e);
            }
            hbaseTestingUtility = null;
        }
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
        tpchLoadComplete.await();
    }
}
