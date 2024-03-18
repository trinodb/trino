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
package io.trino.plugin.warp.extension.execution.callhome;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.ConnectorSyncInitializedEvent;
import io.trino.plugin.warp.extension.configuration.CallHomeConfiguration;
import io.trino.spi.HostAddress;
import io.trino.spi.NodeManager;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.cloudvendors.configuration.CloudVendorConfiguration;
import io.varada.cloudvendors.configuration.StoreType;
import io.varada.tools.CatalogNameProvider;
import io.varada.tools.util.Version;
import jakarta.ws.rs.core.UriBuilder;

import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.varada.storage.engine.ConnectorSync.DEFAULT_CATALOG;
import static java.util.Objects.requireNonNull;

@Singleton
public class CallHomeService
{
    private static final Logger logger = Logger.get(CallHomeService.class);

    private static final String CALL_HOME_STORE_PATH_PREFIX = "/call-home/";
    private static final String LOG_PATH_PROPERTY_KEY1 = "log.output-file";
    private static final String LOG_PATH_PROPERTY_KEY2 = "node.server-log-file";
    private static final String DEFAULT_CATALOG_PATH = "/etc/presto/catalog";
    private static final String DEFAULT_SERVER_LOG_PATH = "/var/log/presto/server.log";
    static final String CATALOG_PROPERTY_KEY = "catalog.config-dir";
    private final ConnectorSync connectorSync;
    private final ScheduledExecutorService scheduledExecutorService;
    private final NodeManager nodeManager;
    private final CloudVendorConfiguration cloudVendorConfiguration;
    private final CallHomeConfiguration callHomeConfiguration;
    private final CloudVendorService cloudVendorService;
    private final HostAddress currentNodeAddress;
    private final String nodeStorePathPrefix;

    private ScheduledFuture<?> scheduledFuture;

    @SuppressWarnings("unused")
    @Inject
    public CallHomeService(ConnectorSync connectorSync,
            NodeManager nodeManager,
            CatalogNameProvider catalogNameProvider,
            @ForWarp CloudVendorConfiguration cloudVendorConfiguration,
            CallHomeConfiguration callHomeConfiguration,
            @ForWarp CloudVendorService cloudVendorService,
            EventBus eventBus)
    {
        this(connectorSync,
                nodeManager,
                catalogNameProvider,
                cloudVendorConfiguration,
                callHomeConfiguration,
                cloudVendorService,
                eventBus,
                Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = Executors.defaultThreadFactory().newThread(r);
                    t.setName("CallHomeService:call-home Thread-" + t.threadId());
                    t.setDaemon(true);
                    return t;
                }));
    }

    @VisibleForTesting
    public CallHomeService(ConnectorSync connectorSync,
            NodeManager nodeManager,
            CatalogNameProvider catalogNameProvider,
            CloudVendorConfiguration cloudVendorConfiguration,
            CallHomeConfiguration callHomeConfiguration,
            CloudVendorService cloudVendorService,
            EventBus eventBus,
            ScheduledExecutorService scheduledExecutorService)
    {
        this.connectorSync = requireNonNull(connectorSync);
        this.nodeManager = requireNonNull(nodeManager);
        this.cloudVendorConfiguration = requireNonNull(cloudVendorConfiguration);
        this.callHomeConfiguration = requireNonNull(callHomeConfiguration);
        this.cloudVendorService = requireNonNull(cloudVendorService);
        this.currentNodeAddress = nodeManager.getCurrentNode().getHostAndPort();
        this.nodeStorePathPrefix = UriBuilder.fromPath(cloudVendorConfiguration.getStorePath()).path(catalogNameProvider.get()).path(CALL_HOME_STORE_PATH_PREFIX).path(nodeManager.getCurrentNode().getNodeIdentifier()).build().toString();
        eventBus.register(this);
        this.scheduledExecutorService = requireNonNull(scheduledExecutorService);
    }

    @SuppressWarnings("unused")
    @Subscribe
    public void connectorSyncInitialized(ConnectorSyncInitializedEvent event)
    {
        if (event.catalogSequence() == DEFAULT_CATALOG && callHomeConfiguration.isEnable() && cloudVendorConfiguration.getStoreType() != StoreType.LOCAL) {
            logger.debug("scheduling call-home every %s seconds", callHomeConfiguration.getIntervalInSeconds());
            uploadNodeInfo();
            scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(
                    new CallHomeJob(
                            cloudVendorService,
                            currentNodeAddress,
                            nodeStorePathPrefix,
                            getServerLogPath(),
                            getCatalogPath(),
                            false),
                    callHomeConfiguration.getIntervalInSeconds(),
                    callHomeConfiguration.getIntervalInSeconds(),
                    TimeUnit.SECONDS);
        }
        else {
            logger.debug("call-home is disabled for this connector");
        }
    }

    private void uploadNodeInfo()
    {
        try {
            Instant uptime = Instant.ofEpochMilli(ManagementFactory.getRuntimeMXBean().getStartTime());
            StringJoiner nodeInfo = new StringJoiner("\n");
            nodeInfo.add("host-ip=" + nodeManager.getCurrentNode().getHost());
            nodeInfo.add("version=" + Version.getInstance().getVersion());
            nodeInfo.add("is-coordinator=" + nodeManager.getCurrentNode().isCoordinator());
            nodeInfo.add("node-id" + nodeManager.getCurrentNode().getNodeIdentifier());
            nodeInfo.add("uptime=" + uptime.toString());

            cloudVendorService.uploadToCloud(
                    nodeInfo.toString().getBytes(Charset.defaultCharset()),
                    CloudVendorService.concatenatePath(nodeStorePathPrefix, uptime.toString(), "-node.info"));
        }
        catch (Exception e) {
            logger.warn(e, "failed to store node info file");
        }
    }

    public synchronized Optional<Integer> triggerCallHome(String storePath, boolean collectThreadDump, boolean waitToFinish)
    {
        if ((scheduledFuture == null) || (scheduledFuture.getDelay(TimeUnit.SECONDS) > 0)) {
            if (connectorSync.getCatalogSequence() != DEFAULT_CATALOG) {
                logger.debug("trigger non default catalog");
            }
            logger.debug("storePath = %s, nodeStorePathPrefix=%s", storePath, nodeStorePathPrefix);
            CallHomeJob callHomeJob = new CallHomeJob(
                    cloudVendorService,
                    currentNodeAddress,
                    (storePath != null) ? storePath : nodeStorePathPrefix,
                    getServerLogPath(),
                    getCatalogPath(),
                    collectThreadDump);
            if (waitToFinish) {
                callHomeJob.run();
                return Optional.of(callHomeJob.getNumberOfUploaded());
            }
            else {
                scheduledFuture = scheduledExecutorService.schedule(callHomeJob, 0, TimeUnit.SECONDS);
            }
        }
        return Optional.empty();
    }

    private String getCatalogPath()
    {
        return System.getProperty(CATALOG_PROPERTY_KEY, DEFAULT_CATALOG_PATH);
    }

    private static String getServerLogPath()
    {
        return System.getProperty(LOG_PATH_PROPERTY_KEY1,
                System.getProperty(LOG_PATH_PROPERTY_KEY2, DEFAULT_SERVER_LOG_PATH));
    }
}
