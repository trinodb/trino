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
package io.prestosql.plugin.hive.rubix;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.qubole.rubix.bookkeeper.BookKeeper;
import com.qubole.rubix.bookkeeper.BookKeeperServer;
import com.qubole.rubix.bookkeeper.LocalDataTransferServer;
import com.qubole.rubix.core.CachingFileSystem;
import io.airlift.log.Logger;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.hive.HdfsConfigurationInitializer;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.prestosql.plugin.hive.util.ConfigurationUtils.getInitialConfiguration;
import static io.prestosql.spi.StandardErrorCode.EXCEEDED_TIME_LIMIT;

/*
 * Responsibilities of this initializer:
 * 1. Lazily setup RubixConfigurationInitializer with information about master when it is available
 * 2. Start Rubix Servers.
 * 3. Inject BookKeeper object into CachingFileSystem class
 */
public class RubixInitializer
{
    private static final RetryPolicy<?> RETRY_POLICY =
            new RetryPolicy<>()
                    // unlimited attempts
                    .withMaxAttempts(-1)
                    .withDelay(Duration.ofMillis(100))
                    .withMaxDuration(Duration.ofDays(1));

    private static final Logger log = Logger.get(RubixInitializer.class);

    private final CatalogName catalogName;
    private final RubixConfigurationInitializer rubixConfigurationInitializer;
    private final HdfsConfigurationInitializer hdfsConfigurationInitializer;

    @Inject
    public RubixInitializer(
            CatalogName catalogName,
            RubixConfigurationInitializer rubixConfigurationInitializer,
            HdfsConfigurationInitializer hdfsConfigurationInitializer)
    {
        this.catalogName = catalogName;
        this.rubixConfigurationInitializer = rubixConfigurationInitializer;
        this.hdfsConfigurationInitializer = hdfsConfigurationInitializer;
    }

    public void initializeRubix(NodeManager nodeManager)
    {
        ExecutorService initializerService = Executors.newSingleThreadExecutor();
        ListenableFuture<?> nodeJoinFuture = MoreExecutors.listeningDecorator(initializerService).submit(() ->
                Failsafe.with(RETRY_POLICY).run(() -> {
                    if (!nodeManager.getAllNodes().contains(nodeManager.getCurrentNode()) ||
                            !nodeManager.getAllNodes().stream().anyMatch(Node::isCoordinator)) {
                        // Failsafe will propagate this exception only when timeout reached.
                        throw new PrestoException(EXCEEDED_TIME_LIMIT, "Exceeded timeout while waiting for coordinator node");
                    }
                }));

        addSuccessCallback(
                nodeJoinFuture,
                () -> startRubix(nodeManager));
    }

    private void startRubix(NodeManager nodeManager)
    {
        Node master = nodeManager.getAllNodes().stream().filter(node -> node.isCoordinator()).findFirst().get();
        boolean isMaster = nodeManager.getCurrentNode().isCoordinator();

        rubixConfigurationInitializer.setMaster(isMaster);
        rubixConfigurationInitializer.setMasterAddress(master.getHostAndPort());
        rubixConfigurationInitializer.setCurrentNodeAddress(nodeManager.getCurrentNode().getHost());

        Configuration configuration = getInitialConfiguration();
        // Perform standard HDFS configuration initialization.
        hdfsConfigurationInitializer.initializeConfiguration(configuration);
        // Apply RubixConfigurationInitializer directly suppressing cacheReady check
        rubixConfigurationInitializer.updateConfiguration(configuration);

        MetricRegistry metricRegistry = new MetricRegistry();
        BookKeeperServer bookKeeperServer = new BookKeeperServer();
        BookKeeper bookKeeper = bookKeeperServer.startServer(configuration, metricRegistry);
        LocalDataTransferServer.startServer(configuration, metricRegistry, bookKeeper);

        CachingFileSystem.setLocalBookKeeper(bookKeeper, "catalog=" + catalogName);
        log.info("Rubix initialized successfully");
        rubixConfigurationInitializer.initializationDone();
    }
}
