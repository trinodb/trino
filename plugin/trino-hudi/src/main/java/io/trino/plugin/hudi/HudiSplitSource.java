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

package io.trino.plugin.hudi;

import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.query.HudiFileListing;
import io.trino.plugin.hudi.query.HudiFileListingFactory;
import io.trino.plugin.hudi.query.HudiQueryMode;
import io.trino.plugin.hudi.split.HudiSplitBackgroundLoader;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.hudi.HudiSessionProperties.isHudiMetadataEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.shouldSkipMetaStoreForPartition;
import static io.trino.plugin.hudi.HudiUtil.getMetaClient;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.lang.Thread.sleep;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HudiSplitSource.class);
    private static final long IDLE_WAIT_TIME_MS = 10;
    private final HudiFileListing hudiFileListing;
    private final Deque<ConnectorSplit> connectorSplitQueue;
    private final ScheduledFuture splitLoaderFuture;

    public HudiSplitSource(
            ConnectorSession session,
            HiveMetastore metastore,
            Table table,
            HudiTableHandle tableHandle,
            Configuration conf,
            Map<String, HiveColumnHandle> partitionColumnHandleMap)
    {
        boolean metadataEnabled = isHudiMetadataEnabled(session);
        boolean shouldSkipMetastoreForPartition = shouldSkipMetaStoreForPartition(session);
        HoodieTableMetaClient metaClient = tableHandle.getMetaClient().orElseGet(() -> getMetaClient(conf, tableHandle.getBasePath()));
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(conf);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(metadataEnabled)
                .build();
        List<HiveColumnHandle> partitionColumnHandles = table.getPartitionColumns().stream()
                .map(column -> partitionColumnHandleMap.get(column.getName())).collect(toList());
        // TODO: fetch the query mode from config / query context
        this.hudiFileListing = HudiFileListingFactory.get(
                HudiQueryMode.READ_OPTIMIZED,
                metadataConfig,
                engineContext,
                tableHandle,
                metaClient,
                metastore,
                table,
                partitionColumnHandles,
                shouldSkipMetastoreForPartition);
        this.connectorSplitQueue = new ArrayDeque<>();
        HudiSplitBackgroundLoader splitLoader = new HudiSplitBackgroundLoader(
                session,
                tableHandle,
                metaClient,
                hudiFileListing,
                connectorSplitQueue);
        ScheduledExecutorService splitLoaderExecutorService = Executors.newSingleThreadScheduledExecutor();
        this.splitLoaderFuture = splitLoaderExecutorService.schedule(
                splitLoader, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        if (isFinished()) {
            return completedFuture(new ConnectorSplitBatch(new ArrayList<>(), true));
        }

        HoodieTimer timer = new HoodieTimer().startTimer();
        List<ConnectorSplit> connectorSplits = new ArrayList<>();

        while (!splitLoaderFuture.isDone() && connectorSplitQueue.isEmpty()) {
            try {
                sleep(IDLE_WAIT_TIME_MS);
            }
            catch (InterruptedException e) {
                currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        synchronized (connectorSplitQueue) {
            while (connectorSplits.size() < maxSize && !connectorSplitQueue.isEmpty()) {
                connectorSplits.add(connectorSplitQueue.pollFirst());
            }
        }

        log.debug(format("Get the next batch of %d splits in %d ms", connectorSplits.size(), timer.endTimer()));
        return completedFuture(new ConnectorSplitBatch(connectorSplits, isFinished()));
    }

    @Override
    public void close()
    {
        hudiFileListing.close();
    }

    @Override
    public boolean isFinished()
    {
        return splitLoaderFuture.isDone() && connectorSplitQueue.isEmpty();
    }
}
