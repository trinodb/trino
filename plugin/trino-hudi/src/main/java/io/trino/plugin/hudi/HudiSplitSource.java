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

import com.google.common.util.concurrent.Futures;
import io.airlift.units.DataSize;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.query.HudiReadOptimizedDirectoryLister;
import io.trino.plugin.hudi.split.HudiBackgroundSplitLoader;
import io.trino.plugin.hudi.split.HudiSplitWeightProvider;
import io.trino.plugin.hudi.split.SizeBasedSplitWeightProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.hudi.HudiSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.plugin.hudi.HudiSessionProperties.getStandardSplitWeightSize;
import static io.trino.plugin.hudi.HudiSessionProperties.isHudiMetadataEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isSizeBasedSplitWeightsEnabled;
import static io.trino.plugin.hudi.HudiUtil.buildTableMetaClient;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.stream.Collectors.toList;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private final AsyncQueue<ConnectorSplit> queue;
    private final AtomicReference<TrinoException> trinoException = new AtomicReference<>();

    public HudiSplitSource(
            ConnectorSession session,
            HiveMetastore metastore,
            Table table,
            HudiTableHandle tableHandle,
            Configuration configuration,
            Map<String, HiveColumnHandle> partitionColumnHandleMap,
            ExecutorService executor,
            int maxSplitsPerSecond,
            int maxOutstandingSplits)
    {
        boolean metadataEnabled = isHudiMetadataEnabled(session);
        HoodieTableMetaClient metaClient = buildTableMetaClient(configuration, tableHandle.getBasePath());
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(configuration);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(metadataEnabled)
                .build();
        List<HiveColumnHandle> partitionColumnHandles = table.getPartitionColumns().stream()
                .map(column -> partitionColumnHandleMap.get(column.getName())).collect(toList());

        HudiDirectoryLister hudiDirectoryLister = new HudiReadOptimizedDirectoryLister(
                metadataConfig,
                engineContext,
                tableHandle,
                metaClient,
                metastore,
                table,
                partitionColumnHandles);

        this.queue = new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executor);
        HudiBackgroundSplitLoader splitLoader = new HudiBackgroundSplitLoader(
                session,
                tableHandle,
                hudiDirectoryLister,
                queue,
                executor,
                createSplitWeightProvider(session),
                throwable -> {
                    trinoException.compareAndSet(null, new TrinoException(GENERIC_INTERNAL_ERROR,
                            "Failed to generate splits for " + table.getTableName(), throwable));
                    queue.finish();
                });
        splitLoader.start();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        boolean noMoreSplits = isFinished();
        Throwable throwable = trinoException.get();
        if (throwable != null) {
            return CompletableFuture.failedFuture(throwable);
        }

        return toCompletableFuture(Futures.transform(
                queue.getBatchAsync(maxSize),
                splits -> new ConnectorSplitBatch(splits, noMoreSplits),
                directExecutor()));
    }

    @Override
    public void close()
    {
        queue.finish();
    }

    @Override
    public boolean isFinished()
    {
        return queue.isFinished();
    }

    private static HudiSplitWeightProvider createSplitWeightProvider(ConnectorSession session)
    {
        if (isSizeBasedSplitWeightsEnabled(session)) {
            DataSize standardSplitWeightSize = getStandardSplitWeightSize(session);
            double minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
            return new SizeBasedSplitWeightProvider(minimumAssignedSplitWeight, standardSplitWeightSize);
        }
        return HudiSplitWeightProvider.uniformStandardWeightProvider();
    }
}
