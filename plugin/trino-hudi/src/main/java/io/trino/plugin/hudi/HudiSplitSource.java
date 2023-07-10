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
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystemFactory;
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
import io.trino.plugin.hudi.table.HudiTableMetaClient;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.hudi.HudiSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.plugin.hudi.HudiSessionProperties.getSplitGeneratorParallelism;
import static io.trino.plugin.hudi.HudiSessionProperties.getStandardSplitWeightSize;
import static io.trino.plugin.hudi.HudiSessionProperties.isSizeBasedSplitWeightsEnabled;
import static io.trino.plugin.hudi.HudiUtil.buildTableMetaClient;
import static java.util.stream.Collectors.toList;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private final AsyncQueue<ConnectorSplit> queue;
    private final ScheduledFuture splitLoaderFuture;
    private final AtomicReference<TrinoException> trinoException = new AtomicReference<>();

    public HudiSplitSource(
            ConnectorSession session,
            HiveMetastore metastore,
            Table table,
            HudiTableHandle tableHandle,
            TrinoFileSystemFactory fileSystemFactory,
            Map<String, HiveColumnHandle> partitionColumnHandleMap,
            ExecutorService executor,
            ScheduledExecutorService splitLoaderExecutorService,
            int maxSplitsPerSecond,
            int maxOutstandingSplits,
            List<String> partitions)
    {
        HudiTableMetaClient metaClient = buildTableMetaClient(fileSystemFactory.create(session), tableHandle.getBasePath());
        List<HiveColumnHandle> partitionColumnHandles = table.getPartitionColumns().stream()
                .map(column -> partitionColumnHandleMap.get(column.getName())).collect(toList());

        HudiDirectoryLister hudiDirectoryLister = new HudiReadOptimizedDirectoryLister(
                tableHandle,
                metaClient,
                metastore,
                table,
                partitionColumnHandles,
                partitions);

        this.queue = new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executor);
        HudiBackgroundSplitLoader splitLoader = new HudiBackgroundSplitLoader(
                session,
                tableHandle,
                hudiDirectoryLister,
                queue,
                new BoundedExecutor(executor, getSplitGeneratorParallelism(session)),
                createSplitWeightProvider(session),
                partitions);
        this.splitLoaderFuture = splitLoaderExecutorService.schedule(splitLoader, 0, TimeUnit.MILLISECONDS);
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
        return splitLoaderFuture.isDone() && queue.isFinished();
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
