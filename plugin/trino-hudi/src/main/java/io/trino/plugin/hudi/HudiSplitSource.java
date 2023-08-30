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
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.query.HudiReadOptimizedDirectoryLister;
import io.trino.plugin.hudi.split.HudiBackgroundSplitLoader;
import io.trino.plugin.hudi.split.HudiSplitWeightProvider;
import io.trino.plugin.hudi.split.SizeBasedSplitWeightProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.List;
import java.util.Map;
import java.util.Deque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.hudi.HudiSessionProperties.*;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.stream.Collectors.toList;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private final AsyncQueue<ConnectorSplit> queue;
    private final AtomicReference<TrinoException> trinoException = new AtomicReference<>();
    private List<String> hivePartitionNames;
    private final int minPartitionBatchSize;
    private final int maxPartitionBatchSize;
    private int currentBatchSize = -1;

    public HudiSplitSource(
            ConnectorSession session,
            HiveMetastore metastore,
            Table table,
            HudiTableHandle tableHandle,
            Configuration configuration,
            Map<String, HiveColumnHandle> partitionColumnHandleMap,
            ExecutorService executor,
            int maxSplitsPerSecond,
            int maxOutstandingSplits,
            HdfsEnvironment hdfsEnvironment)
    {
        boolean metadataEnabled = isHudiMetadataEnabled(session);
        HoodieTableMetaClient metaClient =
                hdfsEnvironment.doAs(session.getIdentity(), () -> buildTableMetaClient(configuration, tableHandle.getBasePath()));
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(configuration);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(metadataEnabled)
                .build();
        List<Column> partitionColumns = table.getPartitionColumns();
        List<HiveColumnHandle> partitionColumnHandles = partitionColumns.stream()
                .map(column -> partitionColumnHandleMap.get(column.getName())).collect(toList());
        this.minPartitionBatchSize = getMinPartitionBatchSize(session);
        this.maxPartitionBatchSize = getMaxPartitionBatchSize(session);
        HudiDirectoryLister hudiDirectoryLister = new HudiReadOptimizedDirectoryLister(
                metadataConfig,
                engineContext,
                tableHandle,
                metaClient,
                metastore,
                table,
                partitionColumnHandles,
                partitionColumns);

        this.queue = new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executor);
        Deque<List<String>> partitionNamesQueue = getPartitionNamesQueue(metastore, tableHandle, partitionColumns, partitionColumnHandles);
        Deque<HudiPartitionInfo> partitionInfoQueue = new ConcurrentLinkedDeque<>();
        HudiBackgroundSplitLoader splitLoader = new HudiBackgroundSplitLoader(
                session,
                tableHandle,
                hudiDirectoryLister,
                queue,
                new BoundedExecutor(executor, getSplitLoaderParallelism(session)),
                createSplitWeightProvider(session),
                partitionNamesQueue,
                partitionInfoQueue,
                new BoundedExecutor(executor, getPartitionInfoLoaderParallelism(session)),
                throwable -> {
                    trinoException.compareAndSet(null, new TrinoException(GENERIC_INTERNAL_ERROR,
                            "Failed to generator partitions info for " + table.getTableName(), throwable));
                    queue.finish();
                },
                throwable -> {
                    trinoException.compareAndSet(null, new TrinoException(GENERIC_INTERNAL_ERROR,
                            "Failed to generate splits for " + table.getTableName(), throwable));
                    queue.finish();
                });
        splitLoader.start();
    }

    private Deque<List<String>> getPartitionNamesQueue(HiveMetastore hiveMetastore, HudiTableHandle tableHandle, List<Column> partitionColumns, List<HiveColumnHandle> partitionColumnHandles)
    {
        TupleDomain<String> partitionKeysFilter = MetastoreUtil.computePartitionKeyFilter(partitionColumnHandles, tableHandle.getPartitionPredicates());
        if (hivePartitionNames == null) {
            hivePartitionNames = partitionColumns.isEmpty()
                    ? Collections.singletonList("")
                    : getPartitionNamesFromHiveMetastore(hiveMetastore, partitionColumns, tableHandle, partitionKeysFilter);
        }
        Deque<List<String>> partitionNamesQueue = new ConcurrentLinkedDeque<>();
        Iterator<String> iterator = hivePartitionNames.iterator();
        while(iterator.hasNext()) {
            List<String> paritionNamesBatch = new ArrayList<>();
            int batchSize = updateBatchSize();
            while (iterator.hasNext() && batchSize > 0) {
                paritionNamesBatch.add(iterator.next());
                batchSize--;
            }
            partitionNamesQueue.offer(paritionNamesBatch);
        }
        return partitionNamesQueue;
    }

    private List<String> getPartitionNamesFromHiveMetastore(HiveMetastore hiveMetastore, List<Column> partitionColumns, HudiTableHandle tableHandle, TupleDomain<String> partitionKeysFilter)
    {
        SchemaTableName tableName = tableHandle.getSchemaTableName();
        return hiveMetastore.getPartitionNamesByFilter(
                tableName.getSchemaName(),
                tableName.getTableName(),
                partitionColumns.stream().map(Column::getName).collect(Collectors.toList()),
                partitionKeysFilter).orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
    }

    private int updateBatchSize()
    {
        if (currentBatchSize <= 0) {
            currentBatchSize = minPartitionBatchSize;
        } else {
            currentBatchSize = Math.min(currentBatchSize * 2, maxPartitionBatchSize);
        }
        return currentBatchSize;
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

    private static HoodieTableMetaClient buildTableMetaClient(Configuration configuration, String basePath)
    {
        HoodieTableMetaClient client = HoodieTableMetaClient.builder().setConf(configuration).setBasePath(basePath).build();
        client.getTableConfig().setValue("hoodie.bootstrap.index.enable", "false");
        return client;
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
