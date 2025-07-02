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
package io.trino.plugin.hudi.split;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.metastore.Partition;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HudiPartitionInfoLoader;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.query.index.HudiPartitionStatsIndexSupport;
import io.trino.plugin.hudi.query.index.IndexSupportFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.util.Lazy;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiSessionProperties.getSplitGeneratorParallelism;
import static io.trino.plugin.hudi.HudiSessionProperties.getTargetSplitSize;
import static java.util.Objects.requireNonNull;

public class HudiBackgroundSplitLoader
        implements Runnable
{
    private static final Logger log = Logger.get(HudiBackgroundSplitLoader.class);
    private final HudiTableHandle tableHandle;
    private final HudiDirectoryLister hudiDirectoryLister;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Executor splitGeneratorExecutor;
    private final int splitGeneratorNumThreads;
    private final HudiSplitFactory hudiSplitFactory;
    private final Lazy<List<String>> lazyPartitions;
    private final Consumer<Throwable> errorListener;
    private final boolean enableMetadataTable;
    private final Optional<HudiPartitionStatsIndexSupport> partitionIndexSupportOpt;

    public HudiBackgroundSplitLoader(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            HudiDirectoryLister hudiDirectoryLister,
            AsyncQueue<ConnectorSplit> asyncQueue,
            Executor splitGeneratorExecutor,
            HudiSplitWeightProvider hudiSplitWeightProvider,
            Lazy<Map<String, Partition>> lazyPartitionMap,
            boolean enableMetadataTable,
            Lazy<HoodieTableMetadata> lazyTableMetadata,
            CachingHostAddressProvider cachingHostAddressProvider,
            Consumer<Throwable> errorListener)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.hudiDirectoryLister = requireNonNull(hudiDirectoryLister, "hudiDirectoryLister is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.splitGeneratorExecutor = requireNonNull(splitGeneratorExecutor, "splitGeneratorExecutorService is null");
        this.splitGeneratorNumThreads = getSplitGeneratorParallelism(session);
        this.hudiSplitFactory = new HudiSplitFactory(tableHandle, hudiSplitWeightProvider, getTargetSplitSize(session), cachingHostAddressProvider);
        this.lazyPartitions = Lazy.lazily(() -> requireNonNull(lazyPartitionMap, "partitions is null").get().keySet().stream().toList());
        this.enableMetadataTable = enableMetadataTable;
        this.errorListener = requireNonNull(errorListener, "errorListener is null");
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        this.partitionIndexSupportOpt = enableMetadataTable ?
                IndexSupportFactory.createPartitionStatsIndexSupport(schemaTableName, Lazy.lazily(tableHandle::getMetaClient), lazyTableMetadata, tableHandle.getRegularPredicates(), session) : Optional.empty();
    }

    @Override
    public void run()
    {
        // Wrap entire logic so that ANY error will be thrown out and not cause program to get stuc
        try {
            if (enableMetadataTable) {
                generateSplits(true);
                return;
            }

            // Fallback to partition pruning generator
            generateSplits(false);
        }
        catch (Exception e) {
            errorListener.accept(e);
        }
    }

    private void generateSplits(boolean useIndex)
    {
        // Attempt to apply partition pruning using partition stats index
        Optional<List<String>> effectivePartitionsOpt = useIndex && partitionIndexSupportOpt.isPresent() ?
                partitionIndexSupportOpt.get().prunePartitions(lazyPartitions.get()) : Optional.empty();

        Deque<String> partitionQueue = new ConcurrentLinkedDeque<>(effectivePartitionsOpt.orElse(lazyPartitions.get()));
        List<HudiPartitionInfoLoader> splitGenerators = new ArrayList<>();
        List<ListenableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < splitGeneratorNumThreads; i++) {
            HudiPartitionInfoLoader generator = new HudiPartitionInfoLoader(hudiDirectoryLister, tableHandle.getLatestCommitTime(), hudiSplitFactory,
                    asyncQueue, partitionQueue, useIndex);
            splitGenerators.add(generator);
            ListenableFuture<Void> future = Futures.submit(generator, splitGeneratorExecutor);
            addExceptionCallback(future, errorListener);
            futures.add(future);
        }

        // Signal all generators to stop once partition queue is drained
        splitGenerators.forEach(HudiPartitionInfoLoader::stopRunning);

        log.info("Wait for partition pruning split generation to finish on table %s.%s", tableHandle.getSchemaName(), tableHandle.getTableName());
        try {
            Futures.whenAllComplete(futures)
                    .run(asyncQueue::finish, directExecutor())
                    .get();
            log.info("Partition pruning split generation finished on table %s.%s", tableHandle.getSchemaName(), tableHandle.getTableName());
        }
        catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Error generating Hudi split", e);
        }
    }
}
