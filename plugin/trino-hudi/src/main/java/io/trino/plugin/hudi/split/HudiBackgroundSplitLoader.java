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
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.trino.metastore.Partition;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfoLoader;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.query.index.HudiIndexSupport;
import io.trino.plugin.hudi.query.index.HudiPartitionStatsIndexSupport;
import io.trino.plugin.hudi.query.index.IndexSupportFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.util.Lazy;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
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
import static io.trino.plugin.hudi.partition.HiveHudiPartitionInfo.NON_PARTITION;
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
    private final Lazy<HoodieTableMetaClient> lazyMetaClient;
    private final TupleDomain<HiveColumnHandle> regularPredicates;
    private final Optional<HudiIndexSupport> indexSupportOpt;
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
            Consumer<Throwable> errorListener)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.hudiDirectoryLister = requireNonNull(hudiDirectoryLister, "hudiDirectoryLister is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.splitGeneratorExecutor = requireNonNull(splitGeneratorExecutor, "splitGeneratorExecutorService is null");
        this.splitGeneratorNumThreads = getSplitGeneratorParallelism(session);
        this.hudiSplitFactory = new HudiSplitFactory(tableHandle, hudiSplitWeightProvider);
        this.lazyPartitions = Lazy.lazily(() -> requireNonNull(lazyPartitionMap, "partitions is null").get().keySet().stream().toList());
        this.enableMetadataTable = enableMetadataTable;
        this.lazyMetaClient = Lazy.lazily(tableHandle::getMetaClient);
        this.regularPredicates = tableHandle.getRegularPredicates();
        this.errorListener = requireNonNull(errorListener, "errorListener is null");
        this.indexSupportOpt = IndexSupportFactory.createIndexSupport(lazyMetaClient, regularPredicates, session);
        this.partitionIndexSupportOpt = IndexSupportFactory.createPartitionStatsIndexSupport(lazyMetaClient, regularPredicates, session);
    }

    @Override
    public void run()
    {
        if (enableMetadataTable) {
            // Wrap entire logic so that ANY error will be thrown out and not cause program to get stuck
            try {
                if (indexSupportOpt.isPresent()) {
                    indexEnabledSplitGenerator(indexSupportOpt.get());
                    return;
                }
            }
            catch (Exception e) {
                errorListener.accept(e);
            }
        }

        // Fallback to partition pruning generator
        partitionPruningSplitGenerator();
    }

    private void indexEnabledSplitGenerator(HudiIndexSupport hudiIndexSupport)
    {
        log.info("Start split generation with index support on table %s.%s", tableHandle.getSchemaName(), tableHandle.getTableName());
        // Data Skipping based on column stats
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(lazyMetaClient.get().getStorage().getConf());
        HoodieTableMetadata metadataTable = HoodieTableMetadata.create(
                engineContext,
                lazyMetaClient.get().getStorage(), metadataConfig, lazyMetaClient.get().getBasePath().toString(), true);

        // Attempt to apply partition pruning using partition stats index
        Optional<List<String>> effectivePartitionsOpt = partitionIndexSupportOpt.isPresent() ? partitionIndexSupportOpt.get().prunePartitions(
                lazyPartitions.get(), metadataTable, regularPredicates.transformKeys(HiveColumnHandle::getName)) : Optional.empty();

        // For MDT the file listing is already loaded in memory
        // TODO(yihua): refactor split loader/directory lister API for maintainability
        Map<String, List<FileSlice>> partitionFileSliceMap = new HashMap<>();
        Map<String, List<HivePartitionKey>> partitionToPartitionKeyMap = new HashMap<>();
        // non-partitioned tables have empty strings
        for (String partitionName : effectivePartitionsOpt.orElse(lazyPartitions.get())) {
            Optional<HudiPartitionInfo> partitionInfo = hudiDirectoryLister.getPartitionInfo(partitionName);
            partitionInfo.ifPresent(hudiPartitionInfo -> {
                if (hudiPartitionInfo.doesMatchPredicates() || partitionName.equals(NON_PARTITION)) {
                    List<HivePartitionKey> partitionKeys = hudiPartitionInfo.getHivePartitionKeys();
                    List<FileSlice> partitionFileSlices = hudiDirectoryLister.listStatus(hudiPartitionInfo);
                    partitionFileSliceMap.put(partitionName, partitionFileSlices);
                    partitionToPartitionKeyMap.put(partitionName, partitionKeys);
                }
            });
        }
        TupleDomain<String> regularPredicatesTransformed = regularPredicates.transformKeys(HiveColumnHandle::getName);
        Map<String, List<FileSlice>> prunedFiles = hudiIndexSupport.lookupCandidateFilesInMetadataTable(
                metadataTable, partitionFileSliceMap, regularPredicatesTransformed);
        prunedFiles.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().flatMap(slice ->
                        hudiSplitFactory.createSplits(
                                partitionToPartitionKeyMap.get(entry.getKey()), slice, tableHandle.getLatestCommitTime()).stream()))
                .map(asyncQueue::offer)
                .forEachOrdered(MoreFutures::getFutureValue);
        asyncQueue.finish();
        log.info("Split generation with index support finished on table %s.%s", tableHandle.getSchemaName(), tableHandle.getTableName());
    }

    private void partitionPruningSplitGenerator()
    {
        log.info("Start partition pruning split generation on table %s.%s", tableHandle.getSchemaName(), tableHandle.getTableName());
        Deque<String> partitionQueue = new ConcurrentLinkedDeque<>(lazyPartitions.get());
        List<HudiPartitionInfoLoader> splitGeneratorList = new ArrayList<>();
        List<ListenableFuture<Void>> splitGeneratorFutures = new ArrayList<>();

        // Start a number of partition split generators to generate the splits in parallel
        for (int i = 0; i < splitGeneratorNumThreads; i++) {
            HudiPartitionInfoLoader generator = new HudiPartitionInfoLoader(hudiDirectoryLister, tableHandle.getLatestCommitTime(), hudiSplitFactory, asyncQueue, partitionQueue);
            splitGeneratorList.add(generator);
            ListenableFuture<Void> future = Futures.submit(generator, splitGeneratorExecutor);
            addExceptionCallback(future, errorListener);
            splitGeneratorFutures.add(future);
        }

        for (HudiPartitionInfoLoader generator : splitGeneratorList) {
            // Let the split generator stop once the partition queue is empty
            generator.stopRunning();
        }

        log.info("Wait for partition pruning split generation to finish on table %s.%s", tableHandle.getSchemaName(), tableHandle.getTableName());
        try {
            // Wait for all split generators to finish
            Futures.whenAllComplete(splitGeneratorFutures)
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
