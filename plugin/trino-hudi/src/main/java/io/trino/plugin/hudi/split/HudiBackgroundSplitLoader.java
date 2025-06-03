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
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiTableHandle;
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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiSessionProperties.getSplitGeneratorParallelism;
import static java.util.Objects.requireNonNull;

public class HudiBackgroundSplitLoader
        implements Runnable
{
    private static final Logger log = Logger.get(HudiBackgroundSplitLoader.class);

    private final HudiDirectoryLister hudiDirectoryLister;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final Executor splitGeneratorExecutor;
    private final int splitGeneratorNumThreads;
    private final HudiSplitFactory hudiSplitFactory;
    private final List<String> partitions;
    private final String commitTime;
    private final Consumer<Throwable> errorListener;
    private final boolean enableMetadataTable;
    private final HoodieTableMetaClient metaClient;
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
            List<String> partitions,
            String commitTime,
            boolean enableMetadataTable,
            HoodieTableMetaClient metaClient,
            Consumer<Throwable> errorListener)
    {
        this.hudiDirectoryLister = requireNonNull(hudiDirectoryLister, "hudiDirectoryLister is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.splitGeneratorExecutor = requireNonNull(splitGeneratorExecutor, "splitGeneratorExecutorService is null");
        this.splitGeneratorNumThreads = getSplitGeneratorParallelism(session);
        this.hudiSplitFactory = new HudiSplitFactory(tableHandle, hudiSplitWeightProvider);
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.commitTime = requireNonNull(commitTime, "commitTime is null");
        this.enableMetadataTable = enableMetadataTable;
        this.metaClient = requireNonNull(metaClient, "metaClient is null");
        this.regularPredicates = tableHandle.getRegularPredicates();
        this.errorListener = requireNonNull(errorListener, "errorListener is null");
        this.indexSupportOpt = IndexSupportFactory.createIndexSupport(metaClient, regularPredicates, session);
        this.partitionIndexSupportOpt = IndexSupportFactory.createPartitionStatsIndexSupport(metaClient, regularPredicates, session);
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
        // Data Skipping based on column stats
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorage().getConf());
        HoodieTableMetadata metadataTable = HoodieTableMetadata.create(
                engineContext,
                metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString(), true);

        // Attempt to apply partition pruning using partition stats index
        Optional<List<String>> effectivePartitionsOpt = partitionIndexSupportOpt.isPresent() ? partitionIndexSupportOpt.get().prunePartitions(
                partitions, metadataTable, regularPredicates.transformKeys(HiveColumnHandle::getName)) : Optional.empty();

        // For MDT the file listing is already loaded in memory
        // TODO(yihua): refactor split loader/directory lister API for maintainability
        // non-partitioned tables have empty strings

        TupleDomain<String> regularPredicatesTransformed = regularPredicates.transformKeys(HiveColumnHandle::getName);

        Instant start = Instant.now();
        log.info("Started generating splits");
        Deque<String> partitionQueue = new ConcurrentLinkedDeque<>(partitions);
        List<HudiPartitionInfoLoader> splitGeneratorList = new ArrayList<>();
        List<ListenableFuture<Void>> splitGeneratorFutures = new ArrayList<>();

        // Start a number of partition split generators to generate the splits in parallel
        for (int i = 0; i < splitGeneratorNumThreads; i++) {
            HudiPartitionInfoLoader generator = new HudiPartitionInfoLoader(hudiDirectoryLister, commitTime, hudiSplitFactory,
                    Optional.of(hudiIndexSupport), asyncQueue, partitionQueue);
            splitGeneratorList.add(generator);
            ListenableFuture<Void> future = Futures.submit(generator, splitGeneratorExecutor);
            addExceptionCallback(future, errorListener);
            splitGeneratorFutures.add(future);
        }

        for (HudiPartitionInfoLoader generator : splitGeneratorList) {
            // Let the split generator stop once the partition queue is empty
            generator.stopRunning();
        }

        try {
            // Wait for all split generators to finish
            Futures.whenAllComplete(splitGeneratorFutures)
                    .run(asyncQueue::finish, directExecutor())
                    .get();
        }
        catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Error generating Hudi split", e);
        }
        long timeTaken = Duration.between(start, Instant.now()).toSeconds();
        log.info("Completed generating splits, time taken %s", timeTaken);

        asyncQueue.finish();
    }

    private void partitionPruningSplitGenerator()
    {
        Deque<String> partitionQueue = new ConcurrentLinkedDeque<>(partitions);
        List<HudiPartitionInfoLoader> splitGeneratorList = new ArrayList<>();
        List<ListenableFuture<Void>> splitGeneratorFutures = new ArrayList<>();

        // Start a number of partition split generators to generate the splits in parallel
        for (int i = 0; i < splitGeneratorNumThreads; i++) {
            HudiPartitionInfoLoader generator = new HudiPartitionInfoLoader(hudiDirectoryLister, commitTime, hudiSplitFactory, asyncQueue, partitionQueue);
            splitGeneratorList.add(generator);
            ListenableFuture<Void> future = Futures.submit(generator, splitGeneratorExecutor);
            addExceptionCallback(future, errorListener);
            splitGeneratorFutures.add(future);
        }

        for (HudiPartitionInfoLoader generator : splitGeneratorList) {
            // Let the split generator stop once the partition queue is empty
            generator.stopRunning();
        }

        try {
            // Wait for all split generators to finish
            Futures.whenAllComplete(splitGeneratorFutures)
                    .run(asyncQueue::finish, directExecutor())
                    .get();
        }
        catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Error generating Hudi split", e);
        }
    }
}
