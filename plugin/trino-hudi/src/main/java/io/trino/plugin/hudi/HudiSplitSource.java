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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.metastore.Partition;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.query.HudiSnapshotDirectoryLister;
import io.trino.plugin.hudi.split.HudiBackgroundSplitLoader;
import io.trino.plugin.hudi.split.HudiSplitWeightProvider;
import io.trino.plugin.hudi.split.SizeBasedSplitWeightProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.util.Lazy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.plugin.hudi.HudiSessionProperties.getStandardSplitWeightSize;
import static io.trino.plugin.hudi.HudiSessionProperties.isHudiMetadataTableEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isSizeBasedSplitWeightsEnabled;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HudiSplitSource.class);

    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);
    private final AsyncQueue<ConnectorSplit> queue;
    private final ScheduledFuture splitLoaderFuture;
    private final AtomicReference<TrinoException> trinoException = new AtomicReference<>();
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;

    public HudiSplitSource(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            ExecutorService executor,
            ScheduledExecutorService splitLoaderExecutorService,
            int maxSplitsPerSecond,
            int maxOutstandingSplits,
            Lazy<Map<String, Partition>> lazyPartitions,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeoutMillis,
            CachingHostAddressProvider cachingHostAddressProvider)
    {
        boolean enableMetadataTable = isHudiMetadataTableEnabled(session);
        Lazy<HoodieTableMetadata> lazyTableMetadata = Lazy.lazily(() -> {
            HoodieTimer timer = HoodieTimer.start();
            HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                    .enable(enableMetadataTable)
                    .build();
            HoodieTableMetaClient metaClient = tableHandle.getMetaClient();
            HoodieEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorage().getConf());

            HoodieTableMetadata tableMetadata = HoodieTableMetadata.create(
                    engineContext,
                    tableHandle.getMetaClient().getStorage(), metadataConfig, metaClient.getBasePath().toString(), true);
            log.info("Loaded table metadata for table: %s in %s ms", tableHandle.getSchemaTableName(), timer.endTimer());
            return tableMetadata;
        });

        HudiDirectoryLister hudiDirectoryLister = new HudiSnapshotDirectoryLister(
                session,
                tableHandle,
                enableMetadataTable,
                lazyTableMetadata);

        this.queue = new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executor);
        HudiBackgroundSplitLoader splitLoader = new HudiBackgroundSplitLoader(
                session,
                tableHandle,
                hudiDirectoryLister,
                queue,
                executor,
                createSplitWeightProvider(session),
                lazyPartitions,
                enableMetadataTable,
                lazyTableMetadata,
                cachingHostAddressProvider,
                throwable -> {
                    trinoException.compareAndSet(null, new TrinoException(HUDI_CANNOT_OPEN_SPLIT,
                            "Failed to generate splits for " + tableHandle.getSchemaTableName(), throwable));
                    queue.finish();
                });
        this.splitLoaderFuture = splitLoaderExecutorService.schedule(splitLoader, 0, TimeUnit.MILLISECONDS);
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeoutMillis.toMillis();
        this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        // If dynamic filtering is enabled and we haven't timed out, wait for the build side to provide the dynamic filter.
        long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
        if (dynamicFilter.isAwaitable() && timeLeft > 0) {
            // If the filter is not ready, return an empty batch. The query engine will call getNextBatch() again.
            // As long as isFinished() is false, effectively polling until the filter is ready or timeout occurs.
            return dynamicFilter.isBlocked()
                    .thenApply(_ -> EMPTY_BATCH)
                    .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
        }

        TupleDomain<HiveColumnHandle> dynamicFilterPredicate =
                dynamicFilter.getCurrentPredicate().transformKeys(HiveColumnHandle.class::cast);

        if (dynamicFilterPredicate.isNone()) {
            close();
            return completedFuture(new ConnectorSplitBatch(ImmutableList.of(), true));
        }

        boolean noMoreSplits = isFinished();
        Throwable throwable = trinoException.get();
        if (throwable != null) {
            return CompletableFuture.failedFuture(throwable);
        }

        return toCompletableFuture(Futures.transform(
                queue.getBatchAsync(maxSize),
                splits ->
                {
                    List<ConnectorSplit> filteredSplits = splits.stream()
                            .filter(split -> partitionMatchesPredicate((HudiSplit) split, dynamicFilterPredicate))
                            .collect(toImmutableList());
                    return new ConnectorSplitBatch(filteredSplits, noMoreSplits);
                },
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

    public static HudiSplitWeightProvider createSplitWeightProvider(ConnectorSession session)
    {
        if (isSizeBasedSplitWeightsEnabled(session)) {
            DataSize standardSplitWeightSize = getStandardSplitWeightSize(session);
            double minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
            return new SizeBasedSplitWeightProvider(minimumAssignedSplitWeight, standardSplitWeightSize);
        }
        return HudiSplitWeightProvider.uniformStandardWeightProvider();
    }

    static boolean partitionMatchesPredicate(
            HudiSplit split,
            TupleDomain<HiveColumnHandle> dynamicFilterPredicate)
    {
        if (dynamicFilterPredicate.isNone()) {
            return false;
        }

        // Pre-process the filter predicate to get a map of relevant partition domains keyed by partition column name
        Map<String, Map.Entry<HiveColumnHandle, Domain>> filterPartitionDomains = new HashMap<>();
        if (dynamicFilterPredicate.getDomains().isPresent()) {
            for (Map.Entry<HiveColumnHandle, Domain> entry : dynamicFilterPredicate.getDomains().get().entrySet()) {
                HiveColumnHandle column = entry.getKey();
                if (column.isPartitionKey()) {
                    filterPartitionDomains.put(column.getName(), entry);
                }
            }
        }

        // Match each partition key from the split against the pre-processed filter domains
        for (HivePartitionKey splitPartitionKey : split.getPartitionKeys()) {
            Map.Entry<HiveColumnHandle, Domain> filterInfo = filterPartitionDomains.get(splitPartitionKey.name());

            if (filterInfo == null) {
                // filterInfo is null, the partition key is not constrained by the filter
                continue;
            }

            HiveColumnHandle filterColumnHandle = filterInfo.getKey();
            Domain filterDomain = filterInfo.getValue();

            NullableValue value = HiveUtil.getPrefilledColumnValue(
                    filterColumnHandle,
                    splitPartitionKey,
                    null, OptionalInt.empty(), 0, 0, "");

            // Split does not match this filter condition
            if (!filterDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }
        return true;
    }
}
