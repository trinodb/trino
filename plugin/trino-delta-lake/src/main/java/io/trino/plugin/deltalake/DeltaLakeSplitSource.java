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
package io.trino.plugin.deltalake;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.deltalake.util.DeltaLakeDomains.partitionMatchesPredicate;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DeltaLakeSplitSource
        implements ConnectorSplitSource
{
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);
    private static final Logger LOG = Logger.get(DeltaLakeSplitSource.class);

    private final SchemaTableName tableName;
    private final AsyncQueue<ConnectorSplit> queue;
    private final boolean recordScannedFiles;
    private final ImmutableSet.Builder<DeltaLakeScannedDataFile> scannedFilePaths = ImmutableSet.builder();
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;
    private final Closer closer = Closer.create();

    private volatile TrinoException trinoException;

    public DeltaLakeSplitSource(
            SchemaTableName tableName,
            Stream<DeltaLakeSplit> splits,
            ExecutorService executor,
            int maxSplitsPerSecond,
            int maxOutstandingSplits,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            boolean recordScannedFiles)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.queue = new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executor);
        closer.register(queue::finish);
        this.recordScannedFiles = recordScannedFiles;
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeout.toMillis();
        this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
        closer.register(splits::close);
        queueSplits(splits, queue, executor)
                .exceptionally(throwable -> {
                    // set trinoException before finishing the queue to ensure failure is observed instead of successful completion
                    // (the field is declared as volatile to make sure that the change is visible right away to other threads)
                    trinoException = new TrinoException(findErrorCode(throwable), "Failed to generate splits for " + this.tableName, throwable);
                    try {
                        // Finish the queue to wake up threads from queue.getBatchAsync()
                        queue.finish();
                    }
                    catch (Exception e) {
                        // if we can't finish the queue, consumers that might be waiting for more elements will remain blocked indefinitely
                        LOG.error(e, "Could not communicate split generation error for %s to query; this may cause it to be blocked", tableName);
                    }
                    return null;
                });
    }

    private ErrorCodeSupplier findErrorCode(Throwable throwable)
    {
        return getCausalChain(throwable).stream()
                .filter(TrinoException.class::isInstance)
                .map(TrinoException.class::cast)
                .findFirst()
                .map(t -> (ErrorCodeSupplier) t::getErrorCode)
                .orElse(GENERIC_INTERNAL_ERROR);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
        if (dynamicFilter.isAwaitable() && timeLeft > 0) {
            return dynamicFilter.isBlocked()
                    .thenApply(_ -> EMPTY_BATCH)
                    .completeOnTimeout(EMPTY_BATCH, timeLeft, MILLISECONDS);
        }

        boolean noMoreSplits = isFinished();
        if (trinoException != null) {
            return toCompletableFuture(immediateFailedFuture(trinoException));
        }

        return toCompletableFuture(Futures.transform(
                queue.getBatchAsync(maxSize),
                splits -> {
                    TupleDomain<DeltaLakeColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate().transformKeys(DeltaLakeColumnHandle.class::cast);
                    if (dynamicFilterPredicate.isNone()) {
                        return new ConnectorSplitBatch(ImmutableList.of(), noMoreSplits);
                    }
                    Map<DeltaLakeColumnHandle, Domain> partitionColumnDomains = dynamicFilterPredicate.getDomains().orElseThrow().entrySet().stream()
                            .filter(entry -> entry.getKey().columnType() == DeltaLakeColumnType.PARTITION_KEY)
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                    List<ConnectorSplit> filteredSplits = splits.stream()
                            .map(DeltaLakeSplit.class::cast)
                            .filter(split -> partitionMatchesPredicate(split.getPartitionKeys(), partitionColumnDomains) &&
                                    split.getStatisticsPredicate().overlaps(dynamicFilterPredicate))
                            .collect(toImmutableList());
                    if (recordScannedFiles) {
                        filteredSplits.forEach(split -> scannedFilePaths.add(new DeltaLakeScannedDataFile(((DeltaLakeSplit) split).getPath(), ((DeltaLakeSplit) split).getPartitionKeys())));
                    }
                    return new ConnectorSplitBatch(filteredSplits, noMoreSplits);
                },
                directExecutor()));
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        checkState(isFinished(), "Split source must be finished before TableExecuteSplitsInfo is read");
        if (!recordScannedFiles) {
            return Optional.empty();
        }
        return Optional.of(ImmutableList.copyOf(scannedFilePaths.build()));
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isFinished()
    {
        if (queue.isFinished()) {
            // Note: queue and trinoException need to be checked in the appropriate order
            // When throwable is set, we want getNextBatch to be called, so that we can propagate the exception
            return trinoException == null;
        }
        return false;
    }

    private static CompletableFuture<Void> queueSplits(Stream<DeltaLakeSplit> splits, AsyncQueue<ConnectorSplit> queue, ExecutorService executor)
    {
        requireNonNull(splits, "splits is null");
        return CompletableFuture.runAsync(
                () -> {
                    splits.map(queue::offer).forEachOrdered(MoreFutures::getFutureValue);
                    queue.finish();
                },
                executor);
    }
}
