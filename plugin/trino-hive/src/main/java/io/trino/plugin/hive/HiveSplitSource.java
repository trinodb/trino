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
package io.trino.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.trino.filesystem.cache.CachingHostAddressProvider;
import io.trino.plugin.hive.InternalHiveSplit.InternalHiveBlock;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.AsyncQueue.BorrowResult;
import io.trino.plugin.hive.util.SizeBasedSplitWeightProvider;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.failedFuture;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static io.trino.plugin.hive.HiveSessionProperties.getMaxInitialSplitSize;
import static io.trino.plugin.hive.HiveSessionProperties.getMaxSplitSize;
import static io.trino.plugin.hive.HiveSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.plugin.hive.HiveSessionProperties.isSizeBasedSplitWeightsEnabled;
import static io.trino.plugin.hive.HiveSplitSource.StateKind.CLOSED;
import static io.trino.plugin.hive.HiveSplitSource.StateKind.FAILED;
import static io.trino.plugin.hive.HiveSplitSource.StateKind.INITIAL;
import static io.trino.plugin.hive.HiveSplitSource.StateKind.NO_MORE_SPLITS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class HiveSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HiveSplitSource.class);

    private final String queryId;
    private final String databaseName;
    private final String tableName;
    private final PerBucket queues;
    private final AtomicInteger bufferedInternalSplitCount = new AtomicInteger();
    private final long maxOutstandingSplitsBytes;

    private final DataSize maxSplitSize;
    private final DataSize maxInitialSplitSize;
    private final AtomicInteger remainingInitialSplits;

    private final HiveSplitLoader splitLoader;
    private final AtomicReference<State> stateReference;

    private final AtomicLong estimatedSplitSizeInBytes = new AtomicLong();

    private final CounterStat highMemorySplitSourceCounter;
    private final AtomicBoolean loggedHighMemoryWarning = new AtomicBoolean();
    private final HiveSplitWeightProvider splitWeightProvider;
    private final CachingHostAddressProvider cachingHostAddressProvider;

    private final boolean recordScannedFiles;
    private final ImmutableList.Builder<Object> scannedFilePaths = ImmutableList.builder();

    private HiveSplitSource(
            ConnectorSession session,
            String databaseName,
            String tableName,
            PerBucket queues,
            int maxInitialSplits,
            DataSize maxOutstandingSplitsSize,
            HiveSplitLoader splitLoader,
            AtomicReference<State> stateReference,
            CounterStat highMemorySplitSourceCounter,
            CachingHostAddressProvider cachingHostAddressProvider,
            boolean recordScannedFiles)
    {
        requireNonNull(session, "session is null");
        this.queryId = session.getQueryId();
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.queues = requireNonNull(queues, "queues is null");
        this.maxOutstandingSplitsBytes = maxOutstandingSplitsSize.toBytes();
        this.splitLoader = requireNonNull(splitLoader, "splitLoader is null");
        this.stateReference = requireNonNull(stateReference, "stateReference is null");
        this.highMemorySplitSourceCounter = requireNonNull(highMemorySplitSourceCounter, "highMemorySplitSourceCounter is null");

        this.maxSplitSize = getMaxSplitSize(session);
        this.maxInitialSplitSize = getMaxInitialSplitSize(session);
        this.remainingInitialSplits = new AtomicInteger(maxInitialSplits);
        this.splitWeightProvider = isSizeBasedSplitWeightsEnabled(session) ? new SizeBasedSplitWeightProvider(getMinimumAssignedSplitWeight(session), maxSplitSize) : HiveSplitWeightProvider.uniformStandardWeightProvider();
        this.cachingHostAddressProvider = requireNonNull(cachingHostAddressProvider, "cachingHostAddressProvider is null");
        this.recordScannedFiles = recordScannedFiles;
    }

    public static HiveSplitSource allAtOnce(
            ConnectorSession session,
            String databaseName,
            String tableName,
            int maxInitialSplits,
            int maxOutstandingSplits,
            DataSize maxOutstandingSplitsSize,
            int maxSplitsPerSecond,
            HiveSplitLoader splitLoader,
            Executor executor,
            CounterStat highMemorySplitSourceCounter,
            CachingHostAddressProvider cachingHostAddressProvider,
            boolean recordScannedFiles)
    {
        AtomicReference<State> stateReference = new AtomicReference<>(State.initial());
        return new HiveSplitSource(
                session,
                databaseName,
                tableName,
                new PerBucket()
                {
                    private final AsyncQueue<InternalHiveSplit> queue = new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executor);

                    @Override
                    public ListenableFuture<Void> offer(InternalHiveSplit connectorSplit)
                    {
                        return queue.offer(connectorSplit);
                    }

                    @Override
                    public <O> ListenableFuture<O> borrowBatchAsync(int maxSize, Function<List<InternalHiveSplit>, BorrowResult<InternalHiveSplit, O>> function)
                    {
                        return queue.borrowBatchAsync(maxSize, function);
                    }

                    @Override
                    public void finish()
                    {
                        queue.finish();
                    }

                    @Override
                    public boolean isFinished()
                    {
                        return queue.isFinished();
                    }
                },
                maxInitialSplits,
                maxOutstandingSplitsSize,
                splitLoader,
                stateReference,
                highMemorySplitSourceCounter,
                cachingHostAddressProvider,
                recordScannedFiles);
    }

    /**
     * The upper bound of outstanding split count.
     * It might be larger than the actual number when called concurrently with other methods.
     */
    @VisibleForTesting
    int getBufferedInternalSplitCount()
    {
        return bufferedInternalSplitCount.get();
    }

    ListenableFuture<Void> addToQueue(List<? extends InternalHiveSplit> splits)
    {
        ListenableFuture<Void> lastResult = immediateVoidFuture();
        for (InternalHiveSplit split : splits) {
            lastResult = addToQueue(split);
        }
        return lastResult;
    }

    ListenableFuture<Void> addToQueue(InternalHiveSplit split)
    {
        if (stateReference.get().getKind() != INITIAL) {
            return immediateVoidFuture();
        }
        if (estimatedSplitSizeInBytes.addAndGet(split.getEstimatedSizeInBytes()) > maxOutstandingSplitsBytes) {
            // TODO: investigate alternative split discovery strategies when this error is hit.
            // This limit should never be hit given there is a limit of maxOutstandingSplits.
            // If it's hit, it means individual splits are huge.
            if (loggedHighMemoryWarning.compareAndSet(false, true)) {
                highMemorySplitSourceCounter.update(1);
                log.warn("Split buffering for %s.%s in query %s exceeded memory limit (%s). %s splits are buffered.",
                        databaseName, tableName, queryId, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount());
            }
            throw new TrinoException(HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT, format(
                    "Split buffering for %s.%s exceeded memory limit (%s). %s splits are buffered.",
                    databaseName, tableName, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount()));
        }
        bufferedInternalSplitCount.incrementAndGet();
        return queues.offer(split);
    }

    void noMoreSplits()
    {
        if (setIf(stateReference, State.noMoreSplits(), state -> state.getKind() == INITIAL)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    void fail(Throwable e)
    {
        // The error must be recorded before setting the finish marker to make sure
        // isFinished will observe failure instead of successful completion.
        // Only record the first error message.
        if (setIf(stateReference, State.failed(e), state -> state.getKind() == INITIAL)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        boolean noMoreSplits;
        State state = stateReference.get();
        switch (state.getKind()) {
            case INITIAL:
                noMoreSplits = false;
                break;
            case NO_MORE_SPLITS:
                noMoreSplits = true;
                break;
            case FAILED:
                return failedFuture(state.getThrowable());
            case CLOSED:
                throw new IllegalStateException("HiveSplitSource is already closed");
            default:
                throw new UnsupportedOperationException();
        }

        ListenableFuture<ImmutableList<HiveSplit>> future = queues.borrowBatchAsync(maxSize, internalSplits -> {
            ImmutableList.Builder<InternalHiveSplit> splitsToInsertBuilder = ImmutableList.builder();
            ImmutableList.Builder<HiveSplit> resultBuilder = ImmutableList.builder();
            int removedEstimatedSizeInBytes = 0;
            int removedSplitCount = 0;
            for (InternalHiveSplit internalSplit : internalSplits) {
                // Dynamic filter may not have been ready when partition was loaded in BackgroundHiveSplitLoader.
                // Perform one more dynamic filter check immediately before split is returned to the engine
                if (!internalSplit.getPartitionMatchSupplier().getAsBoolean()) {
                    removedEstimatedSizeInBytes += internalSplit.getEstimatedSizeInBytes();
                    removedSplitCount++;
                    continue;
                }

                long maxSplitBytes = maxSplitSize.toBytes();
                if (remainingInitialSplits.get() > 0) {
                    if (remainingInitialSplits.getAndDecrement() > 0) {
                        maxSplitBytes = maxInitialSplitSize.toBytes();
                    }
                }
                InternalHiveBlock block = internalSplit.currentBlock();
                long splitBytes;
                if (internalSplit.isSplittable()) {
                    long remainingBlockBytes = block.getEnd() - internalSplit.getStart();
                    if (remainingBlockBytes <= maxSplitBytes) {
                        splitBytes = remainingBlockBytes;
                    }
                    else if (maxSplitBytes * 2 >= remainingBlockBytes) {
                        //  Second to last split in this block, generate two evenly sized splits
                        splitBytes = remainingBlockBytes / 2;
                    }
                    else {
                        splitBytes = maxSplitBytes;
                    }
                }
                else {
                    splitBytes = internalSplit.getEnd() - internalSplit.getStart();
                }

                resultBuilder.add(new HiveSplit(
                        internalSplit.getPartitionName(),
                        internalSplit.getPath(),
                        internalSplit.getStart(),
                        splitBytes,
                        internalSplit.getEstimatedFileSize(),
                        internalSplit.getFileModifiedTime(),
                        internalSplit.getSchema(),
                        internalSplit.getPartitionKeys(),
                        cachingHostAddressProvider.getHosts(internalSplit.getPath(), block.getAddresses()),
                        internalSplit.getReadBucketNumber(),
                        internalSplit.getTableBucketNumber(),
                        internalSplit.isForceLocalScheduling(),
                        internalSplit.getHiveColumnCoercions(),
                        internalSplit.getBucketConversion(),
                        internalSplit.getBucketValidation(),
                        internalSplit.getAcidInfo(),
                        splitWeightProvider.weightForSplitSizeInBytes(splitBytes)));

                internalSplit.increaseStart(splitBytes);

                if (internalSplit.isDone()) {
                    removedEstimatedSizeInBytes += internalSplit.getEstimatedSizeInBytes();
                    removedSplitCount++;
                }
                else {
                    splitsToInsertBuilder.add(internalSplit);
                }
            }
            estimatedSplitSizeInBytes.addAndGet(-removedEstimatedSizeInBytes);
            bufferedInternalSplitCount.addAndGet(-removedSplitCount);

            return new AsyncQueue.BorrowResult<>(splitsToInsertBuilder.build(), resultBuilder.build());
        });

        return toCompletableFuture(future).thenApply(hiveSplits -> {
            requireNonNull(hiveSplits, "hiveSplits is null");
            if (recordScannedFiles) {
                hiveSplits.stream()
                        .filter(split -> split.getStart() == 0)
                        .map(HiveSplit::getPath)
                        .forEach(scannedFilePaths::add);
            }
            // This won't actually initiate a copy since hiveSplits is already an ImmutableList, but it will
            // let us convert from List<HiveSplit> to List<ConnectorSplit> without casting
            List<ConnectorSplit> splits = ImmutableList.copyOf(hiveSplits);
            if (noMoreSplits) {
                // Checking splits.isEmpty() here is required for thread safety.
                // Let's say there are 10 splits left, and max number of splits per batch is 5.
                // The futures constructed in two getNextBatch calls could each fetch 5, resulting in zero splits left.
                // After fetching the splits, both futures reach this line at the same time.
                // Without the isEmpty check, both will claim they are the last.
                // Side note 1: In such a case, it doesn't actually matter which one gets to claim it's the last.
                //              But having both claim they are the last would be a surprising behavior.
                // Side note 2: One could argue that the isEmpty check is overly conservative.
                //              The caller of getNextBatch will likely need to make an extra invocation.
                //              But an extra invocation likely doesn't matter.
                return new ConnectorSplitBatch(splits, splits.isEmpty() && queues.isFinished());
            }
            return new ConnectorSplitBatch(splits, false);
        });
    }

    @Override
    public boolean isFinished()
    {
        State state = stateReference.get();

        return switch (state.getKind()) {
            case INITIAL -> false;
            case NO_MORE_SPLITS -> bufferedInternalSplitCount.get() == 0;
            case FAILED -> throw propagateTrinoException(state.getThrowable());
            case CLOSED -> throw new IllegalStateException("HiveSplitSource is already closed");
        };
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        checkState(isFinished(), "HiveSplitSource must be finished before TableExecuteSplitsInfo is read");
        if (!recordScannedFiles) {
            return Optional.empty();
        }
        return Optional.of(scannedFilePaths.build());
    }

    @Override
    public void close()
    {
        if (setIf(stateReference, State.closed(), state -> state.getKind() == INITIAL || state.getKind() == NO_MORE_SPLITS)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    private static <T> boolean setIf(AtomicReference<T> atomicReference, T newValue, Predicate<T> predicate)
    {
        while (true) {
            T current = atomicReference.get();
            if (!predicate.test(current)) {
                return false;
            }
            if (atomicReference.compareAndSet(current, newValue)) {
                return true;
            }
        }
    }

    private static RuntimeException propagateTrinoException(Throwable throwable)
    {
        if (throwable instanceof TrinoException trinoException) {
            throw trinoException;
        }
        if (throwable instanceof FileNotFoundException) {
            throw new TrinoException(HIVE_FILE_NOT_FOUND, throwable);
        }
        throw new TrinoException(HIVE_UNKNOWN_ERROR, throwable);
    }

    interface PerBucket
    {
        ListenableFuture<Void> offer(InternalHiveSplit split);

        <O> ListenableFuture<O> borrowBatchAsync(int maxSize, Function<List<InternalHiveSplit>, BorrowResult<InternalHiveSplit, O>> function);

        void finish();

        boolean isFinished();
    }

    static class State
    {
        private final StateKind kind;
        private final Throwable throwable;

        private State(StateKind kind, Throwable throwable)
        {
            this.kind = kind;
            this.throwable = throwable;
        }

        public StateKind getKind()
        {
            return kind;
        }

        public Throwable getThrowable()
        {
            checkState(throwable != null);
            return throwable;
        }

        public static State initial()
        {
            return new State(INITIAL, null);
        }

        public static State noMoreSplits()
        {
            return new State(NO_MORE_SPLITS, null);
        }

        public static State failed(Throwable throwable)
        {
            return new State(FAILED, throwable);
        }

        public static State closed()
        {
            return new State(CLOSED, null);
        }
    }

    enum StateKind
    {
        INITIAL,
        NO_MORE_SPLITS,
        FAILED,
        CLOSED,
    }
}
