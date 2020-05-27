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
package io.prestosql.plugin.base.splitloader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.MoreFutures;
import io.prestosql.plugin.base.util.AsyncQueue;
import io.prestosql.plugin.base.util.ThrottledAsyncQueue;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public abstract class AbstractAsyncSplitSource<S>
        implements ConnectorSplitSource
{
    protected final String queryId;
    protected final String databaseName;
    protected final String tableName;

    private final QueueManager<S> queues;

    private final AsyncSplitLoader<S> splitLoader;
    private final AtomicReference<State> stateReference;
    private final AtomicInteger bufferedInternalSplitCount = new AtomicInteger();

    protected AbstractAsyncSplitSource(
            String queryId,
            String databaseName,
            String tableName,
            QueueManager<S> queues,
            AsyncSplitLoader<S> splitLoader)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.queues = requireNonNull(queues, "queues is null");
        this.splitLoader = requireNonNull(splitLoader, "splitLoader is null");
        this.stateReference = new AtomicReference<>(State.initial());
    }

    protected abstract void validateSplit(S split);

    /**
     * Convert a list of intermediate splits into ConnectorSplits
     * @param splits Raw splits to process
     * @return A list of intermediate splits not processed, and a list of ready ConnectorSplits
     */
    protected abstract SplitProcessingResult processSplits(List<S> splits);

    public ListenableFuture<?> addToQueue(S split)
    {
        if (stateReference.get().getKind() != StateKind.INITIAL) {
            return immediateFuture(null);
        }
        validateSplit(split);
        bufferedInternalSplitCount.incrementAndGet();
        return queues.offer(split);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
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
                return MoreFutures.failedFuture(state.getThrowable());
            case CLOSED:
                throw new IllegalStateException("HiveSplitSource is already closed");
            default:
                throw new UnsupportedOperationException();
        }

        ListenableFuture<List<ConnectorSplit>> future = queues.queueForPartition(partitionHandle).borrowBatchAsync(maxSize, splits -> {
            SplitProcessingResult splitResults = processSplits(splits);
            bufferedInternalSplitCount.addAndGet(splitResults.getIncompleteSplits().size() - splitResults.getReadySplits().size());
            return new AsyncQueue.BorrowResult<>(splitResults.getIncompleteSplits(), splitResults.getReadySplits());
        });

        ListenableFuture<ConnectorSplitBatch> transform = Futures.transform(future, splits -> {
            requireNonNull(splits, "splits is null");
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
                return new ConnectorSplitBatch(splits, splits.isEmpty() && queues.isFinished(partitionHandle));
            }
            else {
                return new ConnectorSplitBatch(splits, false);
            }
        }, directExecutor());

        return MoreFutures.toCompletableFuture(transform);
    }

    /**
     * The upper bound of outstanding split count.
     * It might be larger than the actual number when called concurrently with other methods.
     */
    @VisibleForTesting
    public int getBufferedInternalSplitCount()
    {
        return bufferedInternalSplitCount.get();
    }

    public void noMoreSplits()
    {
        if (setIf(stateReference, State.noMoreSplits(), state -> state.getKind() == StateKind.INITIAL)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    public void fail(Throwable e)
    {
        // The error must be recorded before setting the finish marker to make sure
        // isFinished will observe failure instead of successful completion.
        // Only record the first error message.
        if (setIf(stateReference, State.failed(e), state -> state.getKind() == StateKind.INITIAL)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    @Override
    public boolean isFinished()
    {
        State state = stateReference.get();

        switch (state.getKind()) {
            case INITIAL:
                return false;
            case NO_MORE_SPLITS:
                return bufferedInternalSplitCount.get() == 0;
            case FAILED:
                throw propagatePrestoException(state.getThrowable());
            case CLOSED:
                throw new IllegalStateException("HiveSplitSource is already closed");
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void close()
    {
        if (setIf(stateReference, State.closed(), state -> state.getKind() == StateKind.INITIAL || state.getKind() == StateKind.NO_MORE_SPLITS)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    private static RuntimeException propagatePrestoException(Throwable throwable)
    {
        if (throwable instanceof PrestoException) {
            throw (PrestoException) throwable;
        }
        // TODO: Make these not Hive specific
        //if (throwable instanceof FileNotFoundException) {
        //    throw new PrestoException(HiveErrorCode.HIVE_FILE_NOT_FOUND, throwable);
        //}
        //throw new PrestoException(HiveErrorCode.HIVE_UNKNOWN_ERROR, throwable);
        throw new RuntimeException(throwable);
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

    protected class SplitProcessingResult
    {
        // Pair<List<S>, List<ConnectorSplit>>
        private final List<S> incompleteSplits;
        private final List<ConnectorSplit> readySplits;

        public SplitProcessingResult(List<S> incompleteSplits, List<ConnectorSplit> readySplits)
        {
            this.incompleteSplits = incompleteSplits;
            this.readySplits = readySplits;
        }

        public List<S> getIncompleteSplits()
        {
            return incompleteSplits;
        }

        public List<ConnectorSplit> getReadySplits()
        {
            return readySplits;
        }
    }

    protected static class State
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
            return new State(StateKind.INITIAL, null);
        }

        public static State noMoreSplits()
        {
            return new State(StateKind.NO_MORE_SPLITS, null);
        }

        public static State failed(Throwable throwable)
        {
            return new State(StateKind.FAILED, throwable);
        }

        public static State closed()
        {
            return new State(StateKind.CLOSED, null);
        }
    }

    protected enum StateKind
    {
        INITIAL,
        NO_MORE_SPLITS,
        FAILED,
        CLOSED,
    }

    protected interface QueueManager<S>
    {
        ListenableFuture<?> offer(S split);

        AsyncQueue<S> queueForPartition(ConnectorPartitionHandle partitionHandle);

        void finish();

        boolean isFinished(ConnectorPartitionHandle partitionHandle);
    }

    protected static class NonBucketedQueueManager<S>
            implements QueueManager<S>
    {
        private final AsyncQueue<S> queue;

        public NonBucketedQueueManager(int maxSplitsPerSecond, int maxOutstandingSplits, Executor executor)
        {
            queue = new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executor);
        }

        @Override
        public ListenableFuture<?> offer(S connectorSplit)
        {
            // bucketNumber can be non-empty because BackgroundHiveSplitLoader does not have knowledge of execution plan
            return queue.offer(connectorSplit);
        }

        @Override
        public AsyncQueue<S> queueForPartition(ConnectorPartitionHandle partitionHandle)
        {
            return queue;
        }

        @Override
        public void finish()
        {
            queue.finish();
        }

        @Override
        public boolean isFinished(ConnectorPartitionHandle partitionHandle)
        {
            return queue.isFinished();
        }
    }
}
