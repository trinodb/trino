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
package io.trino.operator;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.StageId;
import io.trino.execution.TaskId;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.Exchange;
import io.trino.spi.exchange.ExchangeContext;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkHandle;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.exchange.ExchangeSource;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Multimaps.asMap;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.getUnchecked;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.trino.operator.RetryPolicy.NONE;
import static io.trino.operator.RetryPolicy.QUERY;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class DeduplicatingDirectExchangeBuffer
        implements DirectExchangeBuffer
{
    private static final Logger log = Logger.get(DeduplicatingDirectExchangeBuffer.class);

    private final Executor executor;
    private final RetryPolicy retryPolicy;

    @GuardedBy("this")
    private final Set<TaskId> allTasks = new HashSet<>();
    @GuardedBy("this")
    private boolean noMoreTasks;
    @GuardedBy("this")
    private final Set<TaskId> successfulTasks = new HashSet<>();
    @GuardedBy("this")
    private final Map<TaskId, Throwable> failedTasks = new HashMap<>();
    @GuardedBy("this")
    private int maxAttemptId;

    @GuardedBy("this")
    private final PageBuffer pageBuffer;

    private final SettableFuture<Void> outputReady = SettableFuture.create();
    @GuardedBy("this")
    private OutputSource outputSource;

    @GuardedBy("this")
    private long maxRetainedSizeInBytes;

    @GuardedBy("this")
    private Throwable failure;

    @GuardedBy("this")
    private boolean closed;

    public DeduplicatingDirectExchangeBuffer(
            Executor executor,
            DataSize bufferCapacity,
            RetryPolicy retryPolicy,
            ExchangeManagerRegistry exchangeManagerRegistry,
            QueryId queryId,
            ExchangeId exchangeId)
    {
        this.executor = requireNonNull(executor, "executor is null");
        requireNonNull(retryPolicy, "retryPolicy is null");
        checkArgument(retryPolicy != NONE, "retries should be enabled");
        this.retryPolicy = retryPolicy;
        this.pageBuffer = new PageBuffer(
                exchangeManagerRegistry,
                queryId,
                exchangeId,
                executor,
                bufferCapacity);
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        if (failure != null || closed) {
            return immediateVoidFuture();
        }

        if (!outputReady.isDone()) {
            return nonCancellationPropagating(outputReady);
        }

        checkState(outputSource != null, "outputSource is expected to be set");
        return outputSource.isBlocked();
    }

    @Override
    public synchronized Slice pollPage()
    {
        throwIfFailed();

        if (closed) {
            return null;
        }

        if (!outputReady.isDone()) {
            return null;
        }

        checkState(outputSource != null, "outputSource is expected to be set");
        Slice page = outputSource.getNext();
        updateMaxRetainedSize();
        return page;
    }

    @Override
    public synchronized void addTask(TaskId taskId)
    {
        if (closed) {
            return;
        }

        checkState(!noMoreTasks, "no more tasks expected");
        checkState(allTasks.add(taskId), "task already registered: %s", taskId);

        if (taskId.getAttemptId() > maxAttemptId) {
            maxAttemptId = taskId.getAttemptId();

            if (retryPolicy == QUERY) {
                pageBuffer.removePagesForPreviousAttempts(maxAttemptId);
                updateMaxRetainedSize();
            }
        }
    }

    @Override
    public synchronized void addPages(TaskId taskId, List<Slice> pages)
    {
        if (closed) {
            return;
        }

        if (failure != null) {
            return;
        }

        checkState(allTasks.contains(taskId), "task is not registered: %s", taskId);
        checkState(!successfulTasks.contains(taskId), "task is finished: %s", taskId);
        checkState(!failedTasks.containsKey(taskId), "task is failed: %s", taskId);

        if (retryPolicy == QUERY && taskId.getAttemptId() < maxAttemptId) {
            return;
        }

        try {
            pageBuffer.addPages(taskId, pages);
            updateMaxRetainedSize();
        }
        catch (RuntimeException e) {
            fail(e);
        }
    }

    @Override
    public synchronized void taskFinished(TaskId taskId)
    {
        if (closed) {
            return;
        }

        checkState(allTasks.contains(taskId), "task is not registered: %s", taskId);
        checkState(!failedTasks.containsKey(taskId), "task is failed: %s", taskId);
        checkState(successfulTasks.add(taskId), "task is finished: %s", taskId);

        checkInputFinished();
    }

    @Override
    public synchronized void taskFailed(TaskId taskId, Throwable t)
    {
        if (closed) {
            return;
        }

        checkState(allTasks.contains(taskId), "task is not registered: %s", taskId);
        checkState(!successfulTasks.contains(taskId), "task is finished: %s", taskId);
        checkState(failedTasks.put(taskId, t) == null, "task is already failed: %s", taskId);
        checkInputFinished();
    }

    @Override
    public synchronized void noMoreTasks()
    {
        if (closed) {
            return;
        }

        noMoreTasks = true;
        checkInputFinished();
    }

    private void checkInputFinished()
    {
        if (failure != null) {
            return;
        }

        if (outputSource != null) {
            return;
        }

        if (!noMoreTasks) {
            return;
        }

        Map<TaskId, Throwable> failures;
        switch (retryPolicy) {
            case TASK: {
                Set<Integer> allPartitions = allTasks.stream()
                        .map(TaskId::getPartitionId)
                        .collect(toImmutableSet());

                Set<Integer> successfulPartitions = successfulTasks.stream()
                        .map(TaskId::getPartitionId)
                        .collect(toImmutableSet());

                if (successfulPartitions.containsAll(allPartitions)) {
                    Map<Integer, TaskId> partitionToTaskMap = new HashMap<>();
                    for (TaskId successfulTaskId : successfulTasks) {
                        Integer partitionId = successfulTaskId.getPartitionId();
                        TaskId existing = partitionToTaskMap.get(partitionId);
                        if (existing == null || existing.getAttemptId() > successfulTaskId.getAttemptId()) {
                            partitionToTaskMap.put(partitionId, successfulTaskId);
                        }
                    }

                    outputSource = pageBuffer.createOutputSource(ImmutableSet.copyOf(partitionToTaskMap.values()));
                    unblock(outputReady);
                    return;
                }

                Set<Integer> runningPartitions = allTasks.stream()
                        .filter(taskId -> !successfulTasks.contains(taskId))
                        .filter(taskId -> !failedTasks.containsKey(taskId))
                        .map(TaskId::getPartitionId)
                        .collect(toImmutableSet());

                failures = failedTasks.entrySet().stream()
                        .filter(entry -> !successfulPartitions.contains(entry.getKey().getPartitionId()))
                        .filter(entry -> !runningPartitions.contains(entry.getKey().getPartitionId()))
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                break;
            }
            case QUERY: {
                Set<TaskId> latestAttemptTasks = allTasks.stream()
                        .filter(taskId -> taskId.getAttemptId() == maxAttemptId)
                        .collect(toImmutableSet());

                if (successfulTasks.containsAll(latestAttemptTasks)) {
                    outputSource = pageBuffer.createOutputSource(latestAttemptTasks);
                    unblock(outputReady);
                    return;
                }

                failures = failedTasks.entrySet().stream()
                        .filter(entry -> entry.getKey().getAttemptId() == maxAttemptId)
                        .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                break;
            }
            default:
                throw new UnsupportedOperationException("unexpected retry policy: " + retryPolicy);
        }

        Throwable failure = null;
        for (Map.Entry<TaskId, Throwable> entry : failures.entrySet()) {
            TaskId taskId = entry.getKey();
            Throwable taskFailure = entry.getValue();

            if (taskFailure instanceof TrinoException && REMOTE_TASK_FAILED.toErrorCode().equals(((TrinoException) taskFailure).getErrorCode())) {
                // This error indicates that a downstream task was trying to fetch results from an upstream task that is marked as failed
                // Instead of failing a downstream task let the coordinator handle and report the failure of an upstream task to ensure correct error reporting
                log.debug("Task failure discovered while fetching task results: %s", taskId);
                continue;
            }

            if (failure == null) {
                failure = taskFailure;
            }
            else if (failure != taskFailure) {
                failure.addSuppressed(taskFailure);
            }
        }

        if (failure != null) {
            fail(failure);
        }
    }

    @Override
    public synchronized boolean isFinished()
    {
        return failure == null && (closed || (outputSource != null && outputSource.isFinished()));
    }

    @Override
    public synchronized boolean isFailed()
    {
        return failure != null;
    }

    @Override
    public synchronized long getRemainingCapacityInBytes()
    {
        // this buffer is always ready to accept more data
        return Long.MAX_VALUE;
    }

    @Override
    public synchronized long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = pageBuffer.getRetainedSizeInBytes();
        if (outputSource != null) {
            retainedSizeInBytes += outputSource.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    @Override
    public synchronized long getMaxRetainedSizeInBytes()
    {
        return maxRetainedSizeInBytes;
    }

    @Override
    public synchronized int getBufferedPageCount()
    {
        return pageBuffer.getBufferedPageCount();
    }

    @Override
    public synchronized long getSpilledBytes()
    {
        return pageBuffer.getSpilledBytes();
    }

    @Override
    public synchronized int getSpilledPageCount()
    {
        return pageBuffer.getSpilledPageCount();
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        closeAndUnblock();
    }

    private void fail(Throwable failure)
    {
        this.failure = failure;
        closeAndUnblock();
    }

    private void throwIfFailed()
    {
        if (failure != null) {
            throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        }
    }

    private void closeAndUnblock()
    {
        try (Closer closer = Closer.create()) {
            closer.register(pageBuffer);
            closer.register(outputSource);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            unblock(outputReady);
        }
    }

    private void updateMaxRetainedSize()
    {
        maxRetainedSizeInBytes = max(maxRetainedSizeInBytes, getRetainedSizeInBytes());
    }

    private void unblock(SettableFuture<Void> blocked)
    {
        executor.execute(() -> blocked.set(null));
    }

    @NotThreadSafe
    private static class PageBuffer
            implements Closeable
    {
        private final ExchangeManagerRegistry exchangeManagerRegistry;
        private final QueryId queryId;
        private final ExchangeId exchangeId;
        private final Executor executor;

        private final long pageBufferCapacityInBytes;
        private final ListMultimap<TaskId, Slice> pageBuffer = ArrayListMultimap.create();
        private long pageBufferRetainedSizeInBytes;

        private ExchangeManager exchangeManager;
        private Exchange exchange;
        private ExchangeSinkInstanceHandle sinkInstanceHandle;
        private ExchangeSink exchangeSink;
        private SliceOutput writeBuffer;

        private int bufferedPageCount;
        private long spilledBytes;
        private int spilledPageCount;

        private boolean inputFinished;
        private boolean closed;

        private final AtomicBoolean exchangeSinkFinished = new AtomicBoolean();

        private PageBuffer(
                ExchangeManagerRegistry exchangeManagerRegistry,
                QueryId queryId,
                ExchangeId exchangeId,
                Executor executor,
                DataSize pageBufferCapacity)
        {
            this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.exchangeId = requireNonNull(exchangeId, "exchangeId is null");
            this.executor = requireNonNull(executor, "executor is null");
            this.pageBufferCapacityInBytes = requireNonNull(pageBufferCapacity, "pageBufferCapacity is null").toBytes();
        }

        public void addPages(TaskId taskId, List<Slice> pages)
        {
            if (closed) {
                return;
            }

            if (inputFinished) {
                // ignore extra pages after input is marked as finished
                return;
            }

            long pagesRetainedSizeInBytes = getRetainedSizeInBytes(pages);
            if (exchangeSink == null && pageBufferRetainedSizeInBytes + pagesRetainedSizeInBytes <= pageBufferCapacityInBytes) {
                pageBuffer.putAll(taskId, pages);
                pageBufferRetainedSizeInBytes += pagesRetainedSizeInBytes;
                bufferedPageCount += pages.size();
                return;
            }

            if (exchangeSink == null) {
                verify(exchangeManager == null, "exchangeManager is not expected to be initialized");
                verify(exchange == null, "exchange is not expected to be initialized");
                verify(sinkInstanceHandle == null, "sinkInstanceHandle is not expected to be initialized");
                verify(writeBuffer == null, "writeBuffer is not expected to be initialized");

                exchangeManager = exchangeManagerRegistry.getExchangeManager();
                exchange = exchangeManager.createExchange(new ExchangeContext(queryId, exchangeId), 1);

                ExchangeSinkHandle sinkHandle = exchange.addSink(0);
                sinkInstanceHandle = exchange.instantiateSink(sinkHandle, 0);
                exchange.noMoreSinks();
                exchangeSink = exchangeManager.createSink(sinkInstanceHandle, true);

                writeBuffer = new DynamicSliceOutput(DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
            }

            if (!pageBuffer.isEmpty()) {
                for (Map.Entry<TaskId, List<Slice>> entry : asMap(pageBuffer).entrySet()) {
                    writeToSink(entry.getKey(), entry.getValue());
                }
                pageBuffer.clear();
                pageBufferRetainedSizeInBytes = 0;
            }

            writeToSink(taskId, pages);
            bufferedPageCount += pages.size();
        }

        private static long getRetainedSizeInBytes(List<Slice> pages)
        {
            long result = 0;
            for (Slice page : pages) {
                result += page.getRetainedSize();
            }
            return result;
        }

        private void writeToSink(TaskId taskId, List<Slice> pages)
        {
            verify(exchangeSink != null, "exchangeSink is expected to be initialized");
            verify(writeBuffer != null, "writeBuffer is expected to be initialized");
            for (Slice page : pages) {
                // wait for the sink to unblock
                getUnchecked(exchangeSink.isBlocked());
                writeBuffer.writeInt(taskId.getStageId().getId());
                writeBuffer.writeInt(taskId.getPartitionId());
                writeBuffer.writeInt(taskId.getAttemptId());
                writeBuffer.writeBytes(page);
                exchangeSink.add(0, writeBuffer.slice());
                writeBuffer.reset();
                spilledBytes += page.length();
                spilledPageCount++;
            }
        }

        public void removePagesForPreviousAttempts(int currentAttemptId)
        {
            checkState(!inputFinished, "input is finished");

            if (closed) {
                return;
            }

            long removedPagesRetainedSizeInBytes = 0;
            int removedPagesCount = 0;
            Iterator<Map.Entry<TaskId, List<Slice>>> iterator = asMap(pageBuffer).entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<TaskId, List<Slice>> entry = iterator.next();
                TaskId taskId = entry.getKey();
                if (taskId.getAttemptId() < currentAttemptId) {
                    for (Slice page : entry.getValue()) {
                        removedPagesRetainedSizeInBytes += page.getRetainedSize();
                        removedPagesCount++;
                    }
                    iterator.remove();
                }
            }
            pageBufferRetainedSizeInBytes -= removedPagesRetainedSizeInBytes;
            bufferedPageCount -= removedPagesCount;
        }

        public OutputSource createOutputSource(Set<TaskId> selectedTasks)
        {
            checkState(!inputFinished, "input is already marked as finished and page source has already been created");
            inputFinished = true;

            if (exchangeSink == null) {
                Iterator<Slice> iterator = pageBuffer.entries().stream()
                        .filter(entry -> selectedTasks.contains(entry.getKey()))
                        .map(Map.Entry::getValue)
                        .iterator();
                return new InMemoryBufferOutputSource(iterator);
            }

            verify(exchangeManager != null, "exchangeManager is expected to be initialized");
            verify(exchange != null, "exchange is expected to be initialized");
            verify(sinkInstanceHandle != null, "sinkInstanceHandle is expected to be initialized");

            // Finish ExchangeSink and create ExchangeSource asynchronously to avoid blocking an ExchangeClient thread for potentially substantial amount of time
            ListenableFuture<ExchangeSource> exchangeSourceFuture = FluentFuture.from(toListenableFuture(exchangeSink.finish()))
                    .transformAsync((ignored) -> {
                        exchangeSinkFinished.set(true);
                        exchange.sinkFinished(sinkInstanceHandle);
                        return toListenableFuture(exchange.getSourceHandles());
                    }, executor)
                    .transform(exchangeManager::createSource, executor);
            return new ExchangeOutputSource(selectedTasks, queryId, exchangeSourceFuture);
        }

        public long getRetainedSizeInBytes()
        {
            long result = pageBufferRetainedSizeInBytes;
            if (exchangeSink != null) {
                result += exchangeSink.getMemoryUsage();
            }
            if (writeBuffer != null) {
                result += writeBuffer.getRetainedSize();
            }
            return result;
        }

        public int getBufferedPageCount()
        {
            return bufferedPageCount;
        }

        public long getSpilledBytes()
        {
            return spilledBytes;
        }

        public int getSpilledPageCount()
        {
            return spilledPageCount;
        }

        @Override
        public void close()
        {
            if (closed) {
                return;
            }
            closed = true;

            pageBuffer.clear();
            pageBufferRetainedSizeInBytes = 0;
            bufferedPageCount = 0;
            writeBuffer = null;

            if (exchangeSink != null && !exchangeSinkFinished.get()) {
                try {
                    exchangeSink.abort().whenComplete((result, failure) -> {
                        if (failure != null) {
                            log.warn(failure, "Error aborting exchange sink");
                        }
                    });
                }
                catch (RuntimeException e) {
                    log.warn(e, "Error aborting exchange sink");
                }
            }
            if (exchange != null) {
                exchange.close();
            }
        }
    }

    @NotThreadSafe
    private interface OutputSource
            extends Closeable
    {
        Slice getNext();

        boolean isFinished();

        ListenableFuture<Void> isBlocked();

        long getRetainedSizeInBytes();
    }

    @NotThreadSafe
    private static class InMemoryBufferOutputSource
            implements OutputSource
    {
        private final Iterator<Slice> iterator;

        private InMemoryBufferOutputSource(Iterator<Slice> iterator)
        {
            this.iterator = requireNonNull(iterator, "iterator is null");
        }

        @Override
        public Slice getNext()
        {
            if (!iterator.hasNext()) {
                return null;
            }
            return iterator.next();
        }

        @Override
        public boolean isFinished()
        {
            return !iterator.hasNext();
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return immediateVoidFuture();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return 0;
        }

        @Override
        public void close()
        {
        }
    }

    @NotThreadSafe
    private static class ExchangeOutputSource
            implements OutputSource
    {
        private final Set<TaskId> selectedTasks;
        private final QueryId queryId;
        private final ListenableFuture<ExchangeSource> exchangeSourceFuture;

        private ExchangeSource exchangeSource;
        private boolean finished;

        private ExchangeOutputSource(
                Set<TaskId> selectedTasks,
                QueryId queryId,
                ListenableFuture<ExchangeSource> exchangeSourceFuture)
        {
            this.selectedTasks = ImmutableSet.copyOf(requireNonNull(selectedTasks, "selectedTasks is null"));
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.exchangeSourceFuture = requireNonNull(exchangeSourceFuture, "exchangeSourceFuture is null");
        }

        @Override
        public Slice getNext()
        {
            if (finished) {
                return null;
            }
            if (exchangeSource == null) {
                if (!exchangeSourceFuture.isDone()) {
                    return null;
                }
                exchangeSource = getFutureValue(exchangeSourceFuture);
            }
            while (!exchangeSource.isFinished()) {
                if (!exchangeSource.isBlocked().isDone()) {
                    return null;
                }
                Slice buffer = exchangeSource.read();
                if (buffer == null) {
                    continue;
                }
                int stageId = buffer.getInt(0);
                int partitionId = buffer.getInt(Integer.BYTES);
                int attemptId = buffer.getInt(Integer.BYTES * 2);
                TaskId taskId = new TaskId(new StageId(queryId, stageId), partitionId, attemptId);
                if (!selectedTasks.contains(taskId)) {
                    continue;
                }
                return buffer.slice(Integer.BYTES * 3, buffer.length() - Integer.BYTES * 3);
            }
            close();
            return null;
        }

        @Override
        public boolean isFinished()
        {
            return finished;
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            if (finished) {
                return immediateVoidFuture();
            }
            if (!exchangeSourceFuture.isDone()) {
                return nonCancellationPropagating(asVoid(exchangeSourceFuture));
            }
            if (exchangeSource != null) {
                CompletableFuture<?> blocked = exchangeSource.isBlocked();
                if (!blocked.isDone()) {
                    return nonCancellationPropagating(asVoid(toListenableFuture(blocked)));
                }
            }
            return immediateVoidFuture();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            if (exchangeSource != null) {
                return exchangeSource.getMemoryUsage();
            }
            return 0;
        }

        @Override
        public void close()
        {
            if (finished) {
                return;
            }
            finished = true;
            addCallback(exchangeSourceFuture, new FutureCallback<>()
            {
                @Override
                public void onSuccess(ExchangeSource exchangeSource)
                {
                    try {
                        exchangeSource.close();
                    }
                    catch (RuntimeException e) {
                        log.warn(e, "error closing exchange source");
                    }
                }

                @Override
                public void onFailure(Throwable ignored)
                {
                    // The callback is needed to safely close the exchange source
                    // It a failure occurred it is expected to be propagated by the getNext method
                }
            }, directExecutor());
        }
    }
}
