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

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.TaskId;
import io.trino.spi.TrinoException;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.trino.operator.RetryPolicy.QUERY;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class DeduplicationExchangeClientBuffer
        implements ExchangeClientBuffer
{
    private static final Logger log = Logger.get(DeduplicationExchangeClientBuffer.class);

    private final Executor executor;
    private final long bufferCapacityInBytes;
    private final RetryPolicy retryPolicy;

    private final SettableFuture<Void> blocked = SettableFuture.create();
    @GuardedBy("this")
    private final Set<TaskId> allTasks = new HashSet<>();
    @GuardedBy("this")
    private boolean noMoreTasks;
    @GuardedBy("this")
    private final Set<TaskId> successfulTasks = new HashSet<>();
    @GuardedBy("this")
    private final Map<TaskId, Throwable> failedTasks = new HashMap<>();
    @GuardedBy("this")
    private boolean inputFinished;
    @GuardedBy("this")
    private Throwable failure;

    @GuardedBy("this")
    private final ListMultimap<TaskId, Slice> pageBuffer = LinkedListMultimap.create();
    @GuardedBy("this")
    private Iterator<Slice> pagesIterator;
    @GuardedBy("this")
    private volatile long bufferRetainedSizeInBytes;
    @GuardedBy("this")
    private volatile long maxBufferRetainedSizeInBytes;
    @GuardedBy("this")
    private int maxAttemptId;

    @GuardedBy("this")
    private boolean closed;

    public DeduplicationExchangeClientBuffer(Executor executor, DataSize bufferCapacity, RetryPolicy retryPolicy)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.bufferCapacityInBytes = requireNonNull(bufferCapacity, "bufferCapacity is null").toBytes();
        requireNonNull(retryPolicy, "retryPolicy is null");
        checkArgument(retryPolicy == QUERY, "retryPolicy is expected to be QUERY: %s", retryPolicy);
        this.retryPolicy = retryPolicy;
    }

    @Override
    public ListenableFuture<Void> isBlocked()
    {
        return nonCancellationPropagating(blocked);
    }

    @Override
    public synchronized Slice pollPage()
    {
        throwIfFailed();

        if (closed) {
            return null;
        }

        if (!inputFinished) {
            return null;
        }

        if (pagesIterator == null) {
            pagesIterator = pageBuffer.values().iterator();
        }

        if (!pagesIterator.hasNext()) {
            return null;
        }

        Slice page = pagesIterator.next();
        pagesIterator.remove();
        bufferRetainedSizeInBytes -= page.getRetainedSize();

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
                removePagesForPreviousAttempts(taskId.getAttemptId());
            }
        }
    }

    @Override
    public synchronized void addPages(TaskId taskId, List<Slice> pages)
    {
        if (closed) {
            return;
        }

        checkState(allTasks.contains(taskId), "task is not registered: %s", taskId);
        checkState(!successfulTasks.contains(taskId), "task is finished: %s", taskId);
        checkState(!failedTasks.containsKey(taskId), "task is failed: %s", taskId);

        if (failure != null) {
            return;
        }

        if (retryPolicy == QUERY && taskId.getAttemptId() < maxAttemptId) {
            return;
        }

        long pagesRetainedSizeInBytes = 0;
        for (Slice page : pages) {
            pagesRetainedSizeInBytes += page.getRetainedSize();
        }
        bufferRetainedSizeInBytes += pagesRetainedSizeInBytes;
        if (bufferRetainedSizeInBytes > bufferCapacityInBytes) {
            // TODO: implement disk spilling
            failure = new TrinoException(NOT_SUPPORTED, "Retries for queries with large result set currently unsupported");
            pageBuffer.clear();
            bufferRetainedSizeInBytes = 0;
            unblock(blocked);
            return;
        }
        maxBufferRetainedSizeInBytes = max(maxBufferRetainedSizeInBytes, bufferRetainedSizeInBytes);
        pageBuffer.putAll(taskId, pages);
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

        if (retryPolicy == TASK) {
            // TODO implement deduplication for task level retries
            throw new UnsupportedOperationException("task level retry policy is unsupported");
        }
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

    private synchronized void checkInputFinished()
    {
        if (failure != null) {
            return;
        }

        if (inputFinished) {
            return;
        }

        if (!noMoreTasks) {
            return;
        }

        if (allTasks.isEmpty()) {
            inputFinished = true;
            unblock(blocked);
            return;
        }

        switch (retryPolicy) {
            case TASK:
                // TODO implement deduplication for task level retries
                throw new UnsupportedOperationException("task level retry policy is unsupported");
            case QUERY: {
                Set<TaskId> latestAttemptTasks = allTasks.stream()
                        .filter(taskId -> taskId.getAttemptId() == maxAttemptId)
                        .collect(toImmutableSet());

                if (successfulTasks.containsAll(latestAttemptTasks)) {
                    removePagesForPreviousAttempts(maxAttemptId);
                    inputFinished = true;
                    unblock(blocked);
                    return;
                }

                Throwable failure = null;
                for (Map.Entry<TaskId, Throwable> entry : failedTasks.entrySet()) {
                    TaskId taskId = entry.getKey();
                    Throwable taskFailure = entry.getValue();

                    if (taskId.getAttemptId() != maxAttemptId) {
                        // ignore failures from previous attempts
                        continue;
                    }

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
                    pageBuffer.clear();
                    bufferRetainedSizeInBytes = 0;
                    this.failure = failure;
                    unblock(blocked);
                }
                break;
            }
            default:
                throw new UnsupportedOperationException("unexpected retry policy: " + retryPolicy);
        }
    }

    private synchronized void removePagesForPreviousAttempts(int currentAttemptId)
    {
        // wipe previous attempt pages
        long pagesRetainedSizeInBytes = 0;
        Iterator<Map.Entry<TaskId, Slice>> iterator = pageBuffer.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<TaskId, Slice> entry = iterator.next();
            if (entry.getKey().getAttemptId() < currentAttemptId) {
                pagesRetainedSizeInBytes += entry.getValue().getRetainedSize();
                iterator.remove();
            }
        }
        bufferRetainedSizeInBytes -= pagesRetainedSizeInBytes;
    }

    @Override
    public synchronized boolean isFinished()
    {
        return failure == null && (closed || (inputFinished && pageBuffer.isEmpty()));
    }

    @Override
    public synchronized boolean isFailed()
    {
        return failure != null;
    }

    @Override
    public long getRemainingCapacityInBytes()
    {
        return max(bufferCapacityInBytes - bufferRetainedSizeInBytes, 0);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return bufferRetainedSizeInBytes;
    }

    @Override
    public long getMaxRetainedSizeInBytes()
    {
        return maxBufferRetainedSizeInBytes;
    }

    @Override
    public synchronized int getBufferedPageCount()
    {
        return pageBuffer.size();
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        pageBuffer.clear();
        bufferRetainedSizeInBytes = 0;
        unblock(blocked);
    }

    private synchronized void throwIfFailed()
    {
        if (failure != null) {
            throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        }
    }

    private void unblock(SettableFuture<Void> blocked)
    {
        executor.execute(() -> blocked.set(null));
    }
}
