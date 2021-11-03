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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.TaskId;
import io.trino.spi.TrinoException;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.trino.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class StreamingDirectExchangeBuffer
        implements DirectExchangeBuffer
{
    private static final Logger log = Logger.get(StreamingDirectExchangeBuffer.class);

    private final Executor executor;
    private final long bufferCapacityInBytes;

    @GuardedBy("this")
    private final Queue<Slice> bufferedPages = new ArrayDeque<>();
    @GuardedBy("this")
    private volatile long bufferRetainedSizeInBytes;
    @GuardedBy("this")
    private volatile long maxBufferRetainedSizeInBytes;
    @GuardedBy("this")
    private volatile SettableFuture<Void> blocked = SettableFuture.create();
    @GuardedBy("this")
    private final Set<TaskId> activeTasks = new HashSet<>();
    @GuardedBy("this")
    private boolean noMoreTasks;
    @GuardedBy("this")
    private Throwable failure;
    @GuardedBy("this")
    private boolean closed;

    public StreamingDirectExchangeBuffer(Executor executor, DataSize bufferCapacity)
    {
        this.executor = requireNonNull(executor, "executor is null");
        this.bufferCapacityInBytes = requireNonNull(bufferCapacity, "bufferCapacity is null").toBytes();
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
        Slice page = bufferedPages.poll();
        if (page != null) {
            bufferRetainedSizeInBytes -= page.getRetainedSize();
            checkState(bufferRetainedSizeInBytes >= 0, "unexpected bufferRetainedSizeInBytes: %s", bufferRetainedSizeInBytes);
        }
        // if buffer is empty block future calls
        if (bufferedPages.isEmpty() && !isFinished() && blocked.isDone()) {
            blocked = SettableFuture.create();
        }
        return page;
    }

    @Override
    public synchronized void addTask(TaskId taskId)
    {
        if (closed) {
            return;
        }
        checkState(!noMoreTasks, "no more tasks are expected");
        activeTasks.add(taskId);
    }

    @Override
    public void addPages(TaskId taskId, List<Slice> pages)
    {
        long pagesRetainedSizeInBytes = 0;
        for (Slice page : pages) {
            pagesRetainedSizeInBytes += page.getRetainedSize();
        }
        synchronized (this) {
            if (closed) {
                return;
            }
            checkState(activeTasks.contains(taskId), "taskId is not active: %s", taskId);
            bufferedPages.addAll(pages);
            bufferRetainedSizeInBytes += pagesRetainedSizeInBytes;
            maxBufferRetainedSizeInBytes = max(maxBufferRetainedSizeInBytes, bufferRetainedSizeInBytes);
            unblockIfNecessary(blocked);
        }
    }

    @Override
    public synchronized void taskFinished(TaskId taskId)
    {
        if (closed) {
            return;
        }
        checkState(activeTasks.contains(taskId), "taskId not registered: %s", taskId);
        activeTasks.remove(taskId);
        if (noMoreTasks && activeTasks.isEmpty() && !blocked.isDone()) {
            unblockIfNecessary(blocked);
        }
    }

    @Override
    public synchronized void taskFailed(TaskId taskId, Throwable t)
    {
        if (closed) {
            return;
        }
        checkState(activeTasks.contains(taskId), "taskId not registered: %s", taskId);

        if (t instanceof TrinoException && REMOTE_TASK_FAILED.toErrorCode().equals(((TrinoException) t).getErrorCode())) {
            // This error indicates that a downstream task was trying to fetch results from an upstream task that is marked as failed
            // Instead of failing a downstream task let the coordinator handle and report the failure of an upstream task to ensure correct error reporting
            log.debug("Task failure discovered while fetching task results: %s", taskId);
            return;
        }

        failure = t;
        activeTasks.remove(taskId);
        unblockIfNecessary(blocked);
    }

    @Override
    public synchronized void noMoreTasks()
    {
        noMoreTasks = true;
        if (activeTasks.isEmpty() && !blocked.isDone()) {
            unblockIfNecessary(blocked);
        }
    }

    @Override
    public synchronized boolean isFinished()
    {
        return failure == null && noMoreTasks && activeTasks.isEmpty() && bufferedPages.isEmpty();
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
        return bufferedPages.size();
    }

    @Override
    public synchronized void close()
    {
        if (closed) {
            return;
        }
        bufferedPages.clear();
        bufferRetainedSizeInBytes = 0;
        activeTasks.clear();
        noMoreTasks = true;
        closed = true;
        unblockIfNecessary(blocked);
    }

    private void unblockIfNecessary(SettableFuture<Void> blocked)
    {
        if (!blocked.isDone()) {
            executor.execute(() -> blocked.set(null));
        }
    }

    private synchronized void throwIfFailed()
    {
        if (failure != null) {
            throwIfUnchecked(failure);
            throw new RuntimeException(failure);
        }
    }
}
