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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.execution.TaskId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

public class TestingDirectExchangeBuffer
        implements DirectExchangeBuffer
{
    private ListenableFuture<Void> blocked = immediateVoidFuture();
    private final Set<TaskId> allTasks = new HashSet<>();
    private final ListMultimap<TaskId, Slice> pages = ArrayListMultimap.create();
    private final Set<TaskId> finishedTasks = new HashSet<>();
    private final ListMultimap<TaskId, Throwable> failedTasks = ArrayListMultimap.create();
    private boolean noMoreTasks;
    private boolean finished;
    private final long remainingBufferCapacityInBytes;

    private final Map<TaskId, SettableFuture<Void>> taskFinished = new HashMap<>();
    private final Map<TaskId, SettableFuture<Void>> taskFailed = new HashMap<>();

    public TestingDirectExchangeBuffer(DataSize bufferCapacity)
    {
        this.remainingBufferCapacityInBytes = bufferCapacity.toBytes();
    }

    @Override
    public synchronized ListenableFuture<Void> isBlocked()
    {
        return blocked;
    }

    public synchronized void setBlocked(ListenableFuture<Void> blocked)
    {
        this.blocked = requireNonNull(blocked, "blocked is null");
    }

    @Override
    public synchronized Slice pollPage()
    {
        return null;
    }

    @Override
    public synchronized void addTask(TaskId taskId)
    {
        checkState(allTasks.add(taskId), "task is already present: %s", taskId);
    }

    public synchronized Set<TaskId> getAllTasks()
    {
        return ImmutableSet.copyOf(allTasks);
    }

    @Override
    public synchronized void addPages(TaskId taskId, List<Slice> pages)
    {
        checkState(allTasks.contains(taskId), "task is expected to be present: %s", taskId);
        this.pages.putAll(taskId, pages);
    }

    public synchronized ListMultimap<TaskId, Slice> getPages()
    {
        return ImmutableListMultimap.copyOf(pages);
    }

    @Override
    public synchronized void taskFinished(TaskId taskId)
    {
        checkState(allTasks.contains(taskId), "task is expected to be present: %s", taskId);
        checkState(finishedTasks.add(taskId), "task is already finished: %s", taskId);
        taskFinished.computeIfAbsent(taskId, key -> SettableFuture.create()).set(null);
    }

    public synchronized Set<TaskId> getFinishedTasks()
    {
        return ImmutableSet.copyOf(finishedTasks);
    }

    public synchronized ListenableFuture<Void> whenTaskFinished(TaskId taskId)
    {
        return taskFinished.computeIfAbsent(taskId, key -> SettableFuture.create());
    }

    @Override
    public synchronized void taskFailed(TaskId taskId, Throwable t)
    {
        checkState(allTasks.contains(taskId), "task is expected to be present: %s", taskId);
        checkState(!finishedTasks.contains(taskId), "task is already finished: %s", taskId);
        failedTasks.put(taskId, t);
        taskFailed.computeIfAbsent(taskId, key -> SettableFuture.create()).set(null);
    }

    public synchronized ListMultimap<TaskId, Throwable> getFailedTasks()
    {
        return ImmutableListMultimap.copyOf(failedTasks);
    }

    public synchronized ListenableFuture<Void> whenTaskFailed(TaskId taskId)
    {
        return taskFailed.computeIfAbsent(taskId, key -> SettableFuture.create());
    }

    @Override
    public synchronized void noMoreTasks()
    {
        noMoreTasks = true;
    }

    public synchronized boolean isNoMoreTasks()
    {
        return noMoreTasks;
    }

    @Override
    public synchronized boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean isFailed()
    {
        return false;
    }

    public synchronized void setFinished(boolean finished)
    {
        this.finished = finished;
    }

    @Override
    public synchronized long getRemainingCapacityInBytes()
    {
        return remainingBufferCapacityInBytes;
    }

    @Override
    public synchronized long getRetainedSizeInBytes()
    {
        return 0;
    }

    @Override
    public synchronized long getMaxRetainedSizeInBytes()
    {
        return 0;
    }

    @Override
    public synchronized int getBufferedPageCount()
    {
        return 0;
    }

    @Override
    public synchronized void close()
    {
        finished = true;
    }
}
