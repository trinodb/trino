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
package io.trino.execution.executor.timesharing;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.units.Duration;
import io.trino.execution.TaskId;
import io.trino.execution.executor.LeafSplitMemoryEstimate;
import io.trino.execution.executor.SplitConcurrencyController;
import io.trino.execution.executor.TaskHandle;
import io.trino.execution.executor.TaskMemory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TimeSharingTaskHandle
        implements TaskHandle
{
    private volatile boolean destroyed;
    private final TaskId taskId;
    private final DoubleSupplier utilizationSupplier;

    @GuardedBy("this")
    private final Queue<PrioritizedSplitRunner> queuedLeafSplits = new ArrayDeque<>(10);
    @GuardedBy("this")
    private final List<PrioritizedSplitRunner> runningLeafSplits = new ArrayList<>(10);
    @GuardedBy("this")
    private final List<PrioritizedSplitRunner> runningIntermediateSplits = new ArrayList<>(10);
    @GuardedBy("this")
    private long scheduledNanos;
    @GuardedBy("this")
    private final SplitConcurrencyController concurrencyController;
    private final AtomicBoolean targetConcurrencyIncreased = new AtomicBoolean();

    private final AtomicInteger nextSplitId = new AtomicInteger();

    private final AtomicReference<Priority> priority = new AtomicReference<>(new Priority(0, 0));
    private final MultilevelSplitQueue splitQueue;
    private final OptionalInt maxDriversPerTask;

    private final LeafSplitMemoryEstimate leafMemoryEstimate = new LeafSplitMemoryEstimate();
    // Leaf splits blocked on their output or on another operator.
    private final Set<PrioritizedSplitRunner> parkedLeafSplits = ConcurrentHashMap.newKeySet();

    public TimeSharingTaskHandle(
            TaskId taskId,
            MultilevelSplitQueue splitQueue,
            DoubleSupplier utilizationSupplier,
            int initialSplitConcurrency,
            Duration splitConcurrencyAdjustFrequency,
            OptionalInt maxDriversPerTask)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.splitQueue = requireNonNull(splitQueue, "splitQueue is null");
        this.utilizationSupplier = requireNonNull(utilizationSupplier, "utilizationSupplier is null");
        this.maxDriversPerTask = requireNonNull(maxDriversPerTask, "maxDriversPerTask is null");
        this.concurrencyController = new SplitConcurrencyController(
                initialSplitConcurrency,
                requireNonNull(splitConcurrencyAdjustFrequency, "splitConcurrencyAdjustFrequency is null"));
    }

    public synchronized Priority addScheduledNanos(long durationNanos)
    {
        int previousTargetConcurrency = concurrencyController.getTargetConcurrency();
        concurrencyController.update(durationNanos, utilizationSupplier.getAsDouble(), runningLeafSplits.size());
        if (concurrencyController.getTargetConcurrency() > previousTargetConcurrency) {
            targetConcurrencyIncreased.set(true);
        }
        scheduledNanos += durationNanos;

        Priority newPriority = splitQueue.updatePriority(priority.get(), durationNanos, scheduledNanos);

        priority.set(newPriority);
        return newPriority;
    }

    // Returns true once per target concurrency increase so the executor can start queued leaf splits.
    public boolean pollTargetConcurrencyIncreased()
    {
        if (!targetConcurrencyIncreased.get()) {
            return false;
        }
        return targetConcurrencyIncreased.getAndSet(false);
    }

    public synchronized Priority resetLevelPriority()
    {
        Priority currentPriority = priority.get();
        long levelMinPriority = splitQueue.getLevelMinPriority(currentPriority.getLevel(), scheduledNanos);

        if (currentPriority.getLevelPriority() < levelMinPriority) {
            Priority newPriority = new Priority(currentPriority.getLevel(), levelMinPriority);
            priority.set(newPriority);
            return newPriority;
        }

        return currentPriority;
    }

    @Override
    public boolean isDestroyed()
    {
        return destroyed;
    }

    public Priority getPriority()
    {
        return priority.get();
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public OptionalInt getMaxDriversPerTask()
    {
        return maxDriversPerTask;
    }

    @Override
    public void setTaskMemory(TaskMemory taskMemory)
    {
        leafMemoryEstimate.setTaskMemory(taskMemory);
    }

    public LeafSplitMemoryEstimate getLeafMemoryEstimate()
    {
        return leafMemoryEstimate;
    }

    public void leafSplitParked(PrioritizedSplitRunner split)
    {
        parkedLeafSplits.add(split);
    }

    public void leafSplitUnparked(PrioritizedSplitRunner split)
    {
        parkedLeafSplits.remove(split);
    }

    // A task with a leaf split waiting on its output or on another operator gains nothing from more leaf splits.
    public boolean isParked()
    {
        return !parkedLeafSplits.isEmpty();
    }

    public synchronized boolean hasQueuedLeafSplits()
    {
        return !queuedLeafSplits.isEmpty();
    }

    // Returns any remaining splits. The caller must destroy these.
    public synchronized List<PrioritizedSplitRunner> destroy()
    {
        destroyed = true;

        ImmutableList.Builder<PrioritizedSplitRunner> builder = ImmutableList.builderWithExpectedSize(runningIntermediateSplits.size() + runningLeafSplits.size() + queuedLeafSplits.size());
        builder.addAll(runningIntermediateSplits);
        builder.addAll(runningLeafSplits);
        builder.addAll(queuedLeafSplits);
        runningIntermediateSplits.clear();
        runningLeafSplits.clear();
        queuedLeafSplits.clear();
        parkedLeafSplits.clear();
        return builder.build();
    }

    public synchronized boolean enqueueSplit(PrioritizedSplitRunner split)
    {
        if (destroyed) {
            return false;
        }
        queuedLeafSplits.add(split);
        return true;
    }

    public synchronized boolean recordIntermediateSplit(PrioritizedSplitRunner split)
    {
        if (destroyed) {
            return false;
        }
        runningIntermediateSplits.add(split);
        return true;
    }

    synchronized int getRunningLeafSplits()
    {
        return runningLeafSplits.size();
    }

    synchronized int getTargetConcurrency()
    {
        return concurrencyController.getTargetConcurrency();
    }

    public synchronized long getScheduledNanos()
    {
        return scheduledNanos;
    }

    public synchronized PrioritizedSplitRunner pollNextSplit()
    {
        if (destroyed) {
            return null;
        }

        if (runningLeafSplits.size() >= concurrencyController.getTargetConcurrency()) {
            return null;
        }

        PrioritizedSplitRunner split = queuedLeafSplits.poll();
        if (split != null) {
            runningLeafSplits.add(split);
        }
        return split;
    }

    public synchronized void splitComplete(PrioritizedSplitRunner split)
    {
        concurrencyController.splitFinished(split.getScheduledNanos(), utilizationSupplier.getAsDouble(), runningLeafSplits.size());
        runningIntermediateSplits.remove(split);
        runningLeafSplits.remove(split);
        parkedLeafSplits.remove(split);
    }

    public int getNextSplitId()
    {
        return nextSplitId.getAndIncrement();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .toString();
    }
}
