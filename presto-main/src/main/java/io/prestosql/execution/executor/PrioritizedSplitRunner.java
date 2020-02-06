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
package io.prestosql.execution.executor;

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.CpuTimer;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.prestosql.execution.SplitRunner;
import io.prestosql.spi.tracer.Tracer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.prestosql.operator.Operator.NOT_BLOCKED;
import static io.prestosql.spi.tracer.TracerEventType.SPLIT_ADDED;
import static io.prestosql.spi.tracer.TracerEventType.SPLIT_BLOCKED;
import static io.prestosql.spi.tracer.TracerEventType.SPLIT_DESTROY_INVOKED;
import static io.prestosql.spi.tracer.TracerEventType.SPLIT_DRIVER_CREATED;
import static io.prestosql.spi.tracer.TracerEventType.SPLIT_ENDS_WAITING;
import static io.prestosql.spi.tracer.TracerEventType.SPLIT_FINISHED;
import static io.prestosql.spi.tracer.TracerEventType.SPLIT_SCHEDULED;
import static io.prestosql.spi.tracer.TracerEventType.SPLIT_STARTS_WAITING;
import static io.prestosql.spi.tracer.TracerEventType.SPLIT_UNBLOCKED;
import static io.prestosql.spi.tracer.TracerEventType.SPLIT_UNSCHEDULED;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class PrioritizedSplitRunner
        implements Comparable<PrioritizedSplitRunner>
{
    private static final AtomicLong NEXT_WORKER_ID = new AtomicLong();

    private static final Logger log = Logger.get(PrioritizedSplitRunner.class);

    // each time we run a split, run it for this length before returning to the pool
    public static final Duration SPLIT_RUN_QUANTA = new Duration(1, TimeUnit.SECONDS);

    private final long createdNanos = System.nanoTime();

    private final TaskHandle taskHandle;
    private final int splitId;
    private final long workerId;
    private final SplitRunner split;

    private final Ticker ticker;

    private final SettableFuture<?> finishedFuture = SettableFuture.create();

    private final AtomicBoolean destroyed = new AtomicBoolean();

    protected final AtomicReference<Priority> priority = new AtomicReference<>(new Priority(0, 0));

    protected final AtomicLong lastRun = new AtomicLong();
    private final AtomicLong lastReady = new AtomicLong();
    private final AtomicLong start = new AtomicLong();

    private final AtomicLong scheduledNanos = new AtomicLong();
    private final AtomicLong waitNanos = new AtomicLong();
    private final AtomicLong cpuTimeNanos = new AtomicLong();
    private final AtomicLong processCalls = new AtomicLong();

    private final CounterStat globalCpuTimeMicros;
    private final CounterStat globalScheduledTimeMicros;

    private final TimeStat blockedQuantaWallTime;
    private final TimeStat unblockedQuantaWallTime;

    private final Tracer tracer;

    PrioritizedSplitRunner(
            TaskHandle taskHandle,
            SplitRunner split,
            Ticker ticker,
            CounterStat globalCpuTimeMicros,
            CounterStat globalScheduledTimeMicros,
            TimeStat blockedQuantaWallTime,
            TimeStat unblockedQuantaWallTime)
    {
        this.taskHandle = taskHandle;
        this.splitId = taskHandle.getNextSplitId();
        this.split = split;
        this.ticker = ticker;
        this.workerId = NEXT_WORKER_ID.getAndIncrement();
        this.globalCpuTimeMicros = globalCpuTimeMicros;
        this.globalScheduledTimeMicros = globalScheduledTimeMicros;
        this.blockedQuantaWallTime = blockedQuantaWallTime;
        this.unblockedQuantaWallTime = unblockedQuantaWallTime;
        this.tracer = split.getTracer().withSplitId(String.valueOf(splitId));

        this.updateLevelPriority();
    }

    public TaskHandle getTaskHandle()
    {
        return taskHandle;
    }

    public ListenableFuture<?> getFinishedFuture()
    {
        return finishedFuture;
    }

    public boolean isDestroyed()
    {
        return destroyed.get();
    }

    public void destroy()
    {
        destroyed.set(true);
        try {
            split.close();
        }
        catch (RuntimeException e) {
            log.error(e, "Error closing split for task %s", taskHandle.getTaskId());
        }
        tracer.emitEvent(SPLIT_DESTROY_INVOKED, null);
    }

    public long getCreatedNanos()
    {
        return createdNanos;
    }

    public boolean isFinished()
    {
        boolean finished = split.isFinished();
        if (finished) {
            finishedFuture.set(null);
        }
        return finished || destroyed.get() || taskHandle.isDestroyed();
    }

    public long getScheduledNanos()
    {
        return scheduledNanos.get();
    }

    public long getCpuTimeNanos()
    {
        return cpuTimeNanos.get();
    }

    public long getWaitNanos()
    {
        return waitNanos.get();
    }

    public ListenableFuture<?> process()
    {
        try {
            long startNanos = ticker.read();
            start.compareAndSet(0, startNanos);
            lastReady.compareAndSet(0, startNanos);
            processCalls.incrementAndGet();

            waitNanos.getAndAdd(startNanos - lastReady.get());

            CpuTimer timer = new CpuTimer();
            ListenableFuture<?> blocked = split.processFor(SPLIT_RUN_QUANTA);
            CpuTimer.CpuDuration elapsed = timer.elapsedTime();

            long quantaScheduledNanos = ticker.read() - startNanos;
            scheduledNanos.addAndGet(quantaScheduledNanos);

            priority.set(taskHandle.addScheduledNanos(quantaScheduledNanos));
            lastRun.set(ticker.read());

            if (blocked == NOT_BLOCKED) {
                unblockedQuantaWallTime.add(elapsed.getWall());
            }
            else {
                blockedQuantaWallTime.add(elapsed.getWall());
            }

            long quantaCpuNanos = elapsed.getCpu().roundTo(NANOSECONDS);
            cpuTimeNanos.addAndGet(quantaCpuNanos);

            globalCpuTimeMicros.update(quantaCpuNanos / 1000);
            globalScheduledTimeMicros.update(quantaScheduledNanos / 1000);

            return blocked;
        }
        catch (Throwable e) {
            finishedFuture.setException(e);
            throw e;
        }
    }

    public void setReady()
    {
        lastReady.set(ticker.read());
    }

    /**
     * Updates the (potentially stale) priority value cached in this object.
     * This should be called when this object is outside the queue.
     *
     * @return true if the level changed.
     */
    public boolean updateLevelPriority()
    {
        Priority newPriority = taskHandle.getPriority();
        Priority oldPriority = priority.getAndSet(newPriority);
        return newPriority.getLevel() != oldPriority.getLevel();
    }

    /**
     * Updates the task level priority to be greater than or equal to the minimum
     * priority within that level. This ensures that tasks that spend time blocked do
     * not return and starve already-running tasks. Also updates the cached priority
     * object.
     */
    public void resetLevelPriority()
    {
        priority.set(taskHandle.resetLevelPriority());
    }

    @Override
    public int compareTo(PrioritizedSplitRunner o)
    {
        int result = Long.compare(priority.get().getLevelPriority(), o.getPriority().getLevelPriority());
        if (result != 0) {
            return result;
        }

        return Long.compare(workerId, o.workerId);
    }

    public int getSplitId()
    {
        return splitId;
    }

    public Priority getPriority()
    {
        return priority.get();
    }

    public void splitDriverCreated()
    {
        tracer.emitEvent(SPLIT_DRIVER_CREATED, null);
    }

    public void added()
    {
        tracer.emitEvent(SPLIT_ADDED, null);
    }

    public void addedToRunning()
    {
        tracer.emitEvent(SPLIT_SCHEDULED, null);
    }

    public void addedToWaiting()
    {
        tracer.emitEvent(SPLIT_STARTS_WAITING, null);
    }

    public void addedToBlocked()
    {
        tracer.emitEvent(SPLIT_BLOCKED, null);
    }

    public void removedFromRunning()
    {
        tracer.emitEvent(SPLIT_UNSCHEDULED, null);
    }

    public void removedFromWaiting()
    {
        tracer.emitEvent(SPLIT_ENDS_WAITING, null);
    }

    public void removedFromBlocked()
    {
        tracer.emitEvent(SPLIT_UNBLOCKED, null);
    }

    public void removed()
    {
        tracer.emitEvent(SPLIT_FINISHED, null);
    }

    public String getInfo()
    {
        return format("Split %-15s-%d %s (start = %s, wall = %s ms, cpu = %s ms, wait = %s ms, calls = %s)",
                taskHandle.getTaskId(),
                splitId,
                split.getInfo(),
                start.get() / 1.0e6,
                (int) ((ticker.read() - start.get()) / 1.0e6),
                (int) (cpuTimeNanos.get() / 1.0e6),
                (int) (waitNanos.get() / 1.0e6),
                processCalls.get());
    }

    @Override
    public String toString()
    {
        return format("Split %-15s-%d", taskHandle.getTaskId(), splitId);
    }
}
