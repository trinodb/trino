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

import com.google.common.base.Ticker;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.CpuTimer;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.execution.SplitRunner;
import io.trino.tracing.TrinoAttributes;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.operator.Operator.NOT_BLOCKED;
import static io.trino.tracing.ScopedSpan.scopedSpan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class PrioritizedSplitRunner
        implements Comparable<PrioritizedSplitRunner>
{
    private static final AtomicLong NEXT_WORKER_ID = new AtomicLong();

    private static final Logger log = Logger.get(PrioritizedSplitRunner.class);

    // each time we run a split, run it for this length before returning to the pool
    public static final Duration SPLIT_RUN_QUANTA = new Duration(1, TimeUnit.SECONDS);

    private final long createdNanos = System.nanoTime();

    private final TimeSharingTaskHandle taskHandle;
    private final int splitId;
    private final long workerId;
    private final SplitRunner split;

    private final Span splitSpan;

    private final Tracer tracer;
    private final Ticker ticker;

    private final SettableFuture<Void> finishedFuture = SettableFuture.create();

    private final AtomicBoolean destroyed = new AtomicBoolean();

    private final AtomicReference<Priority> priority = new AtomicReference<>(new Priority(0, 0));

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

    PrioritizedSplitRunner(
            TimeSharingTaskHandle taskHandle,
            int splitId,
            SplitRunner split,
            Span splitSpan,
            Tracer tracer,
            Ticker ticker,
            CounterStat globalCpuTimeMicros,
            CounterStat globalScheduledTimeMicros,
            TimeStat blockedQuantaWallTime,
            TimeStat unblockedQuantaWallTime)
    {
        this.taskHandle = requireNonNull(taskHandle, "taskHandle is null");
        this.splitId = splitId;
        this.split = requireNonNull(split, "split is null");
        this.splitSpan = requireNonNull(splitSpan, "splitSpan is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.workerId = NEXT_WORKER_ID.getAndIncrement();
        this.globalCpuTimeMicros = requireNonNull(globalCpuTimeMicros, "globalCpuTimeMicros is null");
        this.globalScheduledTimeMicros = requireNonNull(globalScheduledTimeMicros, "globalScheduledTimeMicros is null");
        this.blockedQuantaWallTime = requireNonNull(blockedQuantaWallTime, "blockedQuantaWallTime is null");
        this.unblockedQuantaWallTime = requireNonNull(unblockedQuantaWallTime, "unblockedQuantaWallTime is null");

        updateLevelPriority();
    }

    public TimeSharingTaskHandle getTaskHandle()
    {
        return taskHandle;
    }

    public ListenableFuture<Void> getFinishedFuture()
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
        finally {
            splitSpan.setAttribute(TrinoAttributes.SPLIT_SCHEDULED_TIME_NANOS, getScheduledNanos());
            splitSpan.setAttribute(TrinoAttributes.SPLIT_CPU_TIME_NANOS, getCpuTimeNanos());
            splitSpan.setAttribute(TrinoAttributes.SPLIT_WAIT_TIME_NANOS, getWaitNanos());
            splitSpan.end();
        }
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

    public ListenableFuture<Void> process()
    {
        Span span = tracer.spanBuilder("process")
                .setParent(Context.current().with(splitSpan))
                .startSpan();

        try (var ignored = scopedSpan(span)) {
            long startNanos = ticker.read();
            start.compareAndSet(0, startNanos);
            lastReady.compareAndSet(0, startNanos);
            processCalls.incrementAndGet();

            waitNanos.getAndAdd(startNanos - lastReady.get());

            // Do not collect user vs system components of CPU time since it's more expensive and not used here
            CpuTimer timer = new CpuTimer(ticker, false);
            ListenableFuture<Void> blocked = split.processFor(SPLIT_RUN_QUANTA);
            CpuTimer.CpuDuration elapsed = timer.elapsedTime();

            long quantaScheduledNanos = elapsed.getWall().roundTo(NANOSECONDS);
            scheduledNanos.addAndGet(quantaScheduledNanos);

            priority.set(taskHandle.addScheduledNanos(quantaScheduledNanos));

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

            span.setAttribute(TrinoAttributes.SPLIT_CPU_TIME_NANOS, quantaCpuNanos);
            span.setAttribute(TrinoAttributes.SPLIT_SCHEDULED_TIME_NANOS, quantaScheduledNanos);
            span.setAttribute(TrinoAttributes.SPLIT_BLOCKED, blocked != NOT_BLOCKED);

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
