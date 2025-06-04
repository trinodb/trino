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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.execution.SplitRunner;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.units.Duration.succinctNanos;
import static io.trino.operator.Operator.NOT_BLOCKED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

abstract class SimulationSplit
        implements SplitRunner
{
    private final SimulationTask task;

    private final AtomicInteger calls = new AtomicInteger(0);

    private final AtomicLong completedProcessNanos = new AtomicLong();
    private final AtomicLong startNanos = new AtomicLong(-1);
    private final AtomicLong doneNanos = new AtomicLong(-1);
    private final AtomicLong waitNanos = new AtomicLong();
    private final AtomicLong lastReadyTime = new AtomicLong(-1);
    private final AtomicBoolean killed = new AtomicBoolean(false);

    private final long scheduledTimeNanos;

    SimulationSplit(SimulationTask task, long scheduledTimeNanos)
    {
        this.task = requireNonNull(task, "task is null");
        this.scheduledTimeNanos = scheduledTimeNanos;
    }

    long getCompletedProcessNanos()
    {
        return completedProcessNanos.get();
    }

    long getWaitNanos()
    {
        return waitNanos.get();
    }

    int getCalls()
    {
        return calls.get();
    }

    long getScheduledTimeNanos()
    {
        return scheduledTimeNanos;
    }

    boolean isKilled()
    {
        return killed.get();
    }

    void setKilled()
    {
        waitNanos.addAndGet(System.nanoTime() - lastReadyTime.get());
        killed.set(true);
    }

    @Override
    public int getPipelineId()
    {
        return 0;
    }

    @Override
    public Span getPipelineSpan()
    {
        return Span.getInvalid();
    }

    @Override
    public boolean isFinished()
    {
        return doneNanos.get() >= 0;
    }

    @Override
    public void close() {}

    abstract boolean process();

    abstract ListenableFuture<Void> getProcessResult();

    void setSplitReady()
    {
        lastReadyTime.set(System.nanoTime());
    }

    @Override
    public ListenableFuture<Void> processFor(Duration duration)
    {
        calls.incrementAndGet();

        long callStart = System.nanoTime();
        startNanos.compareAndSet(-1, callStart);
        lastReadyTime.compareAndSet(-1, callStart);
        waitNanos.addAndGet(callStart - lastReadyTime.get());

        boolean done = process();

        long callEnd = System.nanoTime();

        completedProcessNanos.addAndGet(callEnd - callStart);

        if (done) {
            doneNanos.compareAndSet(-1, callEnd);

            if (!isKilled()) {
                task.splitComplete(this);
            }

            return immediateVoidFuture();
        }

        ListenableFuture<Void> processResult = getProcessResult();
        if (processResult.isDone()) {
            setSplitReady();
        }

        return processResult;
    }

    static class LeafSplit
            extends SimulationSplit
    {
        private final long perQuantaNanos;

        public LeafSplit(SimulationTask task, long scheduledTimeNanos, long perQuantaNanos)
        {
            super(task, scheduledTimeNanos);
            this.perQuantaNanos = perQuantaNanos;
        }

        @Override
        public boolean process()
        {
            if (getCompletedProcessNanos() >= super.scheduledTimeNanos) {
                return true;
            }

            long processNanos = Math.min(super.scheduledTimeNanos - getCompletedProcessNanos(), perQuantaNanos);
            if (processNanos > 0) {
                try {
                    NANOSECONDS.sleep(processNanos);
                }
                catch (InterruptedException e) {
                    setKilled();
                    return true;
                }
            }

            return false;
        }

        @Override
        public ListenableFuture<Void> getProcessResult()
        {
            return NOT_BLOCKED;
        }

        @Override
        public String getInfo()
        {
            double pct = (100.0 * getCompletedProcessNanos() / super.scheduledTimeNanos);
            return format(
                    "leaf %3s%% done (total: %8s, per quanta: %8s)",
                    (int) (pct > 100.00 ? 100.0 : pct),
                    succinctNanos(super.scheduledTimeNanos),
                    succinctNanos(perQuantaNanos));
        }
    }

    static class IntermediateSplit
            extends SimulationSplit
    {
        private final long wallTimeNanos;
        private final long numQuantas;
        private final long perQuantaNanos;
        private final long betweenQuantaNanos;

        private final ScheduledExecutorService executorService;

        private SettableFuture<Void> future = SettableFuture.create();
        private final SettableFuture<Void> doneFuture = SettableFuture.create();

        public IntermediateSplit(SimulationTask task, long scheduledTimeNanos, long wallTimeNanos, long numQuantas, long perQuantaNanos, long betweenQuantaNanos, ScheduledExecutorService executorService)
        {
            super(task, scheduledTimeNanos);
            this.wallTimeNanos = wallTimeNanos;
            this.numQuantas = numQuantas;
            this.perQuantaNanos = perQuantaNanos;
            this.betweenQuantaNanos = betweenQuantaNanos;
            this.executorService = executorService;

            doneFuture.set(null);
        }

        @Override
        public boolean process()
        {
            try {
                if (getCalls() < numQuantas) {
                    NANOSECONDS.sleep(perQuantaNanos);
                    return false;
                }
            }
            catch (InterruptedException _) {
                setKilled();
                return true;
            }

            return true;
        }

        @Override
        public ListenableFuture<Void> getProcessResult()
        {
            future = SettableFuture.create();
            try {
                executorService.schedule(() -> {
                    try {
                        if (!executorService.isShutdown()) {
                            future.set(null);
                        }
                        else {
                            setKilled();
                        }
                        setSplitReady();
                    }
                    catch (RuntimeException _) {
                        setKilled();
                    }
                }, betweenQuantaNanos, NANOSECONDS);
            }
            catch (RejectedExecutionException _) {
                setKilled();
                return doneFuture;
            }
            return future;
        }

        @Override
        public String getInfo()
        {
            double pct = (100.0 * getCalls() / numQuantas);
            return format("intr %3s%% done (wall: %9s, per quanta: %8s, between quanta: %8s)",
                    (int) (pct > 100.00 ? 100.0 : pct),
                    succinctNanos(wallTimeNanos),
                    succinctNanos(perQuantaNanos),
                    succinctNanos(betweenQuantaNanos));
        }
    }
}
