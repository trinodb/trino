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
package io.trino.execution;

import com.google.common.base.Ticker;
import io.airlift.units.Duration;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static io.airlift.units.Duration.succinctNanos;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

class QueryStateTimer
{
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
    private final Ticker ticker;

    private final Instant createTime = Instant.now();

    private final long createNanos;
    private final AtomicReference<Long> beginResourceWaitingNanos = new AtomicReference<>();
    private final AtomicReference<Long> beginDispatchingNanos = new AtomicReference<>();
    private final AtomicReference<Long> beginPlanningNanos = new AtomicReference<>();
    private final AtomicReference<Long> beginPlanningCpuNanos = new AtomicReference<>();
    private final AtomicReference<Long> beginStartingNanos = new AtomicReference<>();
    private final AtomicReference<Long> beginFinishingNanos = new AtomicReference<>();
    private final AtomicReference<Long> endNanos = new AtomicReference<>();

    private final AtomicReference<Duration> queuedTime = new AtomicReference<>();
    private final AtomicReference<Duration> resourceWaitingTime = new AtomicReference<>();
    private final AtomicReference<Duration> dispatchingTime = new AtomicReference<>();
    private final AtomicReference<Duration> executionTime = new AtomicReference<>();
    private final AtomicReference<Duration> planningTime = new AtomicReference<>();
    private final AtomicReference<Duration> planningCpuTime = new AtomicReference<>();
    private final AtomicReference<Duration> startingTime = new AtomicReference<>();
    private final AtomicReference<Duration> finishingTime = new AtomicReference<>();

    private final AtomicReference<Long> beginAnalysisNanos = new AtomicReference<>();
    private final AtomicReference<Duration> analysisTime = new AtomicReference<>();

    private final AtomicReference<Long> lastHeartbeatNanos;

    public QueryStateTimer(Ticker ticker)
    {
        this.ticker = requireNonNull(ticker, "ticker is null");
        this.createNanos = tickerNanos();
        this.lastHeartbeatNanos = new AtomicReference<>(createNanos);
    }

    //
    // State transitions
    //

    public void beginWaitingForResources()
    {
        beginWaitingForResources(tickerNanos());
    }

    private void beginWaitingForResources(long now)
    {
        queuedTime.compareAndSet(null, nanosSince(createNanos, now));
        beginResourceWaitingNanos.compareAndSet(null, now);
    }

    public void beginDispatching()
    {
        beginDispatching(tickerNanos());
    }

    private void beginDispatching(long now)
    {
        beginWaitingForResources(now);
        resourceWaitingTime.compareAndSet(null, nanosSince(beginResourceWaitingNanos, now));
        beginDispatchingNanos.compareAndSet(null, now);
    }

    public void beginPlanning()
    {
        beginPlanning(tickerNanos(), currentThreadCpuTime());
    }

    private void beginPlanning(long now, long cpuNow)
    {
        beginDispatching(now);
        dispatchingTime.compareAndSet(null, nanosSince(beginDispatchingNanos, now));
        beginPlanningNanos.compareAndSet(null, now);
        beginPlanningCpuNanos.compareAndSet(null, cpuNow);
    }

    public void beginStarting()
    {
        beginStarting(tickerNanos(), currentThreadCpuTime());
    }

    private void beginStarting(long now, long cpuNow)
    {
        beginPlanning(now, cpuNow);
        planningTime.compareAndSet(null, nanosSince(beginPlanningNanos, now));
        planningCpuTime.compareAndSet(null, nanosSince(beginPlanningCpuNanos, cpuNow));
        beginStartingNanos.compareAndSet(null, now);
    }

    public void beginRunning()
    {
        beginRunning(tickerNanos());
    }

    private void beginRunning(long now)
    {
        beginStarting(now, currentThreadCpuTime());
        startingTime.compareAndSet(null, nanosSince(beginStartingNanos, now));
    }

    public void beginFinishing()
    {
        beginFinishing(tickerNanos());
    }

    private void beginFinishing(long now)
    {
        beginRunning(now);
        beginFinishingNanos.compareAndSet(null, now);
    }

    public void endQuery()
    {
        endQuery(tickerNanos());
    }

    private void endQuery(long now)
    {
        beginFinishing(now);
        finishingTime.compareAndSet(null, nanosSince(beginFinishingNanos, now));
        executionTime.compareAndSet(null, nanosSince(beginPlanningNanos, now));
        endNanos.compareAndSet(null, now);

        // Analysis is run in separate thread and there is no query state for analysis.
        // In case when analysis thread was canceled the analysis should be marked as finished.
        if (beginAnalysisNanos.get() == null) {
            analysisTime.compareAndSet(null, succinctNanos(0));
        }
        else {
            endAnalysis(now);
        }
    }

    //
    //  Additional timings
    //

    public void beginAnalysis()
    {
        beginAnalysisNanos.compareAndSet(null, tickerNanos());
    }

    public void endAnalysis()
    {
        endAnalysis(tickerNanos());
    }

    private void endAnalysis(long now)
    {
        analysisTime.compareAndSet(null, nanosSince(beginAnalysisNanos, now));
    }

    public void recordHeartbeat()
    {
        lastHeartbeatNanos.set(tickerNanos());
    }

    //
    // Stats
    //

    public Instant getCreateTime()
    {
        return createTime;
    }

    public Optional<Instant> getExecutionStartTime()
    {
        return toInstant(beginPlanningNanos);
    }

    public Optional<Instant> getPlanningStartTime()
    {
        return toInstant(beginPlanningNanos);
    }

    public Duration getElapsedTime()
    {
        if (endNanos.get() != null) {
            return succinctNanos(endNanos.get() - createNanos);
        }
        return nanosSince(createNanos, tickerNanos());
    }

    public Duration getQueuedTime()
    {
        Duration queuedTime = this.queuedTime.get();
        if (queuedTime != null) {
            return queuedTime;
        }

        // if queue time is not set, the query is still queued
        return getElapsedTime();
    }

    public Duration getResourceWaitingTime()
    {
        return getDuration(resourceWaitingTime, beginResourceWaitingNanos);
    }

    public Duration getDispatchingTime()
    {
        return getDuration(dispatchingTime, beginDispatchingNanos);
    }

    public Duration getPlanningTime()
    {
        return getDuration(planningTime, beginPlanningNanos);
    }

    public Duration getPlanningCpuTime()
    {
        return getDuration(planningCpuTime, beginPlanningCpuNanos);
    }

    public Duration getStartingTime()
    {
        return getDuration(startingTime, beginStartingNanos);
    }

    public Duration getFinishingTime()
    {
        return getDuration(finishingTime, beginFinishingNanos);
    }

    public Duration getExecutionTime()
    {
        return getDuration(executionTime, beginPlanningNanos);
    }

    public Optional<Instant> getEndTime()
    {
        return toInstant(endNanos);
    }

    public Duration getAnalysisTime()
    {
        return getDuration(analysisTime, beginAnalysisNanos);
    }

    public Instant getLastHeartbeat()
    {
        return toInstant(lastHeartbeatNanos.get());
    }

    //
    // Helper methods
    //

    private long tickerNanos()
    {
        return ticker.read();
    }

    private static Duration nanosSince(AtomicReference<Long> start, long end)
    {
        Long startNanos = start.get();
        if (startNanos == null) {
            throw new IllegalStateException("Start time not set");
        }
        return nanosSince(startNanos, end);
    }

    private static Duration nanosSince(long start, long now)
    {
        return succinctNanos(max(0, now - start));
    }

    private Duration getDuration(AtomicReference<Duration> finalDuration, AtomicReference<Long> start)
    {
        Duration duration = finalDuration.get();
        if (duration != null) {
            return duration;
        }
        Long startNanos = start.get();
        if (startNanos != null) {
            return nanosSince(startNanos, tickerNanos());
        }
        return new Duration(0, MILLISECONDS);
    }

    private Optional<Instant> toInstant(AtomicReference<Long> instantNanos)
    {
        Long nanos = instantNanos.get();
        if (nanos == null) {
            return Optional.empty();
        }
        return Optional.of(toInstant(nanos));
    }

    private Instant toInstant(long instantNanos)
    {
        return createTime.plusMillis(NANOSECONDS.toMillis(instantNanos - createNanos));
    }

    private static long currentThreadCpuTime()
    {
        return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    }
}
