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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.stats.TimeStat;
import io.trino.dispatcher.DispatchQuery;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.server.BasicQueryInfo;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static io.trino.execution.QueryState.RUNNING;
import static io.trino.spi.StandardErrorCode.ABANDONED_QUERY;
import static io.trino.spi.StandardErrorCode.USER_CANCELED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class QueryManagerStats
{
    private final CounterStat submittedQueries = new CounterStat();
    private final CounterStat startedQueries = new CounterStat();
    private final CounterStat completedQueries = new CounterStat();
    private final CounterStat failedQueries = new CounterStat();
    private final CounterStat abandonedQueries = new CounterStat();
    private final CounterStat canceledQueries = new CounterStat();
    private final CounterStat userErrorFailures = new CounterStat();
    private final CounterStat internalFailures = new CounterStat();
    private final CounterStat externalFailures = new CounterStat();
    private final CounterStat insufficientResourcesFailures = new CounterStat();
    private final CounterStat consumedInputRows = new CounterStat();
    private final CounterStat consumedInputBytes = new CounterStat();
    private final CounterStat consumedCpuTimeSecs = new CounterStat();
    private final AtomicLong runningDrivers = new AtomicLong();
    private final AtomicLong blockedDrivers = new AtomicLong();
    private final AtomicLong completedDrivers = new AtomicLong();
    private final AtomicLong queuedDrivers = new AtomicLong();
    private final TimeStat executionTime = new TimeStat(MILLISECONDS);
    private final TimeStat queuedTime = new TimeStat(MILLISECONDS);
    private final DistributionStat wallInputBytesRate = new DistributionStat();
    private final DistributionStat cpuInputByteRate = new DistributionStat();

    public void trackQueryStats(DispatchQuery managedQueryExecution)
    {
        submittedQueries.update(1);
        managedQueryExecution.addStateChangeListener(new StatisticsListener(managedQueryExecution));
    }

    private void queryStarted()
    {
        startedQueries.update(1);
    }

    private void queryFinished(BasicQueryInfo info)
    {
        completedQueries.update(1);

        long rawInputBytes = info.getQueryStats().getRawInputDataSize().toBytes();

        consumedCpuTimeSecs.update((long) info.getQueryStats().getTotalCpuTime().getValue(SECONDS));
        consumedInputBytes.update(info.getQueryStats().getRawInputDataSize().toBytes());
        consumedInputRows.update(info.getQueryStats().getRawInputPositions());
        executionTime.add(info.getQueryStats().getExecutionTime());
        queuedTime.add(info.getQueryStats().getQueuedTime());

        long executionWallMillis = info.getQueryStats().getExecutionTime().toMillis();
        if (executionWallMillis > 0) {
            wallInputBytesRate.add(rawInputBytes * 1000 / executionWallMillis);
        }

        long executionCpuMillis = info.getQueryStats().getTotalCpuTime().toMillis();
        if (executionCpuMillis > 0) {
            cpuInputByteRate.add(rawInputBytes * 1000 / executionCpuMillis);
        }

        if (info.getErrorCode() != null) {
            switch (info.getErrorCode().getType()) {
                case USER_ERROR:
                    userErrorFailures.update(1);
                    break;
                case INTERNAL_ERROR:
                    internalFailures.update(1);
                    break;
                case INSUFFICIENT_RESOURCES:
                    insufficientResourcesFailures.update(1);
                    break;
                case EXTERNAL:
                    externalFailures.update(1);
                    break;
            }

            if (info.getErrorCode().getCode() == ABANDONED_QUERY.toErrorCode().getCode()) {
                abandonedQueries.update(1);
            }
            else if (info.getErrorCode().getCode() == USER_CANCELED.toErrorCode().getCode()) {
                canceledQueries.update(1);
            }
            failedQueries.update(1);
        }
    }

    private class StatisticsListener
            implements StateChangeListener<QueryState>
    {
        private final Supplier<Optional<BasicQueryInfo>> finalQueryInfoSupplier;

        @GuardedBy("this")
        private boolean stopped;
        @GuardedBy("this")
        private boolean started;

        public StatisticsListener()
        {
            finalQueryInfoSupplier = Optional::empty;
        }

        public StatisticsListener(DispatchQuery managedQueryExecution)
        {
            finalQueryInfoSupplier = () -> Optional.of(managedQueryExecution.getBasicQueryInfo());
        }

        @Override
        public void stateChanged(QueryState newValue)
        {
            synchronized (this) {
                if (stopped) {
                    return;
                }

                if (newValue.isDone()) {
                    stopped = true;
                    finalQueryInfoSupplier.get()
                            .ifPresent(QueryManagerStats.this::queryFinished);
                }
                else if (newValue.ordinal() >= RUNNING.ordinal()) {
                    if (!started) {
                        started = true;
                        queryStarted();
                    }
                }
            }
        }
    }

    @Managed
    @Nested
    public CounterStat getStartedQueries()
    {
        return startedQueries;
    }

    @Managed
    @Nested
    public CounterStat getSubmittedQueries()
    {
        return submittedQueries;
    }

    @Managed
    @Nested
    public CounterStat getCompletedQueries()
    {
        return completedQueries;
    }

    @Managed
    @Nested
    public CounterStat getFailedQueries()
    {
        return failedQueries;
    }

    @Managed
    @Nested
    public CounterStat getConsumedInputRows()
    {
        return consumedInputRows;
    }

    @Managed
    @Nested
    public CounterStat getConsumedInputBytes()
    {
        return consumedInputBytes;
    }

    @Managed
    @Nested
    public CounterStat getConsumedCpuTimeSecs()
    {
        return consumedCpuTimeSecs;
    }

    @Managed
    @Nested
    public TimeStat getExecutionTime()
    {
        return executionTime;
    }

    @Managed
    @Nested
    public TimeStat getQueuedTime()
    {
        return queuedTime;
    }

    @Managed
    @Nested
    public CounterStat getUserErrorFailures()
    {
        return userErrorFailures;
    }

    @Managed
    @Nested
    public CounterStat getInternalFailures()
    {
        return internalFailures;
    }

    @Managed
    @Nested
    public CounterStat getAbandonedQueries()
    {
        return abandonedQueries;
    }

    @Managed
    @Nested
    public CounterStat getCanceledQueries()
    {
        return canceledQueries;
    }

    @Managed
    @Nested
    public CounterStat getExternalFailures()
    {
        return externalFailures;
    }

    @Managed
    @Nested
    public CounterStat getInsufficientResourcesFailures()
    {
        return insufficientResourcesFailures;
    }

    @Managed(description = "Distribution of query input data rates (wall)")
    @Nested
    public DistributionStat getWallInputBytesRate()
    {
        return wallInputBytesRate;
    }

    @Managed(description = "Distribution of query input data rates (cpu)")
    @Nested
    public DistributionStat getCpuInputByteRate()
    {
        return cpuInputByteRate;
    }

    @Managed
    public long getRunningDrivers()
    {
        return runningDrivers.get();
    }

    @Managed
    public long getBlockedDrivers()
    {
        return blockedDrivers.get();
    }

    @Managed
    public long getCompletedDrivers()
    {
        return completedDrivers.get();
    }

    @Managed
    public long getQueuedDrivers()
    {
        return queuedDrivers.get();
    }

    public void updateDriverStats(QueryTracker<DispatchQuery> queryTracker)
    {
        AtomicInteger tmpQueuedDrivers = new AtomicInteger();
        AtomicInteger tmpRunningDrivers = new AtomicInteger();
        AtomicInteger tmpBlockedDrivers = new AtomicInteger();
        AtomicInteger tmpCompletedDrivers = new AtomicInteger();
        queryTracker.getAllQueries().stream()
                .filter(query -> !query.getState().isDone())
                .map(dispatchQuery -> dispatchQuery.getBasicQueryInfo().getQueryStats())
                .forEach(stats -> {
                    tmpQueuedDrivers.addAndGet(stats.getQueuedDrivers());
                    tmpRunningDrivers.addAndGet(stats.getRunningDrivers());
                    tmpBlockedDrivers.addAndGet(stats.getBlockedDrivers());
                    tmpCompletedDrivers.addAndGet(stats.getCompletedDrivers());
                });

        queuedDrivers.set(tmpQueuedDrivers.get());
        runningDrivers.set(tmpRunningDrivers.get());
        blockedDrivers.set(tmpBlockedDrivers.get());
        completedDrivers.set(tmpCompletedDrivers.get());
    }
}
