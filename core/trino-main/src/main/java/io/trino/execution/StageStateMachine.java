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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.log.Logger;
import io.airlift.stats.Distribution;
import io.airlift.units.Duration;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.trino.execution.StateMachine.StateChangeListener;
import io.trino.execution.scheduler.SplitSchedulerStats;
import io.trino.operator.BlockedReason;
import io.trino.operator.OperatorStats;
import io.trino.operator.PipelineStats;
import io.trino.operator.TaskStats;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.eventlistener.StageGcStatistics;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.tracing.TrinoAttributes;
import io.trino.util.Failures;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.execution.StageState.ABORTED;
import static io.trino.execution.StageState.FAILED;
import static io.trino.execution.StageState.FINISHED;
import static io.trino.execution.StageState.PENDING;
import static io.trino.execution.StageState.PLANNED;
import static io.trino.execution.StageState.RUNNING;
import static io.trino.execution.StageState.SCHEDULING;
import static io.trino.execution.StageState.TERMINAL_STAGE_STATES;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class StageStateMachine
{
    private static final Logger log = Logger.get(StageStateMachine.class);

    private final StageId stageId;
    private final PlanFragment fragment;
    private final Map<PlanNodeId, TableInfo> tables;
    private final SplitSchedulerStats scheduledStats;

    private final StateMachine<StageState> stageState;
    private final StateMachine<Optional<StageInfo>> finalStageInfo;
    private final Span stageSpan;
    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private final AtomicReference<DateTime> schedulingComplete = new AtomicReference<>();
    private final Distribution getSplitDistribution = new Distribution();

    private final AtomicLong peakUserMemory = new AtomicLong();
    private final AtomicLong peakRevocableMemory = new AtomicLong();
    private final AtomicLong currentUserMemory = new AtomicLong();
    private final AtomicLong currentRevocableMemory = new AtomicLong();
    private final AtomicLong currentTotalMemory = new AtomicLong();

    public StageStateMachine(
            StageId stageId,
            PlanFragment fragment,
            Map<PlanNodeId, TableInfo> tables,
            Executor executor,
            Tracer tracer,
            Span schedulerSpan,
            SplitSchedulerStats schedulerStats)
    {
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.fragment = requireNonNull(fragment, "fragment is null");
        this.tables = ImmutableMap.copyOf(requireNonNull(tables, "tables is null"));
        this.scheduledStats = requireNonNull(schedulerStats, "schedulerStats is null");

        stageState = new StateMachine<>("stage " + stageId, executor, PLANNED, TERMINAL_STAGE_STATES);
        stageState.addStateChangeListener(state -> log.debug("Stage %s is %s", stageId, state));

        finalStageInfo = new StateMachine<>("final stage " + stageId, executor, Optional.empty());

        stageSpan = tracer.spanBuilder("stage")
                .setParent(Context.current().with(schedulerSpan))
                .setAttribute(TrinoAttributes.QUERY_ID, stageId.getQueryId().toString())
                .setAttribute(TrinoAttributes.STAGE_ID, stageId.toString())
                .startSpan();

        stageState.addStateChangeListener(state -> {
            stageSpan.addEvent("stage_state", Attributes.of(
                    TrinoAttributes.EVENT_STATE, state.toString()));
            if (state.isDone()) {
                stageSpan.end();
            }
        });
    }

    public StageId getStageId()
    {
        return stageId;
    }

    public StageState getState()
    {
        return stageState.get();
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public Span getStageSpan()
    {
        return stageSpan;
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<StageState> stateChangeListener)
    {
        stageState.addStateChangeListener(stateChangeListener);
    }

    public boolean transitionToScheduling()
    {
        return stageState.compareAndSet(PLANNED, SCHEDULING);
    }

    public boolean transitionToRunning()
    {
        schedulingComplete.compareAndSet(null, DateTime.now());
        return stageState.setIf(RUNNING, currentState -> currentState != RUNNING && !currentState.isDone());
    }

    public boolean transitionToPending()
    {
        return stageState.setIf(PENDING, currentState -> currentState != PENDING && !currentState.isDone());
    }

    public boolean transitionToFinished()
    {
        return stageState.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    public boolean transitionToAborted()
    {
        return stageState.setIf(ABORTED, currentState -> !currentState.isDone());
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");

        failureCause.compareAndSet(null, Failures.toFailure(throwable));
        boolean failed = stageState.setIf(FAILED, currentState -> !currentState.isDone());
        if (failed) {
            log.debug(throwable, "Stage %s failed", stageId);
        }
        else {
            log.debug(throwable, "Failure after stage %s finished", stageId);
        }
        return failed;
    }

    /**
     * Add a listener for the final stage info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addFinalStageInfoListener(StateChangeListener<StageInfo> finalStatusListener)
    {
        AtomicBoolean done = new AtomicBoolean();
        StateChangeListener<Optional<StageInfo>> fireOnceStateChangeListener = finalStageInfo -> {
            if (finalStageInfo.isPresent() && done.compareAndSet(false, true)) {
                finalStatusListener.stateChanged(finalStageInfo.get());
            }
        };
        finalStageInfo.addStateChangeListener(fireOnceStateChangeListener);
    }

    public void setAllTasksFinal(Iterable<TaskInfo> finalTaskInfos)
    {
        requireNonNull(finalTaskInfos, "finalTaskInfos is null");
        checkState(stageState.get().isDone());
        StageInfo stageInfo = getStageInfo(() -> finalTaskInfos);
        checkArgument(stageInfo.isFinalStageInfo(), "finalTaskInfos are not all done");
        finalStageInfo.compareAndSet(Optional.empty(), Optional.of(stageInfo));
    }

    public long getUserMemoryReservation()
    {
        return currentUserMemory.get();
    }

    public long getTotalMemoryReservation()
    {
        return currentTotalMemory.get();
    }

    public void updateMemoryUsage(long deltaUserMemoryInBytes, long deltaRevocableMemoryInBytes, long deltaTotalMemoryInBytes)
    {
        currentUserMemory.addAndGet(deltaUserMemoryInBytes);
        currentRevocableMemory.addAndGet(deltaRevocableMemoryInBytes);
        currentTotalMemory.addAndGet(deltaTotalMemoryInBytes);
        peakUserMemory.updateAndGet(currentPeakValue -> max(currentUserMemory.get(), currentPeakValue));
        peakRevocableMemory.updateAndGet(currentPeakValue -> max(currentRevocableMemory.get(), currentPeakValue));
    }

    public BasicStageStats getBasicStageStats(Supplier<Iterable<TaskInfo>> taskInfosSupplier)
    {
        Optional<StageInfo> finalStageInfo = this.finalStageInfo.get();
        if (finalStageInfo.isPresent()) {
            return finalStageInfo.get()
                    .getStageStats()
                    .toBasicStageStats(finalStageInfo.get().getState());
        }

        // stage state must be captured first in order to provide a
        // consistent view of the stage. For example, building this
        // information, the stage could finish, and the task states would
        // never be visible.
        StageState state = stageState.get();
        boolean isScheduled = state == RUNNING || state == StageState.PENDING || state.isDone();

        List<TaskInfo> taskInfos = ImmutableList.copyOf(taskInfosSupplier.get());

        int failedTasks = 0;

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int completedDrivers = 0;
        int blockedDrivers = 0;

        double cumulativeUserMemory = 0;
        double failedCumulativeUserMemory = 0;
        long userMemoryReservation = 0;
        long totalMemoryReservation = 0;

        long totalScheduledTime = 0;
        long failedScheduledTime = 0;
        long totalCpuTime = 0;
        long failedCpuTime = 0;

        long physicalInputDataSize = 0;
        long physicalInputPositions = 0;
        long physicalInputReadTime = 0;
        long physicalWrittenBytes = 0;

        long internalNetworkInputDataSize = 0;
        long internalNetworkInputPositions = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;
        long spilledDataSize = 0;

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        for (TaskInfo taskInfo : taskInfos) {
            TaskState taskState = taskInfo.taskStatus().getState();
            TaskStats taskStats = taskInfo.stats();

            boolean taskFailedOrFailing = taskState == TaskState.FAILED || taskState == TaskState.FAILING;

            if (taskFailedOrFailing) {
                failedTasks++;
            }

            totalDrivers += taskStats.getTotalDrivers();
            queuedDrivers += taskStats.getQueuedDrivers();
            runningDrivers += taskStats.getRunningDrivers();
            completedDrivers += taskStats.getCompletedDrivers();
            blockedDrivers += taskStats.getBlockedDrivers();

            cumulativeUserMemory += taskStats.getCumulativeUserMemory();
            if (taskFailedOrFailing) {
                failedCumulativeUserMemory += taskStats.getCumulativeUserMemory();
            }

            long taskUserMemory = taskStats.getUserMemoryReservation().toBytes();
            long taskRevocableMemory = taskStats.getRevocableMemoryReservation().toBytes();
            userMemoryReservation += taskUserMemory;
            totalMemoryReservation += taskUserMemory + taskRevocableMemory;

            totalScheduledTime += taskStats.getTotalScheduledTime().roundTo(NANOSECONDS);
            totalCpuTime += taskStats.getTotalCpuTime().roundTo(NANOSECONDS);
            if (taskFailedOrFailing) {
                failedScheduledTime += taskStats.getTotalScheduledTime().roundTo(NANOSECONDS);
                failedCpuTime += taskStats.getTotalCpuTime().roundTo(NANOSECONDS);
            }
            if (!taskState.isDone()) {
                fullyBlocked &= taskStats.isFullyBlocked();
                blockedReasons.addAll(taskStats.getBlockedReasons());
            }

            physicalInputDataSize += taskStats.getPhysicalInputDataSize().toBytes();
            physicalInputPositions += taskStats.getPhysicalInputPositions();
            physicalInputReadTime += taskStats.getPhysicalInputReadTime().roundTo(NANOSECONDS);
            physicalWrittenBytes += taskStats.getPhysicalWrittenDataSize().toBytes();

            internalNetworkInputDataSize += taskStats.getInternalNetworkInputDataSize().toBytes();
            internalNetworkInputPositions += taskStats.getInternalNetworkInputPositions();

            if (fragment.containsTableScanNode()) {
                rawInputDataSize += taskStats.getRawInputDataSize().toBytes();
                rawInputPositions += taskStats.getRawInputPositions();
            }

            spilledDataSize += taskStats.getPipelines().stream()
                    .flatMap(pipeline -> pipeline.getOperatorSummaries().stream())
                    .mapToLong(summary -> summary.getSpilledDataSize().toBytes())
                    .sum();
        }

        OptionalDouble progressPercentage = OptionalDouble.empty();
        if (isScheduled && totalDrivers != 0) {
            progressPercentage = OptionalDouble.of(min(100, (completedDrivers * 100.0) / totalDrivers));
        }
        OptionalDouble runningPercentage = OptionalDouble.empty();
        if (isScheduled && totalDrivers != 0) {
            runningPercentage = OptionalDouble.of(min(100, (runningDrivers * 100.0) / totalDrivers));
        }

        return new BasicStageStats(
                isScheduled,

                failedTasks,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                completedDrivers,
                blockedDrivers,

                succinctBytes(physicalInputDataSize),
                physicalInputPositions,
                new Duration(physicalInputReadTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                succinctBytes(physicalWrittenBytes),

                succinctBytes(internalNetworkInputDataSize),
                internalNetworkInputPositions,

                succinctBytes(rawInputDataSize),
                rawInputPositions,
                succinctBytes(spilledDataSize),

                cumulativeUserMemory,
                failedCumulativeUserMemory,
                succinctBytes(userMemoryReservation),
                succinctBytes(totalMemoryReservation),

                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(failedCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(failedScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),

                fullyBlocked,
                blockedReasons,

                progressPercentage,
                runningPercentage);
    }

    public StageInfo getStageInfo(Supplier<Iterable<TaskInfo>> taskInfosSupplier)
    {
        Optional<StageInfo> finalStageInfo = this.finalStageInfo.get();
        if (finalStageInfo.isPresent()) {
            return finalStageInfo.get();
        }

        // stage state must be captured first in order to provide a
        // consistent view of the stage. For example, building this
        // information, the stage could finish, and the task states would
        // never be visible.
        StageState state = stageState.get();

        List<TaskInfo> taskInfos = ImmutableList.copyOf(taskInfosSupplier.get());

        int totalTasks = taskInfos.size();
        int runningTasks = 0;
        int completedTasks = 0;
        int failedTasks = 0;

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int blockedDrivers = 0;
        int completedDrivers = 0;

        double cumulativeUserMemory = 0;
        double failedCumulativeUserMemory = 0;
        long userMemoryReservation = currentUserMemory.get();
        long revocableMemoryReservation = currentRevocableMemory.get();
        long totalMemoryReservation = currentTotalMemory.get();
        long peakUserMemoryReservation = peakUserMemory.get();
        long peakRevocableMemoryReservation = peakRevocableMemory.get();

        long totalScheduledTime = 0;
        long failedScheduledTime = 0;
        long totalCpuTime = 0;
        long failedCpuTime = 0;
        long totalBlockedTime = 0;

        long physicalInputDataSize = 0;
        long failedPhysicalInputDataSize = 0;
        long physicalInputPositions = 0;
        long failedPhysicalInputPositions = 0;
        long physicalInputReadTime = 0;
        long failedPhysicalInputReadTime = 0;

        long internalNetworkInputDataSize = 0;
        long failedInternalNetworkInputDataSize = 0;
        long internalNetworkInputPositions = 0;
        long failedInternalNetworkInputPositions = 0;

        long rawInputDataSize = 0;
        long failedRawInputDataSize = 0;
        long rawInputPositions = 0;
        long failedRawInputPositions = 0;

        long processedInputDataSize = 0;
        long failedProcessedInputDataSize = 0;
        long processedInputPositions = 0;
        long failedProcessedInputPositions = 0;

        long inputBlockedTime = 0;
        long failedInputBlockedTime = 0;

        long bufferedDataSize = 0;
        ImmutableList.Builder<TDigestHistogram> bufferUtilizationHistograms = ImmutableList.builderWithExpectedSize(taskInfos.size());
        long outputDataSize = 0;
        long failedOutputDataSize = 0;
        long outputPositions = 0;
        long failedOutputPositions = 0;
        Metrics.Accumulator outputBufferMetrics = Metrics.accumulator();

        long outputBlockedTime = 0;
        long failedOutputBlockedTime = 0;

        long physicalWrittenDataSize = 0;
        long failedPhysicalWrittenDataSize = 0;

        int fullGcCount = 0;
        int fullGcTaskCount = 0;
        int minFullGcSec = 0;
        int maxFullGcSec = 0;
        int totalFullGcSec = 0;

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        int maxTaskOperatorSummaries = 0;
        for (TaskInfo taskInfo : taskInfos) {
            TaskState taskState = taskInfo.taskStatus().getState();
            if (taskState.isDone()) {
                completedTasks++;
            }
            else {
                runningTasks++;
            }
            boolean taskFailedOrFailing = taskState == TaskState.FAILED || taskState == TaskState.FAILING;

            if (taskFailedOrFailing) {
                failedTasks++;
            }

            TaskStats taskStats = taskInfo.stats();

            totalDrivers += taskStats.getTotalDrivers();
            queuedDrivers += taskStats.getQueuedDrivers();
            runningDrivers += taskStats.getRunningDrivers();
            blockedDrivers += taskStats.getBlockedDrivers();
            completedDrivers += taskStats.getCompletedDrivers();

            cumulativeUserMemory += taskStats.getCumulativeUserMemory();
            if (taskFailedOrFailing) {
                failedCumulativeUserMemory += taskStats.getCumulativeUserMemory();
            }

            totalScheduledTime += taskStats.getTotalScheduledTime().roundTo(NANOSECONDS);
            totalCpuTime += taskStats.getTotalCpuTime().roundTo(NANOSECONDS);
            totalBlockedTime += taskStats.getTotalBlockedTime().roundTo(NANOSECONDS);
            if (taskFailedOrFailing) {
                failedScheduledTime += taskStats.getTotalScheduledTime().roundTo(NANOSECONDS);
                failedCpuTime += taskStats.getTotalCpuTime().roundTo(NANOSECONDS);
            }
            if (!taskState.isDone()) {
                fullyBlocked &= taskStats.isFullyBlocked();
                blockedReasons.addAll(taskStats.getBlockedReasons());
            }

            physicalInputDataSize += taskStats.getPhysicalInputDataSize().toBytes();
            physicalInputPositions += taskStats.getPhysicalInputPositions();
            physicalInputReadTime += taskStats.getPhysicalInputReadTime().roundTo(NANOSECONDS);

            internalNetworkInputDataSize += taskStats.getInternalNetworkInputDataSize().toBytes();
            internalNetworkInputPositions += taskStats.getInternalNetworkInputPositions();

            rawInputDataSize += taskStats.getRawInputDataSize().toBytes();
            rawInputPositions += taskStats.getRawInputPositions();

            processedInputDataSize += taskStats.getProcessedInputDataSize().toBytes();
            processedInputPositions += taskStats.getProcessedInputPositions();

            inputBlockedTime += taskStats.getInputBlockedTime().roundTo(NANOSECONDS);

            bufferedDataSize += taskInfo.outputBuffers().getTotalBufferedBytes();

            Optional<Metrics> bufferMetrics = taskInfo.outputBuffers().getMetrics();

            taskInfo.outputBuffers().getUtilization().ifPresent(bufferUtilizationHistograms::add);
            outputDataSize += taskStats.getOutputDataSize().toBytes();
            outputPositions += taskStats.getOutputPositions();
            bufferMetrics.ifPresent(outputBufferMetrics::add);

            outputBlockedTime += taskStats.getOutputBlockedTime().roundTo(NANOSECONDS);

            physicalWrittenDataSize += taskStats.getPhysicalWrittenDataSize().toBytes();

            if (taskFailedOrFailing) {
                failedPhysicalInputDataSize += taskStats.getPhysicalInputDataSize().toBytes();
                failedPhysicalInputPositions += taskStats.getPhysicalInputPositions();
                failedPhysicalInputReadTime += taskStats.getPhysicalInputReadTime().roundTo(NANOSECONDS);

                failedInternalNetworkInputDataSize += taskStats.getInternalNetworkInputDataSize().toBytes();
                failedInternalNetworkInputPositions += taskStats.getInternalNetworkInputPositions();

                failedRawInputDataSize += taskStats.getRawInputDataSize().toBytes();
                failedRawInputPositions += taskStats.getRawInputPositions();

                failedProcessedInputDataSize += taskStats.getProcessedInputDataSize().toBytes();
                failedProcessedInputPositions += taskStats.getProcessedInputPositions();

                failedInputBlockedTime += taskStats.getInputBlockedTime().roundTo(NANOSECONDS);

                failedOutputDataSize += taskStats.getOutputDataSize().toBytes();
                failedOutputPositions += taskStats.getOutputPositions();

                failedPhysicalWrittenDataSize += taskStats.getPhysicalWrittenDataSize().toBytes();

                failedOutputBlockedTime += taskStats.getOutputBlockedTime().roundTo(NANOSECONDS);
            }

            fullGcCount += taskStats.getFullGcCount();
            fullGcTaskCount += taskStats.getFullGcCount() > 0 ? 1 : 0;

            int gcSec = toIntExact(taskStats.getFullGcTime().roundTo(SECONDS));
            totalFullGcSec += gcSec;
            minFullGcSec = min(minFullGcSec, gcSec);
            maxFullGcSec = max(maxFullGcSec, gcSec);

            // Count and record the maximum number of pipeline / operators across all task infos
            int taskOperatorSummaries = 0;
            for (PipelineStats pipeline : taskStats.getPipelines()) {
                taskOperatorSummaries += pipeline.getOperatorSummaries().size();
            }
            maxTaskOperatorSummaries = max(taskOperatorSummaries, maxTaskOperatorSummaries);
        }

        // Create merged operatorStats list if any operator summaries are present. Summarized task stats have empty operator summary lists
        List<OperatorStats> operatorStats = maxTaskOperatorSummaries == 0 ? ImmutableList.of() : combineTaskOperatorSummaries(taskInfos, maxTaskOperatorSummaries);

        StageStats stageStats = new StageStats(
                schedulingComplete.get(),
                getSplitDistribution.snapshot(),

                totalTasks,
                runningTasks,
                completedTasks,
                failedTasks,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                blockedDrivers,
                completedDrivers,

                cumulativeUserMemory,
                failedCumulativeUserMemory,
                succinctBytes(userMemoryReservation),
                succinctBytes(revocableMemoryReservation),
                succinctBytes(totalMemoryReservation),
                succinctBytes(peakUserMemoryReservation),
                succinctBytes(peakRevocableMemoryReservation),
                succinctDuration(totalScheduledTime, NANOSECONDS),
                succinctDuration(failedScheduledTime, NANOSECONDS),
                succinctDuration(totalCpuTime, NANOSECONDS),
                succinctDuration(failedCpuTime, NANOSECONDS),
                succinctDuration(totalBlockedTime, NANOSECONDS),
                fullyBlocked && runningTasks > 0,
                blockedReasons,

                succinctBytes(physicalInputDataSize),
                succinctBytes(failedPhysicalInputDataSize),
                physicalInputPositions,
                failedPhysicalInputPositions,
                succinctDuration(physicalInputReadTime, NANOSECONDS),
                succinctDuration(failedPhysicalInputReadTime, NANOSECONDS),

                succinctBytes(internalNetworkInputDataSize),
                succinctBytes(failedInternalNetworkInputDataSize),
                internalNetworkInputPositions,
                failedInternalNetworkInputPositions,

                succinctBytes(rawInputDataSize),
                succinctBytes(failedRawInputDataSize),
                rawInputPositions,
                failedRawInputPositions,

                succinctBytes(processedInputDataSize),
                succinctBytes(failedProcessedInputDataSize),
                processedInputPositions,
                failedProcessedInputPositions,
                succinctDuration(inputBlockedTime, NANOSECONDS),
                succinctDuration(failedInputBlockedTime, NANOSECONDS),
                succinctBytes(bufferedDataSize),
                TDigestHistogram.merge(bufferUtilizationHistograms.build()).map(DistributionSnapshot::new),
                succinctBytes(outputDataSize),
                succinctBytes(failedOutputDataSize),
                outputPositions,
                failedOutputPositions,
                outputBufferMetrics.get(),
                succinctDuration(outputBlockedTime, NANOSECONDS),
                succinctDuration(failedOutputBlockedTime, NANOSECONDS),
                succinctBytes(physicalWrittenDataSize),
                succinctBytes(failedPhysicalWrittenDataSize),

                new StageGcStatistics(
                        stageId.getId(),
                        totalTasks,
                        fullGcTaskCount,
                        minFullGcSec,
                        maxFullGcSec,
                        totalFullGcSec,
                        (int) (1.0 * totalFullGcSec / fullGcCount)),

                operatorStats);

        ExecutionFailureInfo failureInfo = null;
        if (state == FAILED) {
            failureInfo = failureCause.get();
        }
        return new StageInfo(
                stageId,
                state,
                fragment,
                fragment.getPartitioning().isCoordinatorOnly(),
                fragment.getTypes(),
                stageStats,
                taskInfos,
                ImmutableList.of(),
                tables,
                failureInfo);
    }

    public BasicStageInfo getBasicStageInfo(Supplier<Iterable<TaskInfo>> taskInfosSupplier)
    {
        Optional<StageInfo> finalStageInfo = this.finalStageInfo.get();
        if (finalStageInfo.isPresent()) {
            return new BasicStageInfo(finalStageInfo.get());
        }

        return new BasicStageInfo(
                stageId,
                stageState.get(),
                fragment.getPartitioning().isCoordinatorOnly(),
                getBasicStageStats(taskInfosSupplier),
                ImmutableList.of(),
                ImmutableList.copyOf(taskInfosSupplier.get()));
    }

    private static List<OperatorStats> combineTaskOperatorSummaries(List<TaskInfo> taskInfos, int maxTaskOperatorSummaries)
    {
        // Group each unique pipelineId + operatorId combination into lists
        Long2ObjectOpenHashMap<List<OperatorStats>> pipelineAndOperatorToStats = new Long2ObjectOpenHashMap<>(maxTaskOperatorSummaries);
        // Expect to have one operator stats entry for each taskInfo
        int taskInfoCount = taskInfos.size();
        LongFunction<List<OperatorStats>> statsListCreator = key -> new ArrayList<>(taskInfoCount);
        for (TaskInfo taskInfo : taskInfos) {
            for (PipelineStats pipeline : taskInfo.stats().getPipelines()) {
                // Place the pipelineId in the high bits of the combinedKey mask
                long pipelineKeyMask = Integer.toUnsignedLong(pipeline.getPipelineId()) << 32;
                for (OperatorStats operator : pipeline.getOperatorSummaries()) {
                    // Place the operatorId into the low bits of the combined key
                    long combinedKey = pipelineKeyMask | Integer.toUnsignedLong(operator.getOperatorId());
                    pipelineAndOperatorToStats.computeIfAbsent(combinedKey, statsListCreator).add(operator);
                }
            }
        }
        // Merge the list of operator stats from each pipelineId + operatorId into a single entry
        ImmutableList.Builder<OperatorStats> operatorStatsBuilder = ImmutableList.builderWithExpectedSize(pipelineAndOperatorToStats.size());
        for (List<OperatorStats> operators : pipelineAndOperatorToStats.values()) {
            OperatorStats stats = operators.get(0);
            if (operators.size() > 1) {
                stats = stats.add(operators.subList(1, operators.size()));
            }
            operatorStatsBuilder.add(stats);
        }
        return operatorStatsBuilder.build();
    }

    public void recordGetSplitTime(long startNanos)
    {
        long elapsedNanos = System.nanoTime() - startNanos;
        getSplitDistribution.add(elapsedNanos);
        scheduledStats.getGetSplitTime().add(elapsedNanos, NANOSECONDS);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stageId", stageId)
                .add("stageState", stageState)
                .toString();
    }
}
