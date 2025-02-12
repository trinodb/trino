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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Unstable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class QueryStatistics
{
    private final Duration cpuTime;
    private final Duration failedCpuTime;
    private final Duration wallTime;
    private final Duration queuedTime;
    private final Optional<Duration> scheduledTime;
    private final Optional<Duration> failedScheduledTime;
    private final Optional<Duration> resourceWaitingTime;
    private final Optional<Duration> analysisTime;
    private final Optional<Duration> planningTime;
    private final Optional<Duration> planningCpuTime;
    private final Optional<Duration> startingTime;
    private final Optional<Duration> executionTime;
    private final Optional<Duration> inputBlockedTime;
    private final Optional<Duration> failedInputBlockedTime;
    private final Optional<Duration> outputBlockedTime;
    private final Optional<Duration> failedOutputBlockedTime;
    private final Optional<Duration> physicalInputReadTime;

    private final long peakUserMemoryBytes;
    private final long peakTaskUserMemory;
    private final long peakTaskTotalMemory;
    private final long physicalInputBytes;
    private final long physicalInputRows;
    private final long processedInputBytes;
    private final long processedInputRows;
    private final long internalNetworkBytes;
    private final long internalNetworkRows;
    private final long totalBytes;
    private final long totalRows;
    private final long outputBytes;
    private final long outputRows;
    private final long writtenBytes;
    private final long writtenRows;
    private final long spilledBytes;

    private final double cumulativeMemory;
    private final double failedCumulativeMemory;

    private final List<StageGcStatistics> stageGcStatistics;

    private final int completedSplits;
    private final boolean complete;

    private final List<StageCpuDistribution> cpuTimeDistribution;
    private final List<StageOutputBufferUtilization> outputBufferUtilization;
    private final List<StageTaskStatistics> taskStatistics;
    private final List<StageOutputBufferMetrics> outputBufferMetrics;

    /**
     * Operator summaries serialized to JSON. Serialization format and structure
     * can change without preserving backward compatibility.
     */
    private final Supplier<List<String>> operatorSummariesProvider;
    private final List<QueryPlanOptimizerStatistics> optimizerRulesSummaries;
    /**
     * Plan node stats and costs serialized to JSON. Serialization format and structure
     * can change without preserving backward compatibility.
     */
    private final Optional<String> planNodeStatsAndCosts;

    @JsonCreator
    @Unstable
    public QueryStatistics(
            Duration cpuTime,
            Duration failedCpuTime,
            Duration wallTime,
            Duration queuedTime,
            Optional<Duration> scheduledTime,
            Optional<Duration> failedScheduledTime,
            Optional<Duration> resourceWaitingTime,
            Optional<Duration> analysisTime,
            Optional<Duration> planningTime,
            Optional<Duration> planningCpuTime,
            Optional<Duration> startingTime,
            Optional<Duration> executionTime,
            Optional<Duration> inputBlockedTime,
            Optional<Duration> failedInputBlockedTime,
            Optional<Duration> outputBlockedTime,
            Optional<Duration> failedOutputBlockedTime,
            Optional<Duration> physicalInputReadTime,
            long peakUserMemoryBytes,
            long peakTaskUserMemory,
            long peakTaskTotalMemory,
            long physicalInputBytes,
            long physicalInputRows,
            long processedInputBytes,
            long processedInputRows,
            long internalNetworkBytes,
            long internalNetworkRows,
            long totalBytes,
            long totalRows,
            long outputBytes,
            long outputRows,
            long writtenBytes,
            long writtenRows,
            long spilledBytes,
            double cumulativeMemory,
            double failedCumulativeMemory,
            List<StageGcStatistics> stageGcStatistics,
            int completedSplits,
            boolean complete,
            List<StageCpuDistribution> cpuTimeDistribution,
            List<StageOutputBufferUtilization> outputBufferUtilization,
            List<StageOutputBufferMetrics> outputBufferMetrics,
            List<StageTaskStatistics> taskStatistics,
            List<String> operatorSummaries,
            List<QueryPlanOptimizerStatistics> optimizerRulesSummaries,
            Optional<String> planNodeStatsAndCosts)
    {
        this(
                cpuTime,
                failedCpuTime,
                wallTime,
                queuedTime,
                scheduledTime,
                failedScheduledTime,
                resourceWaitingTime,
                analysisTime,
                planningTime,
                planningCpuTime,
                startingTime,
                executionTime,
                inputBlockedTime,
                failedInputBlockedTime,
                outputBlockedTime,
                failedOutputBlockedTime,
                physicalInputReadTime,
                peakUserMemoryBytes,
                peakTaskUserMemory,
                peakTaskTotalMemory,
                physicalInputBytes,
                physicalInputRows,
                processedInputBytes,
                processedInputRows,
                internalNetworkBytes,
                internalNetworkRows,
                totalBytes,
                totalRows,
                outputBytes,
                outputRows,
                writtenBytes,
                writtenRows,
                spilledBytes,
                cumulativeMemory,
                failedCumulativeMemory,
                stageGcStatistics,
                completedSplits,
                complete,
                cpuTimeDistribution,
                outputBufferUtilization,
                outputBufferMetrics,
                taskStatistics,
                () -> operatorSummaries,
                optimizerRulesSummaries,
                planNodeStatsAndCosts);
    }

    public QueryStatistics(
            Duration cpuTime,
            Duration failedCpuTime,
            Duration wallTime,
            Duration queuedTime,
            Optional<Duration> scheduledTime,
            Optional<Duration> failedScheduledTime,
            Optional<Duration> resourceWaitingTime,
            Optional<Duration> analysisTime,
            Optional<Duration> planningTime,
            Optional<Duration> planningCpuTime,
            Optional<Duration> startingTime,
            Optional<Duration> executionTime,
            Optional<Duration> inputBlockedTime,
            Optional<Duration> failedInputBlockedTime,
            Optional<Duration> outputBlockedTime,
            Optional<Duration> failedOutputBlockedTime,
            Optional<Duration> physicalInputReadTime,
            long peakUserMemoryBytes,
            long peakTaskUserMemory,
            long peakTaskTotalMemory,
            long physicalInputBytes,
            long physicalInputRows,
            long processedInputBytes,
            long processedInputRows,
            long internalNetworkBytes,
            long internalNetworkRows,
            long totalBytes,
            long totalRows,
            long outputBytes,
            long outputRows,
            long writtenBytes,
            long writtenRows,
            long spilledBytes,
            double cumulativeMemory,
            double failedCumulativeMemory,
            List<StageGcStatistics> stageGcStatistics,
            int completedSplits,
            boolean complete,
            List<StageCpuDistribution> cpuTimeDistribution,
            List<StageOutputBufferUtilization> outputBufferUtilization,
            List<StageOutputBufferMetrics> outputBufferMetrics,
            List<StageTaskStatistics> taskStatistics,
            Supplier<List<String>> operatorSummariesProvider,
            List<QueryPlanOptimizerStatistics> optimizerRulesSummaries,
            Optional<String> planNodeStatsAndCosts)
    {
        this.cpuTime = requireNonNull(cpuTime, "cpuTime is null");
        this.failedCpuTime = requireNonNull(failedCpuTime, "failedCpuTime is null");
        this.wallTime = requireNonNull(wallTime, "wallTime is null");
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.scheduledTime = requireNonNull(scheduledTime, "scheduledTime is null");
        this.failedScheduledTime = requireNonNull(failedScheduledTime, "failedScheduledTime is null");
        this.resourceWaitingTime = requireNonNull(resourceWaitingTime, "resourceWaitingTime is null");
        this.analysisTime = requireNonNull(analysisTime, "analysisTime is null");
        this.planningTime = requireNonNull(planningTime, "planningTime is null");
        this.planningCpuTime = requireNonNull(planningCpuTime, "planningCpuTime is null");
        this.startingTime = requireNonNull(startingTime, "startingTime is null");
        this.executionTime = requireNonNull(executionTime, "executionTime is null");
        this.inputBlockedTime = requireNonNull(inputBlockedTime, "inputBlockedTime is null");
        this.failedInputBlockedTime = requireNonNull(failedInputBlockedTime, "failedInputBlockedTime is null");
        this.outputBlockedTime = requireNonNull(outputBlockedTime, "outputBlockedTime is null");
        this.failedOutputBlockedTime = requireNonNull(failedOutputBlockedTime, "failedOutputBlockedTime is null");
        this.physicalInputReadTime = requireNonNull(physicalInputReadTime, "physicalInputReadTime is null");
        this.peakUserMemoryBytes = peakUserMemoryBytes;
        this.peakTaskUserMemory = peakTaskUserMemory;
        this.peakTaskTotalMemory = peakTaskTotalMemory;
        this.physicalInputBytes = physicalInputBytes;
        this.physicalInputRows = physicalInputRows;
        this.processedInputBytes = processedInputBytes;
        this.processedInputRows = processedInputRows;
        this.internalNetworkBytes = internalNetworkBytes;
        this.internalNetworkRows = internalNetworkRows;
        this.totalBytes = totalBytes;
        this.totalRows = totalRows;
        this.outputBytes = outputBytes;
        this.outputRows = outputRows;
        this.writtenBytes = writtenBytes;
        this.writtenRows = writtenRows;
        this.spilledBytes = spilledBytes;
        this.cumulativeMemory = cumulativeMemory;
        this.failedCumulativeMemory = failedCumulativeMemory;
        this.stageGcStatistics = requireNonNull(stageGcStatistics, "stageGcStatistics is null");
        this.completedSplits = completedSplits;
        this.complete = complete;
        this.cpuTimeDistribution = requireNonNull(cpuTimeDistribution, "cpuTimeDistribution is null");
        this.outputBufferUtilization = requireNonNull(outputBufferUtilization, "outputBufferUtilization is null");
        this.outputBufferMetrics = requireNonNull(outputBufferMetrics, "outputBufferMetrics is null");
        this.taskStatistics = requireNonNull(taskStatistics, "taskStatistics is null");
        this.operatorSummariesProvider = requireNonNull(operatorSummariesProvider, "operatorSummariesProvider is null");
        this.optimizerRulesSummaries = requireNonNull(optimizerRulesSummaries, "optimizerRulesSummaries is null");
        this.planNodeStatsAndCosts = requireNonNull(planNodeStatsAndCosts, "planNodeStatsAndCosts is null");
    }

    @JsonProperty
    public Duration getCpuTime()
    {
        return cpuTime;
    }

    @JsonProperty
    public Duration getFailedCpuTime()
    {
        return failedCpuTime;
    }

    @JsonProperty
    public Duration getWallTime()
    {
        return wallTime;
    }

    @JsonProperty
    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public Optional<Duration> getScheduledTime()
    {
        return scheduledTime;
    }

    @JsonProperty
    public Optional<Duration> getFailedScheduledTime()
    {
        return failedScheduledTime;
    }

    @JsonProperty
    public Optional<Duration> getResourceWaitingTime()
    {
        return resourceWaitingTime;
    }

    @JsonProperty
    public Optional<Duration> getAnalysisTime()
    {
        return analysisTime;
    }

    @JsonProperty
    public Optional<Duration> getPlanningTime()
    {
        return planningTime;
    }

    @JsonProperty
    public Optional<Duration> getPlanningCpuTime()
    {
        return planningCpuTime;
    }

    @JsonProperty
    public Optional<Duration> getStartingTime()
    {
        return startingTime;
    }

    @JsonProperty
    public Optional<Duration> getExecutionTime()
    {
        return executionTime;
    }

    @JsonProperty
    public Optional<Duration> getInputBlockedTime()
    {
        return inputBlockedTime;
    }

    @JsonProperty
    public Optional<Duration> getFailedInputBlockedTime()
    {
        return failedInputBlockedTime;
    }

    @JsonProperty
    public Optional<Duration> getOutputBlockedTime()
    {
        return outputBlockedTime;
    }

    @JsonProperty
    public Optional<Duration> getFailedOutputBlockedTime()
    {
        return failedOutputBlockedTime;
    }

    @JsonProperty
    public Optional<Duration> getPhysicalInputReadTime()
    {
        return physicalInputReadTime;
    }

    @JsonProperty
    public long getPeakUserMemoryBytes()
    {
        return peakUserMemoryBytes;
    }

    @JsonProperty
    public long getPeakTaskUserMemory()
    {
        return peakTaskUserMemory;
    }

    @JsonProperty
    public long getPeakTaskTotalMemory()
    {
        return peakTaskTotalMemory;
    }

    @JsonProperty
    public long getPhysicalInputBytes()
    {
        return physicalInputBytes;
    }

    @JsonProperty
    public long getPhysicalInputRows()
    {
        return physicalInputRows;
    }

    @JsonProperty
    public long getProcessedInputBytes()
    {
        return processedInputBytes;
    }

    @JsonProperty
    public long getProcessedInputRows()
    {
        return processedInputRows;
    }

    @JsonProperty
    public long getInternalNetworkBytes()
    {
        return internalNetworkBytes;
    }

    @JsonProperty
    public long getInternalNetworkRows()
    {
        return internalNetworkRows;
    }

    @JsonProperty
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @JsonProperty
    public long getTotalRows()
    {
        return totalRows;
    }

    @JsonProperty
    public long getOutputBytes()
    {
        return outputBytes;
    }

    @JsonProperty
    public long getOutputRows()
    {
        return outputRows;
    }

    @JsonProperty
    public long getWrittenBytes()
    {
        return writtenBytes;
    }

    @JsonProperty
    public long getWrittenRows()
    {
        return writtenRows;
    }

    @JsonProperty
    public long getSpilledBytes()
    {
        return spilledBytes;
    }

    @JsonProperty
    public double getCumulativeMemory()
    {
        return cumulativeMemory;
    }

    @JsonProperty
    public double getFailedCumulativeMemory()
    {
        return failedCumulativeMemory;
    }

    @JsonProperty
    public List<StageGcStatistics> getStageGcStatistics()
    {
        return stageGcStatistics;
    }

    @JsonProperty
    public int getCompletedSplits()
    {
        return completedSplits;
    }

    @JsonProperty
    public boolean isComplete()
    {
        return complete;
    }

    @JsonProperty
    public List<StageCpuDistribution> getCpuTimeDistribution()
    {
        return cpuTimeDistribution;
    }

    @JsonProperty
    public List<StageOutputBufferUtilization> getOutputBufferUtilization()
    {
        return outputBufferUtilization;
    }

    @JsonProperty
    public List<StageOutputBufferMetrics> getOutputBufferMetrics()
    {
        return outputBufferMetrics;
    }

    @JsonProperty
    public List<StageTaskStatistics> getTaskStatistics()
    {
        return taskStatistics;
    }

    @JsonProperty
    public List<String> getOperatorSummaries()
    {
        return operatorSummariesProvider.get();
    }

    @JsonProperty
    public List<QueryPlanOptimizerStatistics> getOptimizerRulesSummaries()
    {
        return optimizerRulesSummaries;
    }

    @JsonProperty
    public Optional<String> getPlanNodeStatsAndCosts()
    {
        return planNodeStatsAndCosts;
    }
}
