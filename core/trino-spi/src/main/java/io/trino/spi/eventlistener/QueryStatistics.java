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

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryStatistics
{
    private final Duration cpuTime;
    private final Duration wallTime;
    private final Duration queuedTime;
    private final Optional<Duration> scheduledTime;
    private final Optional<Duration> waitingTime;
    private final Optional<Duration> analysisTime;
    private final Optional<Duration> planningTime;
    private final Optional<Duration> executionTime;

    private final long peakUserMemoryBytes;
    // peak of user + system memory
    private final long peakTotalNonRevocableMemoryBytes;
    private final long peakTaskUserMemory;
    private final long peakTaskTotalMemory;
    private final long physicalInputBytes;
    private final long physicalInputRows;
    private final long internalNetworkBytes;
    private final long internalNetworkRows;
    private final long totalBytes;
    private final long totalRows;
    private final long outputBytes;
    private final long outputRows;
    private final long writtenBytes;
    private final long writtenRows;

    private final double cumulativeMemory;
    private final double cumulativeSystemMemory;

    private final List<StageGcStatistics> stageGcStatistics;

    private final int completedSplits;
    private final boolean complete;

    private final List<StageCpuDistribution> cpuTimeDistribution;

    /**
     * Operator summaries serialized to JSON. Serialization format and structure
     * can change without preserving backward compatibility.
     */
    private final List<String> operatorSummaries;
    /**
     * Plan node stats and costs serialized to JSON. Serialization format and structure
     * can change without preserving backward compatibility.
     */
    private final Optional<String> planNodeStatsAndCosts;

    public QueryStatistics(
            Duration cpuTime,
            Duration wallTime,
            Duration queuedTime,
            Optional<Duration> scheduledTime,
            Optional<Duration> waitingTime,
            Optional<Duration> analysisTime,
            Optional<Duration> planningTime,
            Optional<Duration> executionTime,
            long peakUserMemoryBytes,
            long peakTotalNonRevocableMemoryBytes,
            long peakTaskUserMemory,
            long peakTaskTotalMemory,
            long physicalInputBytes,
            long physicalInputRows,
            long internalNetworkBytes,
            long internalNetworkRows,
            long totalBytes,
            long totalRows,
            long outputBytes,
            long outputRows,
            long writtenBytes,
            long writtenRows,
            double cumulativeMemory,
            double cumulativeSystemMemory,
            List<StageGcStatistics> stageGcStatistics,
            int completedSplits,
            boolean complete,
            List<StageCpuDistribution> cpuTimeDistribution,
            List<String> operatorSummaries,
            Optional<String> planNodeStatsAndCosts)
    {
        this.cpuTime = requireNonNull(cpuTime, "cpuTime is null");
        this.wallTime = requireNonNull(wallTime, "wallTime is null");
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.scheduledTime = requireNonNull(scheduledTime, "scheduledTime is null");
        this.waitingTime = requireNonNull(waitingTime, "waitingTime is null");
        this.analysisTime = requireNonNull(analysisTime, "analysisTime is null");
        this.planningTime = requireNonNull(planningTime, "planningTime is null");
        this.executionTime = requireNonNull(executionTime, "executionTime is null");
        this.peakUserMemoryBytes = peakUserMemoryBytes;
        this.peakTotalNonRevocableMemoryBytes = peakTotalNonRevocableMemoryBytes;
        this.peakTaskUserMemory = peakTaskUserMemory;
        this.peakTaskTotalMemory = peakTaskTotalMemory;
        this.physicalInputBytes = physicalInputBytes;
        this.physicalInputRows = physicalInputRows;
        this.internalNetworkBytes = internalNetworkBytes;
        this.internalNetworkRows = internalNetworkRows;
        this.totalBytes = totalBytes;
        this.totalRows = totalRows;
        this.outputBytes = outputBytes;
        this.outputRows = outputRows;
        this.writtenBytes = writtenBytes;
        this.writtenRows = writtenRows;
        this.cumulativeMemory = cumulativeMemory;
        this.cumulativeSystemMemory = cumulativeSystemMemory;
        this.stageGcStatistics = requireNonNull(stageGcStatistics, "stageGcStatistics is null");
        this.completedSplits = completedSplits;
        this.complete = complete;
        this.cpuTimeDistribution = requireNonNull(cpuTimeDistribution, "cpuTimeDistribution is null");
        this.operatorSummaries = requireNonNull(operatorSummaries, "operatorSummaries is null");
        this.planNodeStatsAndCosts = requireNonNull(planNodeStatsAndCosts, "planNodeStatsAndCosts is null");
    }

    public Duration getCpuTime()
    {
        return cpuTime;
    }

    public Duration getWallTime()
    {
        return wallTime;
    }

    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    public Optional<Duration> getScheduledTime()
    {
        return scheduledTime;
    }

    public Optional<Duration> getResourceWaitingTime()
    {
        return waitingTime;
    }

    public Optional<Duration> getAnalysisTime()
    {
        return analysisTime;
    }

    public Optional<Duration> getPlanningTime()
    {
        return planningTime;
    }

    public Optional<Duration> getExecutionTime()
    {
        return executionTime;
    }

    public long getPeakUserMemoryBytes()
    {
        return peakUserMemoryBytes;
    }

    public long getPeakTotalNonRevocableMemoryBytes()
    {
        return peakTotalNonRevocableMemoryBytes;
    }

    public long getPeakTaskUserMemory()
    {
        return peakTaskUserMemory;
    }

    public long getPeakTaskTotalMemory()
    {
        return peakTaskTotalMemory;
    }

    public long getPhysicalInputBytes()
    {
        return physicalInputBytes;
    }

    public long getPhysicalInputRows()
    {
        return physicalInputRows;
    }

    public long getInternalNetworkBytes()
    {
        return internalNetworkBytes;
    }

    public long getInternalNetworkRows()
    {
        return internalNetworkRows;
    }

    public long getTotalBytes()
    {
        return totalBytes;
    }

    public long getTotalRows()
    {
        return totalRows;
    }

    public long getOutputBytes()
    {
        return outputBytes;
    }

    public long getOutputRows()
    {
        return outputRows;
    }

    public long getWrittenBytes()
    {
        return writtenBytes;
    }

    public long getWrittenRows()
    {
        return writtenRows;
    }

    public double getCumulativeMemory()
    {
        return cumulativeMemory;
    }

    public double getCumulativeSystemMemory()
    {
        return cumulativeSystemMemory;
    }

    public List<StageGcStatistics> getStageGcStatistics()
    {
        return stageGcStatistics;
    }

    public int getCompletedSplits()
    {
        return completedSplits;
    }

    public boolean isComplete()
    {
        return complete;
    }

    public List<StageCpuDistribution> getCpuTimeDistribution()
    {
        return cpuTimeDistribution;
    }

    public List<String> getOperatorSummaries()
    {
        return operatorSummaries;
    }

    public Optional<String> getPlanNodeStatsAndCosts()
    {
        return planNodeStatsAndCosts;
    }
}
