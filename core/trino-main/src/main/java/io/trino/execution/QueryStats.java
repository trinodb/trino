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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.operator.BlockedReason;
import io.trino.operator.OperatorStats;
import io.trino.operator.TableWriterOperator;
import io.trino.spi.eventlistener.StageGcStatistics;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.util.List;
import java.util.OptionalDouble;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.server.DynamicFilterService.DynamicFiltersStats;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class QueryStats
{
    private final DateTime createTime;

    private final DateTime executionStartTime;
    private final DateTime lastHeartbeat;
    private final DateTime endTime;

    private final Duration elapsedTime;
    private final Duration queuedTime;
    private final Duration resourceWaitingTime;
    private final Duration dispatchingTime;
    private final Duration executionTime;
    private final Duration analysisTime;
    private final Duration planningTime;
    private final Duration finishingTime;

    private final int totalTasks;
    private final int runningTasks;
    private final int completedTasks;
    private final int failedTasks;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int blockedDrivers;
    private final int completedDrivers;

    private final double cumulativeUserMemory;
    private final double failedCumulativeUserMemory;
    private final DataSize userMemoryReservation;
    private final DataSize revocableMemoryReservation;
    private final DataSize totalMemoryReservation;
    private final DataSize peakUserMemoryReservation;
    private final DataSize peakRevocableMemoryReservation;
    private final DataSize peakTotalMemoryReservation;
    private final DataSize peakTaskUserMemory;
    private final DataSize peakTaskRevocableMemory;
    private final DataSize peakTaskTotalMemory;

    private final boolean scheduled;
    private final Duration totalScheduledTime;
    private final Duration failedScheduledTime;
    private final Duration totalCpuTime;
    private final Duration failedCpuTime;
    private final Duration totalBlockedTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final DataSize physicalInputDataSize;
    private final DataSize failedPhysicalInputDataSize;
    private final long physicalInputPositions;
    private final long failedPhysicalInputPositions;
    private final Duration physicalInputReadTime;
    private final Duration failedPhysicalInputReadTime;

    private final DataSize internalNetworkInputDataSize;
    private final DataSize failedInternalNetworkInputDataSize;
    private final long internalNetworkInputPositions;
    private final long failedInternalNetworkInputPositions;

    private final DataSize rawInputDataSize;
    private final DataSize failedRawInputDataSize;
    private final long rawInputPositions;
    private final long failedRawInputPositions;

    private final DataSize processedInputDataSize;
    private final DataSize failedProcessedInputDataSize;
    private final long processedInputPositions;
    private final long failedProcessedInputPositions;

    private final Duration inputBlockedTime;
    private final Duration failedInputBlockedTime;

    private final DataSize outputDataSize;
    private final DataSize failedOutputDataSize;
    private final long outputPositions;
    private final long failedOutputPositions;

    private final Duration outputBlockedTime;
    private final Duration failedOutputBlockedTime;

    private final DataSize physicalWrittenDataSize;
    private final DataSize failedPhysicalWrittenDataSize;

    private final List<StageGcStatistics> stageGcStatistics;

    private final DynamicFiltersStats dynamicFiltersStats;

    private final List<OperatorStats> operatorSummaries;

    @JsonCreator
    public QueryStats(
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("executionStartTime") DateTime executionStartTime,
            @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
            @JsonProperty("endTime") DateTime endTime,

            @JsonProperty("elapsedTime") Duration elapsedTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("resourceWaitingTime") Duration resourceWaitingTime,
            @JsonProperty("dispatchingTime") Duration dispatchingTime,
            @JsonProperty("executionTime") Duration executionTime,
            @JsonProperty("analysisTime") Duration analysisTime,
            @JsonProperty("planningTime") Duration planningTime,
            @JsonProperty("finishingTime") Duration finishingTime,

            @JsonProperty("totalTasks") int totalTasks,
            @JsonProperty("runningTasks") int runningTasks,
            @JsonProperty("completedTasks") int completedTasks,
            @JsonProperty("failedTasks") int failedTasks,

            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("blockedDrivers") int blockedDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,

            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
            @JsonProperty("failedCumulativeUserMemory") double failedCumulativeUserMemory,
            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("revocableMemoryReservation") DataSize revocableMemoryReservation,
            @JsonProperty("totalMemoryReservation") DataSize totalMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") DataSize peakUserMemoryReservation,
            @JsonProperty("peakRevocableMemoryReservation") DataSize peakRevocableMemoryReservation,
            @JsonProperty("peakTotalMemoryReservation") DataSize peakTotalMemoryReservation,
            @JsonProperty("peakTaskUserMemory") DataSize peakTaskUserMemory,
            @JsonProperty("peakTaskRevocableMemory") DataSize peakTaskRevocableMemory,
            @JsonProperty("peakTaskTotalMemory") DataSize peakTaskTotalMemory,

            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("failedScheduledTime") Duration failedScheduledTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("failedCpuTime") Duration failedCpuTime,
            @JsonProperty("totalBlockedTime") Duration totalBlockedTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,

            @JsonProperty("physicalInputDataSize") DataSize physicalInputDataSize,
            @JsonProperty("failedPhysicalInputDataSize") DataSize failedPhysicalInputDataSize,
            @JsonProperty("physicalInputPositions") long physicalInputPositions,
            @JsonProperty("failedPhysicalInputPositions") long failedPhysicalInputPositions,
            @JsonProperty("physicalInputReadTime") Duration physicalInputReadTime,
            @JsonProperty("failedPhysicalInputReadTime") Duration failedPhysicalInputReadTime,

            @JsonProperty("internalNetworkInputDataSize") DataSize internalNetworkInputDataSize,
            @JsonProperty("failedInternalNetworkInputDataSize") DataSize failedInternalNetworkInputDataSize,
            @JsonProperty("internalNetworkInputPositions") long internalNetworkInputPositions,
            @JsonProperty("failedInternalNetworkInputPositions") long failedInternalNetworkInputPositions,

            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("failedRawInputDataSize") DataSize failedRawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("failedRawInputPositions") long failedRawInputPositions,

            @JsonProperty("processedInputDataSize") DataSize processedInputDataSize,
            @JsonProperty("failedProcessedInputDataSize") DataSize failedProcessedInputDataSize,
            @JsonProperty("processedInputPositions") long processedInputPositions,
            @JsonProperty("failedProcessedInputPositions") long failedProcessedInputPositions,

            @JsonProperty("inputBlockedTime") Duration inputBlockedTime,
            @JsonProperty("failedInputBlockedTime") Duration failedInputBlockedTime,

            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("failedOutputDataSize") DataSize failedOutputDataSize,
            @JsonProperty("outputPositions") long outputPositions,
            @JsonProperty("failedOutputPositions") long failedOutputPositions,

            @JsonProperty("outputBlockedTime") Duration outputBlockedTime,
            @JsonProperty("failedOutputBlockedTime") Duration failedOutputBlockedTime,

            @JsonProperty("physicalWrittenDataSize") DataSize physicalWrittenDataSize,
            @JsonProperty("failedPhysicalWrittenDataSize") DataSize failedPhysicalWrittenDataSize,

            @JsonProperty("stageGcStatistics") List<StageGcStatistics> stageGcStatistics,

            @JsonProperty("dynamicFiltersStats") DynamicFiltersStats dynamicFiltersStats,

            @JsonProperty("operatorSummaries") List<OperatorStats> operatorSummaries)
    {
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.executionStartTime = executionStartTime;
        this.lastHeartbeat = requireNonNull(lastHeartbeat, "lastHeartbeat is null");
        this.endTime = endTime;

        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");
        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.resourceWaitingTime = requireNonNull(resourceWaitingTime, "resourceWaitingTime is null");
        this.dispatchingTime = requireNonNull(dispatchingTime, "dispatchingTime is null");
        this.executionTime = requireNonNull(executionTime, "executionTime is null");
        this.analysisTime = requireNonNull(analysisTime, "analysisTime is null");
        this.planningTime = requireNonNull(planningTime, "planningTime is null");
        this.finishingTime = requireNonNull(finishingTime, "finishingTime is null");

        checkArgument(totalTasks >= 0, "totalTasks is negative");
        this.totalTasks = totalTasks;
        checkArgument(runningTasks >= 0, "runningTasks is negative");
        this.runningTasks = runningTasks;
        checkArgument(completedTasks >= 0, "completedTasks is negative");
        this.completedTasks = completedTasks;
        checkArgument(failedTasks >= 0, "failedTasks is negative");
        this.failedTasks = failedTasks;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(blockedDrivers >= 0, "blockedDrivers is negative");
        this.blockedDrivers = blockedDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;
        checkArgument(cumulativeUserMemory >= 0, "cumulativeUserMemory is negative");
        this.cumulativeUserMemory = cumulativeUserMemory;
        this.failedCumulativeUserMemory = failedCumulativeUserMemory;
        this.userMemoryReservation = requireNonNull(userMemoryReservation, "userMemoryReservation is null");
        this.revocableMemoryReservation = requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null");
        this.totalMemoryReservation = requireNonNull(totalMemoryReservation, "totalMemoryReservation is null");
        this.peakUserMemoryReservation = requireNonNull(peakUserMemoryReservation, "peakUserMemoryReservation is null");
        this.peakRevocableMemoryReservation = requireNonNull(peakRevocableMemoryReservation, "peakRevocableMemoryReservation is null");
        this.peakTotalMemoryReservation = requireNonNull(peakTotalMemoryReservation, "peakTotalMemoryReservation is null");
        this.peakTaskUserMemory = requireNonNull(peakTaskUserMemory, "peakTaskUserMemory is null");
        this.peakTaskRevocableMemory = requireNonNull(peakTaskRevocableMemory, "peakTaskRevocableMemory is null");
        this.peakTaskTotalMemory = requireNonNull(peakTaskTotalMemory, "peakTaskTotalMemory is null");
        this.scheduled = scheduled;
        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.failedScheduledTime = requireNonNull(failedScheduledTime, "failedScheduledTime is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.failedCpuTime = requireNonNull(failedCpuTime, "failedCpuTime is null");
        this.totalBlockedTime = requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.physicalInputDataSize = requireNonNull(physicalInputDataSize, "physicalInputDataSize is null");
        this.failedPhysicalInputDataSize = requireNonNull(failedPhysicalInputDataSize, "failedPhysicalInputDataSize is null");
        checkArgument(physicalInputPositions >= 0, "physicalInputPositions is negative");
        this.physicalInputPositions = physicalInputPositions;
        checkArgument(failedPhysicalInputPositions >= 0, "failedPhysicalInputPositions is negative");
        this.failedPhysicalInputPositions = failedPhysicalInputPositions;
        this.physicalInputReadTime = requireNonNull(physicalInputReadTime, "physicalInputReadTime is null");
        this.failedPhysicalInputReadTime = requireNonNull(failedPhysicalInputReadTime, "failedPhysicalInputReadTime is null");

        this.internalNetworkInputDataSize = requireNonNull(internalNetworkInputDataSize, "internalNetworkInputDataSize is null");
        this.failedInternalNetworkInputDataSize = requireNonNull(failedInternalNetworkInputDataSize, "failedInternalNetworkInputDataSize is null");
        checkArgument(internalNetworkInputPositions >= 0, "internalNetworkInputPositions is negative");
        this.internalNetworkInputPositions = internalNetworkInputPositions;
        checkArgument(failedInternalNetworkInputPositions >= 0, "failedInternalNetworkInputPositions is negative");
        this.failedInternalNetworkInputPositions = failedInternalNetworkInputPositions;

        this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawInputDataSize is null");
        this.failedRawInputDataSize = requireNonNull(failedRawInputDataSize, "failedRawInputDataSize is null");
        checkArgument(rawInputPositions >= 0, "rawInputPositions is negative");
        this.rawInputPositions = rawInputPositions;
        checkArgument(failedRawInputPositions >= 0, "failedRawInputPositions is negative");
        this.failedRawInputPositions = failedRawInputPositions;

        this.processedInputDataSize = requireNonNull(processedInputDataSize, "processedInputDataSize is null");
        this.failedProcessedInputDataSize = requireNonNull(failedProcessedInputDataSize, "failedProcessedInputDataSize is null");
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");
        this.processedInputPositions = processedInputPositions;
        checkArgument(failedProcessedInputPositions >= 0, "failedProcessedInputPositions is negative");
        this.failedProcessedInputPositions = failedProcessedInputPositions;

        this.inputBlockedTime = requireNonNull(inputBlockedTime, "inputBlockedTime is null");
        this.failedInputBlockedTime = requireNonNull(failedInputBlockedTime, "failedInputBlockedTime is null");

        this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");
        this.failedOutputDataSize = requireNonNull(failedOutputDataSize, "failedOutputDataSize is null");
        checkArgument(outputPositions >= 0, "outputPositions is negative");
        this.outputPositions = outputPositions;
        checkArgument(failedOutputPositions >= 0, "failedOutputPositions is negative");
        this.failedOutputPositions = failedOutputPositions;

        this.outputBlockedTime = requireNonNull(outputBlockedTime, "outputBlockedTime is null");
        this.failedOutputBlockedTime = requireNonNull(failedOutputBlockedTime, "failedOutputBlockedTime is null");

        this.physicalWrittenDataSize = requireNonNull(physicalWrittenDataSize, "physicalWrittenDataSize is null");
        this.failedPhysicalWrittenDataSize = requireNonNull(failedPhysicalWrittenDataSize, "failedPhysicalWrittenDataSize is null");

        this.stageGcStatistics = ImmutableList.copyOf(requireNonNull(stageGcStatistics, "stageGcStatistics is null"));

        this.dynamicFiltersStats = requireNonNull(dynamicFiltersStats, "dynamicFiltersStats is null");

        this.operatorSummaries = ImmutableList.copyOf(requireNonNull(operatorSummaries, "operatorSummaries is null"));
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public DateTime getExecutionStartTime()
    {
        return executionStartTime;
    }

    @JsonProperty
    public DateTime getLastHeartbeat()
    {
        return lastHeartbeat;
    }

    @Nullable
    @JsonProperty
    public DateTime getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public Duration getResourceWaitingTime()
    {
        return resourceWaitingTime;
    }

    @JsonProperty
    public Duration getDispatchingTime()
    {
        return dispatchingTime;
    }

    @JsonProperty
    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public Duration getExecutionTime()
    {
        return executionTime;
    }

    @JsonProperty
    public Duration getAnalysisTime()
    {
        return analysisTime;
    }

    @JsonProperty
    public Duration getPlanningTime()
    {
        return planningTime;
    }

    @JsonProperty
    public Duration getFinishingTime()
    {
        return finishingTime;
    }

    @JsonProperty
    public int getTotalTasks()
    {
        return totalTasks;
    }

    @JsonProperty
    public int getFailedTasks()
    {
        return failedTasks;
    }

    @JsonProperty
    public int getRunningTasks()
    {
        return runningTasks;
    }

    @JsonProperty
    public int getCompletedTasks()
    {
        return completedTasks;
    }

    @JsonProperty
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    @JsonProperty
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @JsonProperty
    public int getBlockedDrivers()
    {
        return blockedDrivers;
    }

    @JsonProperty
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @JsonProperty
    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

    @JsonProperty
    public double getFailedCumulativeUserMemory()
    {
        return failedCumulativeUserMemory;
    }

    @JsonProperty
    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    @JsonProperty
    public DataSize getRevocableMemoryReservation()
    {
        return revocableMemoryReservation;
    }

    @JsonProperty
    public DataSize getTotalMemoryReservation()
    {
        return totalMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakUserMemoryReservation()
    {
        return peakUserMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakRevocableMemoryReservation()
    {
        return peakRevocableMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    @JsonProperty
    public DataSize getPeakTaskUserMemory()
    {
        return peakTaskUserMemory;
    }

    @JsonProperty
    public DataSize getPeakTaskRevocableMemory()
    {
        return peakTaskRevocableMemory;
    }

    @JsonProperty
    public DataSize getPeakTaskTotalMemory()
    {
        return peakTaskTotalMemory;
    }

    @JsonProperty
    public boolean isScheduled()
    {
        return scheduled;
    }

    @JsonProperty
    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    @JsonProperty
    public Duration getFailedScheduledTime()
    {
        return failedScheduledTime;
    }

    @JsonProperty
    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    @JsonProperty
    public Duration getFailedCpuTime()
    {
        return failedCpuTime;
    }

    @JsonProperty
    public Duration getTotalBlockedTime()
    {
        return totalBlockedTime;
    }

    @JsonProperty
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    @JsonProperty
    public DataSize getPhysicalInputDataSize()
    {
        return physicalInputDataSize;
    }

    @JsonProperty
    public DataSize getFailedPhysicalInputDataSize()
    {
        return failedPhysicalInputDataSize;
    }

    @JsonProperty
    public long getPhysicalInputPositions()
    {
        return physicalInputPositions;
    }

    @JsonProperty
    public long getFailedPhysicalInputPositions()
    {
        return failedPhysicalInputPositions;
    }

    @JsonProperty
    public Duration getPhysicalInputReadTime()
    {
        return physicalInputReadTime;
    }

    @JsonProperty
    public Duration getFailedPhysicalInputReadTime()
    {
        return failedPhysicalInputReadTime;
    }

    @JsonProperty
    public DataSize getInternalNetworkInputDataSize()
    {
        return internalNetworkInputDataSize;
    }

    @JsonProperty
    public DataSize getFailedInternalNetworkInputDataSize()
    {
        return failedInternalNetworkInputDataSize;
    }

    @JsonProperty
    public long getInternalNetworkInputPositions()
    {
        return internalNetworkInputPositions;
    }

    @JsonProperty
    public long getFailedInternalNetworkInputPositions()
    {
        return failedInternalNetworkInputPositions;
    }

    @JsonProperty
    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @JsonProperty
    public DataSize getFailedRawInputDataSize()
    {
        return failedRawInputDataSize;
    }

    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    public long getFailedRawInputPositions()
    {
        return failedRawInputPositions;
    }

    @JsonProperty
    public DataSize getProcessedInputDataSize()
    {
        return processedInputDataSize;
    }

    @JsonProperty
    public DataSize getFailedProcessedInputDataSize()
    {
        return failedProcessedInputDataSize;
    }

    @JsonProperty
    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    @JsonProperty
    public long getFailedProcessedInputPositions()
    {
        return failedProcessedInputPositions;
    }

    @JsonProperty
    public Duration getInputBlockedTime()
    {
        return inputBlockedTime;
    }

    @JsonProperty
    public Duration getFailedInputBlockedTime()
    {
        return failedInputBlockedTime;
    }

    @JsonProperty
    public DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    public DataSize getFailedOutputDataSize()
    {
        return failedOutputDataSize;
    }

    @JsonProperty
    public long getOutputPositions()
    {
        return outputPositions;
    }

    @JsonProperty
    public long getFailedOutputPositions()
    {
        return failedOutputPositions;
    }

    @JsonProperty
    public Duration getOutputBlockedTime()
    {
        return outputBlockedTime;
    }

    @JsonProperty
    public Duration getFailedOutputBlockedTime()
    {
        return failedOutputBlockedTime;
    }

    @JsonProperty
    public DataSize getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    @JsonProperty
    public DataSize getFailedPhysicalWrittenDataSize()
    {
        return failedPhysicalWrittenDataSize;
    }

    @JsonProperty
    public long getWrittenPositions()
    {
        return operatorSummaries.stream()
                .filter(stats -> stats.getOperatorType().equals(TableWriterOperator.class.getSimpleName()))
                .mapToLong(OperatorStats::getInputPositions)
                .sum();
    }

    @JsonProperty
    public DataSize getLogicalWrittenDataSize()
    {
        return succinctBytes(
                operatorSummaries.stream()
                        .filter(stats -> stats.getOperatorType().equals(TableWriterOperator.class.getSimpleName()))
                        .mapToLong(stats -> stats.getInputDataSize().toBytes())
                        .sum());
    }

    @JsonProperty
    public List<StageGcStatistics> getStageGcStatistics()
    {
        return stageGcStatistics;
    }

    @JsonProperty
    public DynamicFiltersStats getDynamicFiltersStats()
    {
        return dynamicFiltersStats;
    }

    @JsonProperty
    public List<OperatorStats> getOperatorSummaries()
    {
        return operatorSummaries;
    }

    @JsonProperty
    public OptionalDouble getProgressPercentage()
    {
        if (!scheduled || totalDrivers == 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(min(100, (completedDrivers * 100.0) / totalDrivers));
    }

    @JsonProperty
    public DataSize getSpilledDataSize()
    {
        return succinctBytes(operatorSummaries.stream()
                .mapToLong(stats -> stats.getSpilledDataSize().toBytes())
                .sum());
    }
}
