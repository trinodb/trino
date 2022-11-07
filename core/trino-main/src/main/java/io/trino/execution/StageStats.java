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
import io.airlift.stats.Distribution;
import io.airlift.stats.Distribution.DistributionSnapshot;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.operator.BlockedReason;
import io.trino.operator.OperatorStats;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.spi.eventlistener.StageGcStatistics;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.trino.execution.StageState.RUNNING;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@Immutable
public class StageStats
{
    private final DateTime schedulingComplete;

    private final DistributionSnapshot getSplitDistribution;

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

    private final DataSize bufferedDataSize;
    private final Optional<TDigestHistogram> outputBufferUtilization;
    private final DataSize outputDataSize;
    private final DataSize failedOutputDataSize;
    private final long outputPositions;
    private final long failedOutputPositions;

    private final Duration outputBlockedTime;
    private final Duration failedOutputBlockedTime;

    private final DataSize physicalWrittenDataSize;
    private final DataSize failedPhysicalWrittenDataSize;

    private final StageGcStatistics gcInfo;

    private final List<OperatorStats> operatorSummaries;

    @JsonCreator
    public StageStats(
            @JsonProperty("schedulingComplete") DateTime schedulingComplete,

            @JsonProperty("getSplitDistribution") DistributionSnapshot getSplitDistribution,

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

            @JsonProperty("bufferedDataSize") DataSize bufferedDataSize,
            @JsonProperty("outputBufferUtilization") Optional<TDigestHistogram> outputBufferUtilization,
            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("failedOutputDataSize") DataSize failedOutputDataSize,
            @JsonProperty("outputPositions") long outputPositions,
            @JsonProperty("failedOutputPositions") long failedOutputPositions,

            @JsonProperty("outputBlockedTime") Duration outputBlockedTime,
            @JsonProperty("failedOutputBlockedTime") Duration failedOutputBlockedTime,

            @JsonProperty("physicalWrittenDataSize") DataSize physicalWrittenDataSize,
            @JsonProperty("failedPhysicalWrittenDataSize") DataSize failedPhysicalWrittenDataSize,

            @JsonProperty("gcInfo") StageGcStatistics gcInfo,

            @JsonProperty("operatorSummaries") List<OperatorStats> operatorSummaries)
    {
        this.schedulingComplete = schedulingComplete;
        this.getSplitDistribution = requireNonNull(getSplitDistribution, "getSplitDistribution is null");

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

        this.bufferedDataSize = requireNonNull(bufferedDataSize, "bufferedDataSize is null");
        this.outputBufferUtilization = requireNonNull(outputBufferUtilization, "outputBufferUtilization is null");
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

        this.gcInfo = requireNonNull(gcInfo, "gcInfo is null");

        this.operatorSummaries = ImmutableList.copyOf(requireNonNull(operatorSummaries, "operatorSummaries is null"));
    }

    @JsonProperty
    public DateTime getSchedulingComplete()
    {
        return schedulingComplete;
    }

    @JsonProperty
    public DistributionSnapshot getGetSplitDistribution()
    {
        return getSplitDistribution;
    }

    @JsonProperty
    public int getTotalTasks()
    {
        return totalTasks;
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
    public int getFailedTasks()
    {
        return failedTasks;
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
    public DataSize getBufferedDataSize()
    {
        return bufferedDataSize;
    }

    @JsonProperty
    public Optional<TDigestHistogram> getOutputBufferUtilization()
    {
        return outputBufferUtilization;
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
    public StageGcStatistics getGcInfo()
    {
        return gcInfo;
    }

    @JsonProperty
    public List<OperatorStats> getOperatorSummaries()
    {
        return operatorSummaries;
    }

    public BasicStageStats toBasicStageStats(StageState stageState)
    {
        boolean isScheduled = stageState == RUNNING || stageState == StageState.PENDING || stageState.isDone();

        OptionalDouble progressPercentage = OptionalDouble.empty();
        if (isScheduled && totalDrivers != 0) {
            progressPercentage = OptionalDouble.of(min(100, (completedDrivers * 100.0) / totalDrivers));
        }

        return new BasicStageStats(
                isScheduled,
                failedTasks,
                totalDrivers,
                queuedDrivers,
                runningDrivers,
                completedDrivers,
                physicalInputDataSize,
                physicalInputPositions,
                physicalInputReadTime,
                internalNetworkInputDataSize,
                internalNetworkInputPositions,
                rawInputDataSize,
                rawInputPositions,
                (long) cumulativeUserMemory,
                (long) failedCumulativeUserMemory,
                userMemoryReservation,
                totalMemoryReservation,
                totalCpuTime,
                failedCpuTime,
                totalScheduledTime,
                failedScheduledTime,
                fullyBlocked,
                blockedReasons,
                progressPercentage);
    }

    public static StageStats createInitial()
    {
        DataSize zeroBytes = DataSize.of(0, BYTE);
        Duration zeroSeconds = new Duration(0, SECONDS);
        return new StageStats(
                null,
                new Distribution().snapshot(),
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                zeroBytes,
                zeroBytes,
                zeroBytes,
                zeroBytes,
                zeroBytes,
                zeroSeconds,
                zeroSeconds,
                zeroSeconds,
                zeroSeconds,
                zeroSeconds,
                false,
                ImmutableSet.of(),
                zeroBytes,
                zeroBytes,
                0,
                0,
                zeroSeconds,
                zeroSeconds,
                zeroBytes,
                zeroBytes,
                0,
                0,
                zeroBytes,
                zeroBytes,
                0,
                0,
                zeroBytes,
                zeroBytes,
                0,
                0,
                zeroSeconds,
                zeroSeconds,
                zeroBytes,
                Optional.empty(),
                zeroBytes,
                zeroBytes,
                0,
                0,
                zeroSeconds,
                zeroSeconds,
                zeroBytes,
                zeroBytes,
                new StageGcStatistics(
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0),
                ImmutableList.of());
    }
}
