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
package io.trino.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.QueryStats;
import io.trino.operator.BlockedReason;

import java.time.Instant;
import java.util.OptionalDouble;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Lightweight version of QueryStats. Parts of the web UI depend on the fields
 * being named consistently across these classes.
 */
@Immutable
public class BasicQueryStats
{
    private final Instant createTime;
    private final Instant endTime;

    private final Duration queuedTime;
    private final Duration elapsedTime;
    private final Duration executionTime;

    private final int failedTasks;

    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int completedDrivers;
    private final int blockedDrivers;

    private final DataSize rawInputDataSize;
    private final long rawInputPositions;
    private final DataSize spilledDataSize;
    private final DataSize physicalInputDataSize;
    private final DataSize physicalWrittenDataSize;
    private final DataSize internalNetworkInputDataSize;
    private final double cumulativeUserMemory;
    private final double failedCumulativeUserMemory;
    private final DataSize userMemoryReservation;
    private final DataSize totalMemoryReservation;
    private final DataSize peakUserMemoryReservation;
    private final DataSize peakTotalMemoryReservation;
    private final Duration planningTime;
    private final Duration analysisTime;
    private final Duration totalCpuTime;
    private final Duration failedCpuTime;
    private final Duration totalScheduledTime;
    private final Duration failedScheduledTime;
    private final Duration finishingTime;
    private final Duration physicalInputReadTime;

    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;

    private final OptionalDouble progressPercentage;
    private final OptionalDouble runningPercentage;

    @JsonCreator
    public BasicQueryStats(
            @JsonProperty("createTime") Instant createTime,
            @JsonProperty("endTime") Instant endTime,
            @JsonProperty("queuedTime") Duration queuedTime,
            @JsonProperty("elapsedTime") Duration elapsedTime,
            @JsonProperty("executionTime") Duration executionTime,
            @JsonProperty("failedTasks") int failedTasks,
            @JsonProperty("totalDrivers") int totalDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,
            @JsonProperty("blockedDrivers") int blockedDrivers,
            @JsonProperty("rawInputDataSize") DataSize rawInputDataSize,
            @JsonProperty("rawInputPositions") long rawInputPositions,
            @JsonProperty("spilledDataSize") DataSize spilledDataSize,
            @JsonProperty("physicalInputDataSize") DataSize physicalInputDataSize,
            @JsonProperty("physicalWrittenDataSize") DataSize physicalWrittenDataSize,
            @JsonProperty("internalNetworkInputDataSize") DataSize internalNetworkInputDataSize,
            @JsonProperty("cumulativeUserMemory") double cumulativeUserMemory,
            @JsonProperty("failedCumulativeUserMemory") double failedCumulativeUserMemory,
            @JsonProperty("userMemoryReservation") DataSize userMemoryReservation,
            @JsonProperty("totalMemoryReservation") DataSize totalMemoryReservation,
            @JsonProperty("peakUserMemoryReservation") DataSize peakUserMemoryReservation,
            @JsonProperty("peakTotalMemoryReservation") DataSize peakTotalMemoryReservation,
            @JsonProperty("planningTime") Duration planningTime,
            @JsonProperty("analysisTime") Duration analysisTime,
            @JsonProperty("totalCpuTime") Duration totalCpuTime,
            @JsonProperty("failedCpuTime") Duration failedCpuTime,
            @JsonProperty("totalScheduledTime") Duration totalScheduledTime,
            @JsonProperty("failedScheduledTime") Duration failedScheduledTime,
            @JsonProperty("finishingTime") Duration finishingTime,
            @JsonProperty("physicalInputReadTime") Duration physicalInputReadTime,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,
            @JsonProperty("progressPercentage") OptionalDouble progressPercentage,
            @JsonProperty("runningPercentage") OptionalDouble runningPercentage)
    {
        this.createTime = createTime;
        this.endTime = endTime;

        this.queuedTime = requireNonNull(queuedTime, "queuedTime is null");
        this.elapsedTime = requireNonNull(elapsedTime, "elapsedTime is null");
        this.executionTime = requireNonNull(executionTime, "executionTime is null");

        checkArgument(failedTasks >= 0, "failedTasks is negative");
        this.failedTasks = failedTasks;

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        this.totalDrivers = totalDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        this.queuedDrivers = queuedDrivers;
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        this.runningDrivers = runningDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");
        this.completedDrivers = completedDrivers;
        checkArgument(blockedDrivers >= 0, "blockedDrivers is negative");
        this.blockedDrivers = blockedDrivers;

        this.rawInputDataSize = requireNonNull(rawInputDataSize, "rawInputDataSize is null");
        this.rawInputPositions = rawInputPositions;
        this.spilledDataSize = spilledDataSize;
        this.physicalInputDataSize = physicalInputDataSize;
        this.physicalWrittenDataSize = physicalWrittenDataSize;
        this.internalNetworkInputDataSize = internalNetworkInputDataSize;

        this.cumulativeUserMemory = cumulativeUserMemory;
        this.failedCumulativeUserMemory = failedCumulativeUserMemory;
        this.userMemoryReservation = userMemoryReservation;
        this.totalMemoryReservation = totalMemoryReservation;
        this.peakUserMemoryReservation = peakUserMemoryReservation;
        this.peakTotalMemoryReservation = peakTotalMemoryReservation;
        this.planningTime = planningTime;
        this.analysisTime = analysisTime;
        this.totalCpuTime = totalCpuTime;
        this.failedCpuTime = failedCpuTime;
        this.totalScheduledTime = totalScheduledTime;
        this.failedScheduledTime = failedScheduledTime;
        this.finishingTime = finishingTime;
        this.physicalInputReadTime = physicalInputReadTime;

        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        this.progressPercentage = requireNonNull(progressPercentage, "progressPercentage is null");
        this.runningPercentage = requireNonNull(runningPercentage, "runningPercentage is null");
    }

    public BasicQueryStats(QueryStats queryStats)
    {
        this(queryStats.getCreateTime(),
                queryStats.getEndTime(),
                queryStats.getQueuedTime(),
                queryStats.getElapsedTime(),
                queryStats.getExecutionTime(),
                queryStats.getFailedTasks(),
                queryStats.getTotalDrivers(),
                queryStats.getQueuedDrivers(),
                queryStats.getRunningDrivers(),
                queryStats.getCompletedDrivers(),
                queryStats.getBlockedDrivers(),
                queryStats.getRawInputDataSize(),
                queryStats.getRawInputPositions(),
                queryStats.getSpilledDataSize(),
                queryStats.getPhysicalInputDataSize(),
                queryStats.getPhysicalWrittenDataSize(),
                queryStats.getInternalNetworkInputDataSize(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getFailedCumulativeUserMemory(),
                queryStats.getUserMemoryReservation(),
                queryStats.getTotalMemoryReservation(),
                queryStats.getPeakUserMemoryReservation(),
                queryStats.getPeakTotalMemoryReservation(),
                queryStats.getPlanningTime(),
                queryStats.getAnalysisTime(),
                queryStats.getTotalCpuTime(),
                queryStats.getFailedCpuTime(),
                queryStats.getTotalScheduledTime(),
                queryStats.getFailedScheduledTime(),
                queryStats.getFinishingTime(),
                queryStats.getPhysicalInputReadTime(),
                queryStats.isFullyBlocked(),
                queryStats.getBlockedReasons(),
                queryStats.getProgressPercentage(),
                queryStats.getRunningPercentage());
    }

    public static BasicQueryStats immediateFailureQueryStats()
    {
        Instant now = Instant.now();
        return new BasicQueryStats(
                now,
                now,
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                0,
                0,
                0,
                0,
                0,
                0,
                DataSize.ofBytes(0),
                0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                0,
                0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                new Duration(0, MILLISECONDS),
                false,
                ImmutableSet.of(),
                OptionalDouble.empty(),
                OptionalDouble.empty());
    }

    @JsonProperty
    public Instant getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public Instant getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public Duration getQueuedTime()
    {
        return queuedTime;
    }

    @JsonProperty
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public Duration getExecutionTime()
    {
        return executionTime;
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
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @JsonProperty
    public int getBlockedDrivers()
    {
        return blockedDrivers;
    }

    @JsonProperty
    public DataSize getRawInputDataSize()
    {
        return rawInputDataSize;
    }

    @JsonProperty
    public long getRawInputPositions()
    {
        return rawInputPositions;
    }

    @JsonProperty
    public DataSize getSpilledDataSize()
    {
        return spilledDataSize;
    }

    @JsonProperty
    public DataSize getPhysicalInputDataSize()
    {
        return physicalInputDataSize;
    }

    @JsonProperty
    public DataSize getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    @JsonProperty
    public DataSize getInternalNetworkInputDataSize()
    {
        return internalNetworkInputDataSize;
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
    public DataSize getPeakTotalMemoryReservation()
    {
        return peakTotalMemoryReservation;
    }

    @JsonProperty
    public Duration getPlanningTime()
    {
        return planningTime;
    }

    @JsonProperty
    public Duration getAnalysisTime()
    {
        return analysisTime;
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
    public Duration getFinishingTime()
    {
        return finishingTime;
    }

    @JsonProperty
    public Duration getPhysicalInputReadTime()
    {
        return physicalInputReadTime;
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
    public OptionalDouble getProgressPercentage()
    {
        return progressPercentage;
    }

    @JsonProperty
    public OptionalDouble getRunningPercentage()
    {
        return runningPercentage;
    }
}
