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

import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.operator.BlockedReason;

import java.util.HashSet;
import java.util.OptionalDouble;
import java.util.Set;

import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class BasicStageStats
{
    public static final BasicStageStats EMPTY_STAGE_STATS = new BasicStageStats(
            false,

            0,

            0,
            0,
            0,
            0,
            0,

            DataSize.ofBytes(0),
            0,
            new Duration(0, MILLISECONDS),
            DataSize.ofBytes(0),

            DataSize.ofBytes(0),
            0,

            0,

            DataSize.ofBytes(0),

            0,
            0,
            DataSize.ofBytes(0),
            DataSize.ofBytes(0),

            new Duration(0, MILLISECONDS),
            new Duration(0, MILLISECONDS),
            new Duration(0, MILLISECONDS),
            new Duration(0, MILLISECONDS),

            false,
            ImmutableSet.of(),

            OptionalDouble.empty(),
            OptionalDouble.empty());

    private final boolean isScheduled;
    private final int failedTasks;
    private final int totalDrivers;
    private final int queuedDrivers;
    private final int runningDrivers;
    private final int completedDrivers;
    private final int blockedDrivers;
    private final DataSize physicalInputDataSize;
    private final long physicalInputPositions;
    private final Duration physicalInputReadTime;
    private final DataSize physicalWrittenDataSize;
    private final DataSize internalNetworkInputDataSize;
    private final long internalNetworkInputPositions;
    private final long processedInputPositions;
    private final DataSize spilledDataSize;
    private final double cumulativeUserMemory;
    private final double failedCumulativeUserMemory;
    private final DataSize userMemoryReservation;
    private final DataSize totalMemoryReservation;
    private final Duration totalCpuTime;
    private final Duration failedCpuTime;
    private final Duration totalScheduledTime;
    private final Duration failedScheduledTime;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;
    private final OptionalDouble progressPercentage;
    private final OptionalDouble runningPercentage;

    public BasicStageStats(
            boolean isScheduled,

            int failedTasks,

            int totalDrivers,
            int queuedDrivers,
            int runningDrivers,
            int completedDrivers,
            int blockedDrivers,

            DataSize physicalInputDataSize,
            long physicalInputPositions,
            Duration physicalInputReadTime,
            DataSize physicalWrittenDataSize,

            DataSize internalNetworkInputDataSize,
            long internalNetworkInputPositions,

            long processedInputPositions,

            DataSize spilledDataSize,

            double cumulativeUserMemory,
            double failedCumulativeUserMemory,
            DataSize userMemoryReservation,
            DataSize totalMemoryReservation,

            Duration totalCpuTime,
            Duration failedCpuTime,
            Duration totalScheduledTime,
            Duration failedScheduledTime,

            boolean fullyBlocked,
            Set<BlockedReason> blockedReasons,

            OptionalDouble progressPercentage,
            OptionalDouble runningPercentage)
    {
        this.isScheduled = isScheduled;
        this.failedTasks = failedTasks;
        this.totalDrivers = totalDrivers;
        this.queuedDrivers = queuedDrivers;
        this.runningDrivers = runningDrivers;
        this.completedDrivers = completedDrivers;
        this.blockedDrivers = blockedDrivers;
        this.physicalInputDataSize = requireNonNull(physicalInputDataSize, "physicalInputDataSize is null");
        this.physicalInputPositions = physicalInputPositions;
        this.physicalInputReadTime = requireNonNull(physicalInputReadTime, "physicalInputReadTime is null");
        this.physicalWrittenDataSize = requireNonNull(physicalWrittenDataSize, "physicalWrittenDataSize is null");
        this.internalNetworkInputDataSize = requireNonNull(internalNetworkInputDataSize, "internalNetworkInputDataSize is null");
        this.internalNetworkInputPositions = internalNetworkInputPositions;
        this.processedInputPositions = processedInputPositions;
        this.spilledDataSize = requireNonNull(spilledDataSize, "spilledDataSize is null");
        this.cumulativeUserMemory = cumulativeUserMemory;
        this.failedCumulativeUserMemory = failedCumulativeUserMemory;
        this.userMemoryReservation = requireNonNull(userMemoryReservation, "userMemoryReservation is null");
        this.totalMemoryReservation = requireNonNull(totalMemoryReservation, "totalMemoryReservation is null");
        this.totalCpuTime = requireNonNull(totalCpuTime, "totalCpuTime is null");
        this.failedCpuTime = requireNonNull(failedCpuTime, "failedCpuTime is null");
        this.totalScheduledTime = requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        this.failedScheduledTime = requireNonNull(failedScheduledTime, "failedScheduledTime is null");
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));
        this.progressPercentage = requireNonNull(progressPercentage, "progressPercentage is null");
        this.runningPercentage = requireNonNull(runningPercentage, "runningPerentage is null");
    }

    public boolean isScheduled()
    {
        return isScheduled;
    }

    public int getFailedTasks()
    {
        return failedTasks;
    }

    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    public int getBlockedDrivers()
    {
        return blockedDrivers;
    }

    public DataSize getPhysicalInputDataSize()
    {
        return physicalInputDataSize;
    }

    public long getPhysicalInputPositions()
    {
        return physicalInputPositions;
    }

    public Duration getPhysicalInputReadTime()
    {
        return physicalInputReadTime;
    }

    public DataSize getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    public DataSize getInternalNetworkInputDataSize()
    {
        return internalNetworkInputDataSize;
    }

    public long getInternalNetworkInputPositions()
    {
        return internalNetworkInputPositions;
    }

    public long getProcessedInputPositions()
    {
        return processedInputPositions;
    }

    public DataSize getSpilledDataSize()
    {
        return spilledDataSize;
    }

    public double getCumulativeUserMemory()
    {
        return cumulativeUserMemory;
    }

    public double getFailedCumulativeUserMemory()
    {
        return failedCumulativeUserMemory;
    }

    public DataSize getUserMemoryReservation()
    {
        return userMemoryReservation;
    }

    public DataSize getTotalMemoryReservation()
    {
        return totalMemoryReservation;
    }

    public Duration getTotalCpuTime()
    {
        return totalCpuTime;
    }

    public Duration getFailedCpuTime()
    {
        return failedCpuTime;
    }

    public Duration getTotalScheduledTime()
    {
        return totalScheduledTime;
    }

    public Duration getFailedScheduledTime()
    {
        return failedScheduledTime;
    }

    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
    }

    public OptionalDouble getProgressPercentage()
    {
        return progressPercentage;
    }

    public OptionalDouble getRunningPercentage()
    {
        return runningPercentage;
    }

    public static BasicStageStats aggregateBasicStageStats(Iterable<BasicStageStats> stages)
    {
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

        long totalScheduledTimeMillis = 0;
        long failedScheduledTimeMillis = 0;
        long totalCpuTime = 0;
        long failedCpuTime = 0;

        long physicalInputDataSize = 0;
        long physicalInputPositions = 0;
        long physicalInputReadTime = 0;
        long physicalWrittenBytes = 0;

        long internalNetworkInputDataSize = 0;
        long internalNetworkInputPositions = 0;

        long processedInputPositions = 0;
        long spilledDataSize = 0;

        boolean isScheduled = true;

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        for (BasicStageStats stageStats : stages) {
            failedTasks += stageStats.getFailedTasks();

            totalDrivers += stageStats.getTotalDrivers();
            queuedDrivers += stageStats.getQueuedDrivers();
            runningDrivers += stageStats.getRunningDrivers();
            completedDrivers += stageStats.getCompletedDrivers();
            blockedDrivers += stageStats.getBlockedDrivers();

            cumulativeUserMemory += stageStats.getCumulativeUserMemory();
            failedCumulativeUserMemory += stageStats.getFailedCumulativeUserMemory();
            userMemoryReservation += stageStats.getUserMemoryReservation().toBytes();
            totalMemoryReservation += stageStats.getTotalMemoryReservation().toBytes();

            totalScheduledTimeMillis += stageStats.getTotalScheduledTime().roundTo(MILLISECONDS);
            failedScheduledTimeMillis += stageStats.getFailedScheduledTime().roundTo(MILLISECONDS);
            totalCpuTime += stageStats.getTotalCpuTime().roundTo(MILLISECONDS);
            failedCpuTime += stageStats.getFailedCpuTime().roundTo(MILLISECONDS);

            isScheduled &= stageStats.isScheduled();

            fullyBlocked &= stageStats.isFullyBlocked();
            blockedReasons.addAll(stageStats.getBlockedReasons());

            physicalInputDataSize += stageStats.getPhysicalInputDataSize().toBytes();
            physicalInputPositions += stageStats.getPhysicalInputPositions();
            physicalInputReadTime += stageStats.getPhysicalInputReadTime().roundTo(MILLISECONDS);
            physicalWrittenBytes += stageStats.getPhysicalWrittenDataSize().toBytes();

            internalNetworkInputDataSize += stageStats.getInternalNetworkInputDataSize().toBytes();
            internalNetworkInputPositions += stageStats.getInternalNetworkInputPositions();

            processedInputPositions += stageStats.getProcessedInputPositions();
            spilledDataSize += stageStats.getSpilledDataSize().toBytes();
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
                new Duration(physicalInputReadTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                succinctBytes(physicalWrittenBytes),

                succinctBytes(internalNetworkInputDataSize),
                internalNetworkInputPositions,

                processedInputPositions,

                succinctBytes(spilledDataSize),

                cumulativeUserMemory,
                failedCumulativeUserMemory,
                succinctBytes(userMemoryReservation),
                succinctBytes(totalMemoryReservation),

                new Duration(totalCpuTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(failedCpuTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalScheduledTimeMillis, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(failedScheduledTimeMillis, MILLISECONDS).convertToMostSuccinctTimeUnit(),

                fullyBlocked,
                blockedReasons,

                progressPercentage,
                runningPercentage);
    }
}
