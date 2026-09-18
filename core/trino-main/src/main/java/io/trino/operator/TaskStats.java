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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import jakarta.annotation.Nullable;

import java.time.Instant;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public record TaskStats(
        Instant createTime,
        @Nullable Instant firstStartTime,
        @Nullable Instant lastStartTime,
        @Nullable Instant terminatingStartTime,
        @Nullable Instant lastEndTime,
        @Nullable Instant endTime,
        Duration elapsedTime,
        Duration queuedTime,
        int totalDrivers,
        int queuedDrivers,
        int queuedPartitionedDrivers,
        long queuedPartitionedSplitsWeight,
        int runningDrivers,
        int runningPartitionedDrivers,
        long runningPartitionedSplitsWeight,
        int blockedDrivers,
        int completedDrivers,
        double cumulativeUserMemory,
        DataSize userMemoryReservation,
        DataSize peakUserMemoryReservation,
        DataSize revocableMemoryReservation,
        DataSize spilledDataSize,
        Duration totalScheduledTime,
        Duration totalCpuTime,
        Duration totalBlockedTime,
        boolean fullyBlocked,
        Set<BlockedReason> blockedReasons,
        DataSize physicalInputDataSize,
        long physicalInputPositions,
        Duration physicalInputReadTime,
        DataSize internalNetworkInputDataSize,
        long internalNetworkInputPositions,
        DataSize processedInputDataSize,
        long processedInputPositions,
        Duration inputBlockedTime,
        DataSize outputDataSize,
        long outputPositions,
        Duration outputBlockedTime,
        DataSize writerInputDataSize,
        DataSize physicalWrittenDataSize,
        OptionalInt maxWriterCount,
        int fullGcCount,
        Duration fullGcTime,
        List<PipelineStats> pipelines)
{
    public TaskStats(Instant createTime, Instant endTime)
    {
        this(createTime,
                null,
                null,
                null,
                null,
                endTime,
                succinctDuration(0, MILLISECONDS),
                succinctDuration(0, MILLISECONDS),
                0,
                0,
                0,
                0L,
                0,
                0,
                0L,
                0,
                0,
                0.0,
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                succinctDuration(0, MILLISECONDS),
                succinctDuration(0, MILLISECONDS),
                succinctDuration(0, MILLISECONDS),
                false,
                ImmutableSet.of(),
                DataSize.ofBytes(0),
                0,
                succinctDuration(0, MILLISECONDS),
                DataSize.ofBytes(0),
                0,
                DataSize.ofBytes(0),
                0,
                succinctDuration(0, MILLISECONDS),
                DataSize.ofBytes(0),
                0,
                succinctDuration(0, MILLISECONDS),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                OptionalInt.empty(),
                0,
                succinctDuration(0, MILLISECONDS),
                ImmutableList.of());
    }

    public TaskStats
    {
        requireNonNull(createTime, "createTime is null");
        requireNonNull(elapsedTime, "elapsedTime is null");
        requireNonNull(queuedTime, "queuedTime is null");

        checkArgument(totalDrivers >= 0, "totalDrivers is negative");
        checkArgument(queuedDrivers >= 0, "queuedDrivers is negative");
        checkArgument(queuedPartitionedDrivers >= 0, "queuedPartitionedDrivers is negative");
        checkArgument(queuedPartitionedSplitsWeight >= 0, "queuedPartitionedSplitsWeight must be positive");
        checkArgument(runningDrivers >= 0, "runningDrivers is negative");
        checkArgument(runningPartitionedDrivers >= 0, "runningPartitionedDrivers is negative");
        checkArgument(runningPartitionedSplitsWeight >= 0, "runningPartitionedSplitsWeight must be positive");
        checkArgument(blockedDrivers >= 0, "blockedDrivers is negative");
        checkArgument(completedDrivers >= 0, "completedDrivers is negative");

        requireNonNull(userMemoryReservation, "userMemoryReservation is null");
        requireNonNull(peakUserMemoryReservation, "peakUserMemoryReservation is null");
        requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null");

        requireNonNull(spilledDataSize, "spilledDataSize is null");

        requireNonNull(totalScheduledTime, "totalScheduledTime is null");
        requireNonNull(totalCpuTime, "totalCpuTime is null");
        requireNonNull(totalBlockedTime, "totalBlockedTime is null");
        blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));

        requireNonNull(physicalInputDataSize, "physicalInputDataSize is null");
        checkArgument(physicalInputPositions >= 0, "physicalInputPositions is negative");
        requireNonNull(physicalInputReadTime, "physicalInputReadTime is null");

        requireNonNull(internalNetworkInputDataSize, "internalNetworkInputDataSize is null");
        checkArgument(internalNetworkInputPositions >= 0, "internalNetworkInputPositions is negative");

        requireNonNull(processedInputDataSize, "processedInputDataSize is null");
        checkArgument(processedInputPositions >= 0, "processedInputPositions is negative");

        requireNonNull(inputBlockedTime, "inputBlockedTime is null");

        requireNonNull(outputDataSize, "outputDataSize is null");
        checkArgument(outputPositions >= 0, "outputPositions is negative");

        requireNonNull(outputBlockedTime, "outputBlockedTime is null");

        requireNonNull(writerInputDataSize, "writerInputDataSize is null");
        requireNonNull(physicalWrittenDataSize, "physicalWrittenDataSize is null");
        requireNonNull(maxWriterCount, "writerCount is null");

        checkArgument(fullGcCount >= 0, "fullGcCount is negative");
        requireNonNull(fullGcTime, "fullGcTime is null");

        pipelines = ImmutableList.copyOf(requireNonNull(pipelines, "pipelines is null"));
    }

    public TaskStats summarize()
    {
        return new TaskStats(
                createTime,
                firstStartTime,
                lastStartTime,
                terminatingStartTime,
                lastEndTime,
                endTime,
                elapsedTime,
                queuedTime,
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                queuedPartitionedSplitsWeight,
                runningDrivers,
                runningPartitionedDrivers,
                runningPartitionedSplitsWeight,
                blockedDrivers,
                completedDrivers,
                cumulativeUserMemory,
                userMemoryReservation,
                peakUserMemoryReservation,
                revocableMemoryReservation,
                spilledDataSize,
                totalScheduledTime,
                totalCpuTime,
                totalBlockedTime,
                fullyBlocked,
                blockedReasons,
                physicalInputDataSize,
                physicalInputPositions,
                physicalInputReadTime,
                internalNetworkInputDataSize,
                internalNetworkInputPositions,
                processedInputDataSize,
                processedInputPositions,
                inputBlockedTime,
                outputDataSize,
                outputPositions,
                outputBlockedTime,
                writerInputDataSize,
                physicalWrittenDataSize,
                maxWriterCount,
                fullGcCount,
                fullGcTime,
                ImmutableList.of());
    }

    public TaskStats summarizeFinal()
    {
        return new TaskStats(
                createTime,
                firstStartTime,
                lastStartTime,
                terminatingStartTime,
                lastEndTime,
                endTime,
                elapsedTime,
                queuedTime,
                totalDrivers,
                queuedDrivers,
                queuedPartitionedDrivers,
                queuedPartitionedSplitsWeight,
                runningDrivers,
                runningPartitionedDrivers,
                runningPartitionedSplitsWeight,
                blockedDrivers,
                completedDrivers,
                cumulativeUserMemory,
                userMemoryReservation,
                peakUserMemoryReservation,
                revocableMemoryReservation,
                spilledDataSize,
                totalScheduledTime,
                totalCpuTime,
                totalBlockedTime,
                fullyBlocked,
                blockedReasons,
                physicalInputDataSize,
                physicalInputPositions,
                physicalInputReadTime,
                internalNetworkInputDataSize,
                internalNetworkInputPositions,
                processedInputDataSize,
                processedInputPositions,
                inputBlockedTime,
                outputDataSize,
                outputPositions,
                outputBlockedTime,
                writerInputDataSize,
                physicalWrittenDataSize,
                maxWriterCount,
                fullGcCount,
                fullGcTime,
                summarizePipelineStats(pipelines));
    }

    private static List<PipelineStats> summarizePipelineStats(List<PipelineStats> pipelines)
    {
        // Use an exact size ImmutableList builder to avoid a redundant copy in the TaskStats constructor
        ImmutableList.Builder<PipelineStats> results = ImmutableList.builderWithExpectedSize(pipelines.size());
        for (PipelineStats pipeline : pipelines) {
            results.add(pipeline.summarize());
        }
        return results.build();
    }
}
