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
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.execution.buffer.OutputBufferStatus;

import java.net.URI;
import java.util.List;
import java.util.OptionalInt;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.Duration.succinctDuration;
import static io.trino.execution.DynamicFiltersCollector.INITIAL_DYNAMIC_FILTERS_VERSION;
import static io.trino.execution.TaskState.PLANNED;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public record TaskStatus(
        TaskId taskId,
        long taskInstanceId,
        long version,
        TaskState state,
        URI self,
        String nodeId,
        boolean speculative,
        List<ExecutionFailureInfo> failures,
        int queuedPartitionedDrivers,
        int runningPartitionedDrivers,
        OutputBufferStatus outputBufferStatus,
        DataSize outputDataSize,
        DataSize writerInputDataSize,
        DataSize physicalWrittenDataSize,
        OptionalInt maxWriterCount,
        DataSize memoryReservation,
        DataSize peakMemoryReservation,
        DataSize revocableMemoryReservation,
        long fullGcCount,
        Duration fullGcTime,
        long dynamicFiltersVersion,
        long queuedPartitionedSplitsWeight,
        long runningPartitionedSplitsWeight)
{
    /**
     * Version of task status that can be used to create an initial local task
     * that is always older or equal than any remote task.
     */
    public static final long STARTING_VERSION = 0;

    /**
     * A value larger than any valid value. This value can be used to create
     * a final local task that is always newer than any remote task.
     */
    private static final long MAX_VERSION = Long.MAX_VALUE;

    public TaskStatus
    {
        requireNonNull(taskId, "taskId is null");
        checkState(version >= STARTING_VERSION, "version must be >= STARTING_VERSION");
        requireNonNull(state, "state is null");
        requireNonNull(self, "self is null");
        requireNonNull(nodeId, "nodeId is null");

        checkArgument(queuedPartitionedDrivers >= 0, "queuedPartitionedDrivers must be positive");
        checkArgument(queuedPartitionedSplitsWeight >= 0, "queuedPartitionedSplitsWeight must be positive");
        checkArgument(runningPartitionedDrivers >= 0, "runningPartitionedDrivers must be positive");
        checkArgument(runningPartitionedSplitsWeight >= 0, "runningPartitionedSplitsWeight must be positive");

        requireNonNull(outputBufferStatus, "outputBufferStatus is null");
        requireNonNull(outputDataSize, "outputDataSize is null");

        requireNonNull(writerInputDataSize, "writerInputDataSize is null");
        requireNonNull(physicalWrittenDataSize, "physicalWrittenDataSize is null");
        requireNonNull(maxWriterCount, "maxWriterCount is null");

        requireNonNull(memoryReservation, "memoryReservation is null");
        requireNonNull(peakMemoryReservation, "peakMemoryReservation is null");
        requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null");
        failures = ImmutableList.copyOf(requireNonNull(failures, "failures is null"));

        checkArgument(fullGcCount >= 0, "fullGcCount is negative");
        requireNonNull(fullGcTime, "fullGcTime is null");
        checkArgument(dynamicFiltersVersion >= INITIAL_DYNAMIC_FILTERS_VERSION, "dynamicFiltersVersion must be >= INITIAL_DYNAMIC_FILTERS_VERSION");
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("state", state)
                .toString();
    }

    public static TaskStatus initialTaskStatus(TaskId taskId, URI location, String nodeId, boolean speculative)
    {
        return new TaskStatus(
                taskId,
                0,
                STARTING_VERSION,
                PLANNED,
                location,
                nodeId,
                speculative,
                ImmutableList.of(),
                0,
                0,
                OutputBufferStatus.initial(),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                OptionalInt.empty(),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                DataSize.ofBytes(0),
                0,
                succinctDuration(0, MILLISECONDS),
                INITIAL_DYNAMIC_FILTERS_VERSION,
                0L,
                0L);
    }

    public static TaskStatus failWith(TaskStatus taskStatus, TaskState state, List<ExecutionFailureInfo> exceptions)
    {
        return new TaskStatus(
                taskStatus.taskId(),
                taskStatus.taskInstanceId(),
                MAX_VERSION,
                state,
                taskStatus.self(),
                taskStatus.nodeId(),
                false,
                exceptions,
                taskStatus.queuedPartitionedDrivers(),
                taskStatus.runningPartitionedDrivers(),
                taskStatus.outputBufferStatus(),
                taskStatus.outputDataSize(),
                taskStatus.writerInputDataSize(),
                taskStatus.physicalWrittenDataSize(),
                taskStatus.maxWriterCount(),
                taskStatus.memoryReservation(),
                taskStatus.peakMemoryReservation(),
                taskStatus.revocableMemoryReservation(),
                taskStatus.fullGcCount(),
                taskStatus.fullGcTime(),
                taskStatus.dynamicFiltersVersion(),
                taskStatus.queuedPartitionedSplitsWeight(),
                taskStatus.runningPartitionedSplitsWeight());
    }
}
