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

public class TaskStatus
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

    private final TaskId taskId;
    private final String taskInstanceId;
    private final long version;
    private final TaskState state;
    private final URI self;
    private final String nodeId;
    private final boolean speculative;

    private final int queuedPartitionedDrivers;
    private final long queuedPartitionedSplitsWeight;
    private final int runningPartitionedDrivers;
    private final long runningPartitionedSplitsWeight;
    private final OutputBufferStatus outputBufferStatus;
    private final DataSize writerInputDataSize;
    private final DataSize outputDataSize;
    private final DataSize physicalWrittenDataSize;
    private final OptionalInt maxWriterCount;
    private final DataSize memoryReservation;
    private final DataSize peakMemoryReservation;
    private final DataSize revocableMemoryReservation;

    private final long fullGcCount;
    private final Duration fullGcTime;

    private final List<ExecutionFailureInfo> failures;

    private final long dynamicFiltersVersion;

    @JsonCreator
    public TaskStatus(
            @JsonProperty("taskId") TaskId taskId,
            @JsonProperty("taskInstanceId") String taskInstanceId,
            @JsonProperty("version") long version,
            @JsonProperty("state") TaskState state,
            @JsonProperty("self") URI self,
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("speculative") boolean speculative,
            @JsonProperty("failures") List<ExecutionFailureInfo> failures,
            @JsonProperty("queuedPartitionedDrivers") int queuedPartitionedDrivers,
            @JsonProperty("runningPartitionedDrivers") int runningPartitionedDrivers,
            @JsonProperty("outputBufferStatus") OutputBufferStatus outputBufferStatus,
            @JsonProperty("outputDataSize") DataSize outputDataSize,
            @JsonProperty("writerInputDataSize") DataSize writerInputDataSize,
            @JsonProperty("physicalWrittenDataSize") DataSize physicalWrittenDataSize,
            @JsonProperty("writerCount") OptionalInt maxWriterCount,
            @JsonProperty("memoryReservation") DataSize memoryReservation,
            @JsonProperty("peakMemoryReservation") DataSize peakMemoryReservation,
            @JsonProperty("revocableMemoryReservation") DataSize revocableMemoryReservation,
            @JsonProperty("fullGcCount") long fullGcCount,
            @JsonProperty("fullGcTime") Duration fullGcTime,
            @JsonProperty("dynamicFiltersVersion") long dynamicFiltersVersion,
            @JsonProperty("queuedPartitionedSplitsWeight") long queuedPartitionedSplitsWeight,
            @JsonProperty("runningPartitionedSplitsWeight") long runningPartitionedSplitsWeight)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");

        checkState(version >= STARTING_VERSION, "version must be >= STARTING_VERSION");
        this.version = version;
        this.state = requireNonNull(state, "state is null");
        this.self = requireNonNull(self, "self is null");
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.speculative = speculative;

        checkArgument(queuedPartitionedDrivers >= 0, "queuedPartitionedDrivers must be positive");
        this.queuedPartitionedDrivers = queuedPartitionedDrivers;
        checkArgument(queuedPartitionedSplitsWeight >= 0, "queuedPartitionedSplitsWeight must be positive");
        this.queuedPartitionedSplitsWeight = queuedPartitionedSplitsWeight;

        checkArgument(runningPartitionedDrivers >= 0, "runningPartitionedDrivers must be positive");
        this.runningPartitionedDrivers = runningPartitionedDrivers;
        checkArgument(runningPartitionedSplitsWeight >= 0, "runningPartitionedSplitsWeight must be positive");
        this.runningPartitionedSplitsWeight = runningPartitionedSplitsWeight;

        this.outputBufferStatus = requireNonNull(outputBufferStatus, "outputBufferStatus is null");
        this.outputDataSize = requireNonNull(outputDataSize, "outputDataSize is null");

        this.writerInputDataSize = requireNonNull(writerInputDataSize, "writerInputDataSize is null");
        this.physicalWrittenDataSize = requireNonNull(physicalWrittenDataSize, "physicalWrittenDataSize is null");
        this.maxWriterCount = requireNonNull(maxWriterCount, "maxWriterCount is null");

        this.memoryReservation = requireNonNull(memoryReservation, "memoryReservation is null");
        this.peakMemoryReservation = requireNonNull(peakMemoryReservation, "peakMemoryReservation is null");
        this.revocableMemoryReservation = requireNonNull(revocableMemoryReservation, "revocableMemoryReservation is null");
        this.failures = ImmutableList.copyOf(requireNonNull(failures, "failures is null"));

        checkArgument(fullGcCount >= 0, "fullGcCount is negative");
        this.fullGcCount = fullGcCount;
        this.fullGcTime = requireNonNull(fullGcTime, "fullGcTime is null");
        checkArgument(dynamicFiltersVersion >= INITIAL_DYNAMIC_FILTERS_VERSION, "dynamicFiltersVersion must be >= INITIAL_DYNAMIC_FILTERS_VERSION");
        this.dynamicFiltersVersion = dynamicFiltersVersion;
    }

    @JsonProperty
    public TaskId getTaskId()
    {
        return taskId;
    }

    @JsonProperty
    public String getTaskInstanceId()
    {
        return taskInstanceId;
    }

    @JsonProperty
    public long getVersion()
    {
        return version;
    }

    @JsonProperty
    public TaskState getState()
    {
        return state;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    public String getNodeId()
    {
        return nodeId;
    }

    @JsonProperty
    public boolean isSpeculative()
    {
        return speculative;
    }

    @JsonProperty
    public List<ExecutionFailureInfo> getFailures()
    {
        return failures;
    }

    @JsonProperty
    public int getQueuedPartitionedDrivers()
    {
        return queuedPartitionedDrivers;
    }

    @JsonProperty
    public int getRunningPartitionedDrivers()
    {
        return runningPartitionedDrivers;
    }

    @JsonProperty
    public DataSize getWriterInputDataSize()
    {
        return writerInputDataSize;
    }

    @JsonProperty
    public DataSize getPhysicalWrittenDataSize()
    {
        return physicalWrittenDataSize;
    }

    @JsonProperty
    public OptionalInt getMaxWriterCount()
    {
        return maxWriterCount;
    }

    @JsonProperty
    public OutputBufferStatus getOutputBufferStatus()
    {
        return outputBufferStatus;
    }

    @JsonProperty
    public DataSize getOutputDataSize()
    {
        return outputDataSize;
    }

    @JsonProperty
    public DataSize getMemoryReservation()
    {
        return memoryReservation;
    }

    @JsonProperty
    public DataSize getPeakMemoryReservation()
    {
        return peakMemoryReservation;
    }

    @JsonProperty
    public DataSize getRevocableMemoryReservation()
    {
        return revocableMemoryReservation;
    }

    @JsonProperty
    public long getFullGcCount()
    {
        return fullGcCount;
    }

    @JsonProperty
    public Duration getFullGcTime()
    {
        return fullGcTime;
    }

    @JsonProperty
    public long getDynamicFiltersVersion()
    {
        return dynamicFiltersVersion;
    }

    @JsonProperty
    public long getQueuedPartitionedSplitsWeight()
    {
        return queuedPartitionedSplitsWeight;
    }

    @JsonProperty
    public long getRunningPartitionedSplitsWeight()
    {
        return runningPartitionedSplitsWeight;
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
                "",
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
                taskStatus.getTaskId(),
                taskStatus.getTaskInstanceId(),
                MAX_VERSION,
                state,
                taskStatus.getSelf(),
                taskStatus.getNodeId(),
                false,
                exceptions,
                taskStatus.getQueuedPartitionedDrivers(),
                taskStatus.getRunningPartitionedDrivers(),
                taskStatus.getOutputBufferStatus(),
                taskStatus.getOutputDataSize(),
                taskStatus.getWriterInputDataSize(),
                taskStatus.getPhysicalWrittenDataSize(),
                taskStatus.getMaxWriterCount(),
                taskStatus.getMemoryReservation(),
                taskStatus.getPeakMemoryReservation(),
                taskStatus.getRevocableMemoryReservation(),
                taskStatus.getFullGcCount(),
                taskStatus.getFullGcTime(),
                taskStatus.getDynamicFiltersVersion(),
                taskStatus.getQueuedPartitionedSplitsWeight(),
                taskStatus.getRunningPartitionedSplitsWeight());
    }
}
