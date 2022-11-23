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
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.execution.buffer.OutputBufferInfo;
import io.trino.execution.buffer.PipelinedBufferInfo;
import io.trino.operator.TaskStats;
import io.trino.sql.planner.plan.PlanNodeId;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.execution.TaskStatus.initialTaskStatus;
import static io.trino.execution.buffer.BufferState.OPEN;
import static java.util.Objects.requireNonNull;

@Immutable
public class TaskInfo
{
    private final TaskStatus taskStatus;
    private final DateTime lastHeartbeat;
    private final OutputBufferInfo outputBuffers;
    private final Set<PlanNodeId> noMoreSplits;
    private final TaskStats stats;
    private final Optional<DataSize> estimatedMemory; // filled in on coordinator

    private final boolean needsPlan;

    @JsonCreator
    public TaskInfo(@JsonProperty("taskStatus") TaskStatus taskStatus,
            @JsonProperty("lastHeartbeat") DateTime lastHeartbeat,
            @JsonProperty("outputBuffers") OutputBufferInfo outputBuffers,
            @JsonProperty("noMoreSplits") Set<PlanNodeId> noMoreSplits,
            @JsonProperty("stats") TaskStats stats,
            @JsonProperty("estimatedMemory") Optional<DataSize> estimatedMemory,
            @JsonProperty("needsPlan") boolean needsPlan)
    {
        this.taskStatus = requireNonNull(taskStatus, "taskStatus is null");
        this.lastHeartbeat = requireNonNull(lastHeartbeat, "lastHeartbeat is null");
        this.outputBuffers = requireNonNull(outputBuffers, "outputBuffers is null");
        this.noMoreSplits = requireNonNull(noMoreSplits, "noMoreSplits is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.estimatedMemory = requireNonNull(estimatedMemory, "estimatedMemory is null");

        this.needsPlan = needsPlan;
    }

    @JsonProperty
    public TaskStatus getTaskStatus()
    {
        return taskStatus;
    }

    @JsonProperty
    public DateTime getLastHeartbeat()
    {
        return lastHeartbeat;
    }

    @JsonProperty
    public OutputBufferInfo getOutputBuffers()
    {
        return outputBuffers;
    }

    @JsonProperty
    public Set<PlanNodeId> getNoMoreSplits()
    {
        return noMoreSplits;
    }

    @JsonProperty
    public TaskStats getStats()
    {
        return stats;
    }

    @JsonProperty
    public Optional<DataSize> getEstimatedMemory()
    {
        return estimatedMemory;
    }

    @JsonProperty
    public boolean isNeedsPlan()
    {
        return needsPlan;
    }

    public TaskInfo summarize()
    {
        if (taskStatus.getState().isDone()) {
            return new TaskInfo(taskStatus, lastHeartbeat, outputBuffers.summarizeFinal(), noMoreSplits, stats.summarizeFinal(), estimatedMemory, needsPlan);
        }
        return new TaskInfo(taskStatus, lastHeartbeat, outputBuffers.summarize(), noMoreSplits, stats.summarize(), estimatedMemory, needsPlan);
    }

    public TaskInfo pruneSpoolingOutputStats()
    {
        return new TaskInfo(taskStatus, lastHeartbeat, outputBuffers.pruneSpoolingOutputStats(), noMoreSplits, stats, estimatedMemory, needsPlan);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskStatus.getTaskId())
                .add("state", taskStatus.getState())
                .toString();
    }

    public static TaskInfo createInitialTask(TaskId taskId, URI location, String nodeId, Optional<List<PipelinedBufferInfo>> pipelinedBufferStates, TaskStats taskStats)
    {
        return new TaskInfo(
                initialTaskStatus(taskId, location, nodeId),
                DateTime.now(),
                new OutputBufferInfo(
                        "UNINITIALIZED",
                        OPEN,
                        true,
                        true,
                        0,
                        0,
                        0,
                        0,
                        pipelinedBufferStates,
                        Optional.empty(),
                        Optional.empty()),
                ImmutableSet.of(),
                taskStats,
                Optional.empty(),
                true);
    }

    public TaskInfo withTaskStatus(TaskStatus newTaskStatus)
    {
        return new TaskInfo(newTaskStatus, lastHeartbeat, outputBuffers, noMoreSplits, stats, estimatedMemory, needsPlan);
    }

    public TaskInfo withEstimatedMemory(DataSize estimatedMemory)
    {
        return new TaskInfo(taskStatus, lastHeartbeat, outputBuffers, noMoreSplits, stats, Optional.of(estimatedMemory), needsPlan);
    }
}
