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
import com.google.errorprone.annotations.Immutable;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * This class is a light representation of stage information - {@link StageInfo}.
 * {@link BasicStageInfo} keeps stage statistics as {@link BasicStageStats} that does not contain
 * operator summaries as {@link StageInfo} does. It allows to avoid heavy operation of merging operators stats.
 */
@Immutable
public class BasicStageInfo
{
    private final StageId stageId;
    private final StageState state;
    private final boolean coordinatorOnly;
    private final List<StageId> subStages;
    private final BasicStageStats stageStats;
    private final List<TaskInfo> tasks;

    @JsonCreator
    public BasicStageInfo(
            @JsonProperty("stageId") StageId stageId,
            @JsonProperty("state") StageState state,
            @JsonProperty("coordinatorOnly") boolean coordinatorOnly,
            @JsonProperty("stageStats") BasicStageStats stageStats,
            @JsonProperty("subStages") List<StageId> subStages,
            @JsonProperty("tasks") List<TaskInfo> tasks)
    {
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.state = requireNonNull(state, "state is null");
        this.coordinatorOnly = coordinatorOnly;
        this.subStages = requireNonNull(subStages, "subStages is null");
        this.stageStats = requireNonNull(stageStats, "stageStats is null");
        this.tasks = requireNonNull(tasks, "tasks is null");
    }

    public BasicStageInfo(StageInfo fullStageInfo)
    {
        this(fullStageInfo.getStageId(),
                fullStageInfo.getState(),
                fullStageInfo.isCoordinatorOnly(),
                fullStageInfo.getStageStats().toBasicStageStats(fullStageInfo.getState()),
                fullStageInfo.getSubStages(),
                fullStageInfo.getTasks());
    }

    @JsonProperty
    public StageId getStageId()
    {
        return stageId;
    }

    @JsonProperty
    public StageState getState()
    {
        return state;
    }

    @JsonProperty
    public boolean isCoordinatorOnly()
    {
        return coordinatorOnly;
    }

    @JsonProperty
    public BasicStageStats getStageStats()
    {
        return stageStats;
    }

    @JsonProperty
    public List<StageId> getSubStages()
    {
        return subStages;
    }

    @JsonProperty
    public List<TaskInfo> getTasks()
    {
        return tasks;
    }

    public boolean isFinalStageInfo()
    {
        return state.isDone() && tasks.stream().allMatch(taskInfo -> taskInfo.taskStatus().getState().isDone());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stageId", stageId)
                .add("state", state)
                .toString();
    }

    public BasicStageInfo withSubStages(List<StageId> subStages)
    {
        return new BasicStageInfo(
                stageId,
                state,
                coordinatorOnly,
                stageStats,
                subStages,
                tasks);
    }
}
