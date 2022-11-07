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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.QueryId;
import io.trino.spi.type.Type;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanNodeId;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class StageInfo
{
    private final StageId stageId;
    private final StageState state;
    private final PlanFragment plan;
    private final boolean coordinatorOnly;
    private final List<Type> types;
    private final StageStats stageStats;
    private final List<TaskInfo> tasks;
    private final List<StageInfo> subStages;
    private final ExecutionFailureInfo failureCause;
    private final Map<PlanNodeId, TableInfo> tables;

    @JsonCreator
    public StageInfo(
            @JsonProperty("stageId") StageId stageId,
            @JsonProperty("state") StageState state,
            @JsonProperty("plan") @Nullable PlanFragment plan,
            @JsonProperty("coordinatorOnly") boolean coordinatorOnly,
            @JsonProperty("types") List<Type> types,
            @JsonProperty("stageStats") StageStats stageStats,
            @JsonProperty("tasks") List<TaskInfo> tasks,
            @JsonProperty("subStages") List<StageInfo> subStages,
            @JsonProperty("tables") Map<PlanNodeId, TableInfo> tables,
            @JsonProperty("failureCause") ExecutionFailureInfo failureCause)
    {
        requireNonNull(stageId, "stageId is null");
        requireNonNull(state, "state is null");
        requireNonNull(stageStats, "stageStats is null");
        requireNonNull(tasks, "tasks is null");
        requireNonNull(subStages, "subStages is null");
        requireNonNull(tables, "tables is null");

        this.stageId = stageId;
        this.state = state;
        this.plan = plan;
        this.coordinatorOnly = coordinatorOnly;
        this.types = types;
        this.stageStats = stageStats;
        this.tasks = ImmutableList.copyOf(tasks);
        this.subStages = subStages;
        this.failureCause = failureCause;
        this.tables = ImmutableMap.copyOf(tables);
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
    @Nullable
    public PlanFragment getPlan()
    {
        return plan;
    }

    @JsonProperty
    public boolean isCoordinatorOnly()
    {
        return coordinatorOnly;
    }

    @JsonProperty
    public List<Type> getTypes()
    {
        return types;
    }

    @JsonProperty
    public StageStats getStageStats()
    {
        return stageStats;
    }

    @JsonProperty
    public List<TaskInfo> getTasks()
    {
        return tasks;
    }

    @JsonProperty
    public List<StageInfo> getSubStages()
    {
        return subStages;
    }

    @JsonProperty
    public Map<PlanNodeId, TableInfo> getTables()
    {
        return tables;
    }

    @JsonProperty
    public ExecutionFailureInfo getFailureCause()
    {
        return failureCause;
    }

    public boolean isFinalStageInfo()
    {
        return state.isDone() && tasks.stream().allMatch(taskInfo -> taskInfo.getTaskStatus().getState().isDone());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stageId", stageId)
                .add("state", state)
                .toString();
    }

    public StageInfo withSubStages(List<StageInfo> subStages)
    {
        return new StageInfo(
                stageId,
                state,
                plan,
                coordinatorOnly,
                types,
                stageStats,
                tasks,
                subStages,
                tables,
                failureCause);
    }

    public static StageInfo createInitial(QueryId queryId, StageState state, PlanFragment fragment)
    {
        return new StageInfo(
                StageId.create(queryId, fragment.getId()),
                state,
                fragment,
                fragment.getPartitioning().isCoordinatorOnly(),
                fragment.getTypes(),
                StageStats.createInitial(),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                null);
    }

    public static List<StageInfo> getAllStages(Optional<StageInfo> stageInfo)
    {
        if (stageInfo.isEmpty()) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<StageInfo> collector = ImmutableList.builder();
        addAllStages(stageInfo.get(), collector);
        return collector.build();
    }

    private static void addAllStages(@Nullable StageInfo stage, ImmutableList.Builder<StageInfo> collector)
    {
        if (stage != null) {
            collector.add(stage);
            for (StageInfo subStage : stage.getSubStages()) {
                addAllStages(subStage, collector);
            }
        }
    }
}
