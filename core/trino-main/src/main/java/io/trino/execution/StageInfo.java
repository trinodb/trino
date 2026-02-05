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
import com.google.common.collect.ImmutableMap;
import io.trino.spi.QueryId;
import io.trino.spi.type.Type;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.plan.PlanNodeId;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public record StageInfo(
        StageId stageId,
        StageState state,
        @Nullable PlanFragment plan,
        boolean coordinatorOnly,
        List<Type> types,
        StageStats stageStats,
        List<TaskInfo> tasks,
        List<StageId> subStages,
        Map<PlanNodeId, TableInfo> tables,
        ExecutionFailureInfo failureCause)
{
    public StageInfo {
        requireNonNull(stageId, "stageId is null");
        requireNonNull(state, "state is null");
        requireNonNull(stageStats, "stageStats is null");
        requireNonNull(tasks, "tasks is null");
        requireNonNull(subStages, "subStages is null");
        requireNonNull(tables, "tables is null");
        tasks = ImmutableList.copyOf(tasks);
        tables = ImmutableMap.copyOf(tables);
    }

    public boolean isFinalStageInfo()
    {
        return state.isDone() && tasks.stream().allMatch(taskInfo -> taskInfo.taskStatus().state().isDone());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stageId", stageId)
                .add("state", state)
                .toString();
    }

    public StageInfo withSubStages(List<StageId> subStages)
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
}
