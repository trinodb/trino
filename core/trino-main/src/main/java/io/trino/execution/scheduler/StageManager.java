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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.execution.BasicStageStats;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.QueryStateMachine;
import io.trino.execution.RemoteTaskFactory;
import io.trino.execution.SqlStage;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.TableInfo;
import io.trino.execution.TaskId;
import io.trino.metadata.Metadata;
import io.trino.spi.QueryId;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.SubPlan;
import io.trino.sql.planner.plan.PlanFragmentId;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.execution.BasicStageStats.aggregateBasicStageStats;
import static io.trino.execution.SqlStage.createSqlStage;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;

class StageManager
{
    private final QueryStateMachine queryStateMachine;
    private final Map<StageId, SqlStage> stages;
    private final List<SqlStage> stagesInTopologicalOrder;
    private final List<SqlStage> coordinatorStagesInTopologicalOrder;
    private final List<SqlStage> distributedStagesInTopologicalOrder;
    private final StageId rootStageId;
    private final Map<StageId, Set<StageId>> children;
    private final Map<StageId, StageId> parents;

    static StageManager create(
            QueryStateMachine queryStateMachine,
            Metadata metadata,
            RemoteTaskFactory taskFactory,
            NodeTaskMap nodeTaskMap,
            SplitSchedulerStats schedulerStats,
            SubPlan planTree,
            boolean summarizeTaskInfo)
    {
        Session session = queryStateMachine.getSession();
        ImmutableMap.Builder<StageId, SqlStage> stages = ImmutableMap.builder();
        ImmutableList.Builder<SqlStage> stagesInTopologicalOrder = ImmutableList.builder();
        ImmutableList.Builder<SqlStage> coordinatorStagesInTopologicalOrder = ImmutableList.builder();
        ImmutableList.Builder<SqlStage> distributedStagesInTopologicalOrder = ImmutableList.builder();
        StageId rootStageId = null;
        ImmutableMap.Builder<StageId, Set<StageId>> children = ImmutableMap.builder();
        ImmutableMap.Builder<StageId, StageId> parents = ImmutableMap.builder();
        for (SubPlan planNode : Traverser.forTree(SubPlan::getChildren).breadthFirst(planTree)) {
            PlanFragment fragment = planNode.getFragment();
            SqlStage stage = createSqlStage(
                    getStageId(session.getQueryId(), fragment.getId()),
                    fragment,
                    TableInfo.extract(session, metadata, fragment),
                    taskFactory,
                    session,
                    summarizeTaskInfo,
                    nodeTaskMap,
                    queryStateMachine.getStateMachineExecutor(),
                    schedulerStats);
            StageId stageId = stage.getStageId();
            stages.put(stageId, stage);
            stagesInTopologicalOrder.add(stage);
            if (fragment.getPartitioning().isCoordinatorOnly()) {
                coordinatorStagesInTopologicalOrder.add(stage);
            }
            else {
                distributedStagesInTopologicalOrder.add(stage);
            }
            if (rootStageId == null) {
                rootStageId = stageId;
            }
            Set<StageId> childStageIds = planNode.getChildren().stream()
                    .map(childStage -> getStageId(session.getQueryId(), childStage.getFragment().getId()))
                    .collect(toImmutableSet());
            children.put(stageId, childStageIds);
            childStageIds.forEach(child -> parents.put(child, stageId));
        }
        StageManager stageManager = new StageManager(
                queryStateMachine,
                stages.buildOrThrow(),
                stagesInTopologicalOrder.build(),
                coordinatorStagesInTopologicalOrder.build(),
                distributedStagesInTopologicalOrder.build(),
                rootStageId,
                children.buildOrThrow(),
                parents.buildOrThrow());
        stageManager.initialize();
        return stageManager;
    }

    private static StageId getStageId(QueryId queryId, PlanFragmentId fragmentId)
    {
        // TODO: refactor fragment id to be based on an integer
        return new StageId(queryId, parseInt(fragmentId.toString()));
    }

    private StageManager(
            QueryStateMachine queryStateMachine,
            Map<StageId, SqlStage> stages,
            List<SqlStage> stagesInTopologicalOrder,
            List<SqlStage> coordinatorStagesInTopologicalOrder,
            List<SqlStage> distributedStagesInTopologicalOrder,
            StageId rootStageId,
            Map<StageId, Set<StageId>> children,
            Map<StageId, StageId> parents)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.stages = ImmutableMap.copyOf(requireNonNull(stages, "stages is null"));
        this.stagesInTopologicalOrder = ImmutableList.copyOf(requireNonNull(stagesInTopologicalOrder, "stagesInTopologicalOrder is null"));
        this.coordinatorStagesInTopologicalOrder = ImmutableList.copyOf(requireNonNull(coordinatorStagesInTopologicalOrder, "coordinatorStagesInTopologicalOrder is null"));
        this.distributedStagesInTopologicalOrder = ImmutableList.copyOf(requireNonNull(distributedStagesInTopologicalOrder, "distributedStagesInTopologicalOrder is null"));
        this.rootStageId = requireNonNull(rootStageId, "rootStageId is null");
        this.children = ImmutableMap.copyOf(requireNonNull(children, "children is null"));
        this.parents = ImmutableMap.copyOf(requireNonNull(parents, "parents is null"));
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        for (SqlStage stage : stages.values()) {
            stage.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo())));
        }
    }

    public void finish()
    {
        stages.values().forEach(SqlStage::finish);
    }

    public void abort()
    {
        stages.values().forEach(SqlStage::abort);
    }

    public void failTaskRemotely(TaskId taskId, Throwable failureCause)
    {
        SqlStage sqlStage = requireNonNull(stages.get(taskId.getStageId()), () -> "stage not found: %s" + taskId.getStageId());
        sqlStage.failTaskRemotely(taskId, failureCause);
    }

    public List<SqlStage> getStagesInTopologicalOrder()
    {
        return stagesInTopologicalOrder;
    }

    public List<SqlStage> getCoordinatorStagesInTopologicalOrder()
    {
        return coordinatorStagesInTopologicalOrder;
    }

    public List<SqlStage> getDistributedStagesInTopologicalOrder()
    {
        return distributedStagesInTopologicalOrder;
    }

    public SqlStage getOutputStage()
    {
        return stages.get(rootStageId);
    }

    public SqlStage get(PlanFragmentId fragmentId)
    {
        return get(getStageId(queryStateMachine.getQueryId(), fragmentId));
    }

    public SqlStage get(StageId stageId)
    {
        return requireNonNull(stages.get(stageId), () -> "stage not found: " + stageId);
    }

    public Set<SqlStage> getChildren(PlanFragmentId fragmentId)
    {
        return getChildren(getStageId(queryStateMachine.getQueryId(), fragmentId));
    }

    public Set<SqlStage> getChildren(StageId stageId)
    {
        return children.get(stageId).stream()
                .map(this::get)
                .collect(toImmutableSet());
    }

    public Optional<SqlStage> getParent(PlanFragmentId fragmentId)
    {
        return getParent(getStageId(queryStateMachine.getQueryId(), fragmentId));
    }

    public Optional<SqlStage> getParent(StageId stageId)
    {
        return Optional.ofNullable(parents.get(stageId)).map(stages::get);
    }

    public BasicStageStats getBasicStageStats()
    {
        List<BasicStageStats> stageStats = stages.values().stream()
                .map(SqlStage::getBasicStageStats)
                .collect(toImmutableList());

        return aggregateBasicStageStats(stageStats);
    }

    public StageInfo getStageInfo()
    {
        Map<StageId, StageInfo> stageInfos = stages.values().stream()
                .map(SqlStage::getStageInfo)
                .collect(toImmutableMap(StageInfo::getStageId, identity()));

        return buildStageInfo(rootStageId, stageInfos);
    }

    private StageInfo buildStageInfo(StageId stageId, Map<StageId, StageInfo> stageInfos)
    {
        StageInfo parent = stageInfos.get(stageId);
        checkArgument(parent != null, "No stageInfo for %s", parent);
        List<StageInfo> childStages = children.get(stageId).stream()
                .map(childStageId -> buildStageInfo(childStageId, stageInfos))
                .collect(toImmutableList());
        if (childStages.isEmpty()) {
            return parent;
        }
        return new StageInfo(
                parent.getStageId(),
                parent.getState(),
                parent.getPlan(),
                parent.isCoordinatorOnly(),
                parent.getTypes(),
                parent.getStageStats(),
                parent.getTasks(),
                childStages,
                parent.getTables(),
                parent.getFailureCause());
    }

    public long getUserMemoryReservation()
    {
        return stages.values().stream()
                .mapToLong(SqlStage::getUserMemoryReservation)
                .sum();
    }

    public long getTotalMemoryReservation()
    {
        return stages.values().stream()
                .mapToLong(SqlStage::getTotalMemoryReservation)
                .sum();
    }

    public Duration getTotalCpuTime()
    {
        long millis = stages.values().stream()
                .mapToLong(stage -> stage.getTotalCpuTime().toMillis())
                .sum();
        return new Duration(millis, MILLISECONDS);
    }
}
