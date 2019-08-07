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

package io.prestosql.execution.warnings.statswarnings;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.StageId;
import io.prestosql.execution.StageInfo;
import io.prestosql.operator.OperatorStats;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.prestosql.spi.connector.StandardWarningCode.BETTER_JOIN_ORDERING;
import static java.lang.String.format;

public class JoinWarningsGenerator
        implements ExecutionStatisticsWarningsGenerator
{
    @Override
    public List<PrestoWarning> generateStatsWarnings(QueryInfo queryInfo, Session session)
    {
        Map<String, List<String>> stageToJoinWarnings = new HashMap<>();
        visitStage(queryInfo, stageToJoinWarnings);
        ImmutableList.Builder<PrestoWarning> allWarnings = new ImmutableList.Builder<>();

        List<String> warningsMessageBuilder = new ArrayList<>();
        for (String stageId : stageToJoinWarnings.keySet()) {
            warningsMessageBuilder.addAll(stageToJoinWarnings.get(stageId));
        }
        if (!warningsMessageBuilder.isEmpty()) {
            allWarnings.add(new PrestoWarning(BETTER_JOIN_ORDERING, String.join(",", warningsMessageBuilder)));
        }
        return allWarnings.build();
    }

    private void visitStage(QueryInfo queryInfo, Map<String, List<String>> stageToJoinWarnings)
    {
        if (queryInfo.getOutputStage().isPresent()) {
            Visitor visitor = new Visitor(stageToJoinWarnings);
            visitStageHelper(queryInfo.getOutputStage().get(), visitor);
        }
    }

    private void visitStageHelper(StageInfo stage, Visitor visitor)
    {
        for (StageInfo stageInfo : stage.getSubStages()) {
            visitStageHelper(stageInfo, visitor);
        }
        visitor.setOperatorStatsList(stage.getStageStats().getOperatorSummaries());
        visitor.setStageId(stage.getStageId());
        stage.getPlan().getRoot().accept(visitor, null);
    }

    private final class Visitor
            extends PlanVisitor<WarningSource, Void>
    {
        private final Map<String, List<String>> stageToJoinWarnings;
        private List<OperatorStats> operatorStatsList;
        private StageId stageId;

        private Visitor(Map<String, List<String>> stageToJoinWarnings)
        {
            this.stageToJoinWarnings = stageToJoinWarnings;
        }

        private void setOperatorStatsList(List<OperatorStats> operatorStatsList)
        {
            this.operatorStatsList = operatorStatsList;
        }

        private void setStageId(StageId stageId)
        {
            this.stageId = stageId;
        }

        @Override
        public WarningSource visitPlan(PlanNode node, Void context)
        {
            WarningSource warningSource = new WarningSource();
            for (PlanNode source : node.getSources()) {
                WarningSource sourceInfo = source.accept(this, context);
                warningSource = combineWarningSources(warningSource, sourceInfo);
            }
            return warningSource;
        }

        @Override
        public WarningSource visitJoin(JoinNode node, Void context)
        {
            String id = Integer.toString(stageId.getId());
            PlanNode leftNode = node.getLeft();
            PlanNode rightNode = node.getRight();
            String joinLabel = node.getType().getJoinLabel();
            WarningSource left = leftNode.accept(this, context);
            WarningSource right = rightNode.accept(this, context);
            generateJoinWarnings(left, right, leftNode, rightNode, joinLabel, id);
            return combineWarningSources(left, right);
        }

        @Override
        public WarningSource visitLateralJoin(LateralJoinNode node, Void context)
        {
            String id = Integer.toString(stageId.getId());
            PlanNode leftNode = node.getInput();
            PlanNode rightNode = node.getSubquery();
            String joinLabel = node.getType().toJoinNodeType().getJoinLabel();
            WarningSource left = leftNode.accept(this, context);
            WarningSource right = rightNode.accept(this, context);
            generateJoinWarnings(left, right, leftNode, rightNode, joinLabel, id);
            return combineWarningSources(left, right);
        }

        @Override
        public WarningSource visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            String id = Integer.toString(stageId.getId());
            PlanNode leftNode = node.getLeft();
            PlanNode rightNode = node.getRight();
            String joinLabel = node.getType().getJoinLabel();
            WarningSource left = leftNode.accept(this, context);
            WarningSource right = rightNode.accept(this, context);
            generateJoinWarnings(left, right, leftNode, rightNode, joinLabel, id);
            return combineWarningSources(left, right);
        }

        @Override
        public WarningSource visitIndexJoin(IndexJoinNode node, Void context)
        {
            String id = Integer.toString(stageId.getId());
            PlanNode leftNode = node.getIndexSource();
            PlanNode rightNode = node.getProbeSource();
            String joinLabel = "IndexJoin";
            WarningSource left = leftNode.accept(this, context);
            WarningSource right = rightNode.accept(this, context);
            generateJoinWarnings(left, right, leftNode, rightNode, joinLabel, id);
            return combineWarningSources(left, right);
        }

        @Override
        public WarningSource visitSemiJoin(SemiJoinNode node, Void context)
        {
            String id = Integer.toString(stageId.getId());
            PlanNode leftNode = node.getFilteringSource();
            PlanNode rightNode = node.getSource();
            String joinLabel = "SemiJoin";
            WarningSource left = leftNode.accept(this, context);
            WarningSource right = rightNode.accept(this, context);
            generateJoinWarnings(left, right, leftNode, rightNode, joinLabel, id);
            return combineWarningSources(left, right);
        }

        @Override
        public WarningSource visitRemoteSource(RemoteSourceNode node, Void context)
        {
            WarningSource warningSource = new WarningSource();
            warningSource.addStageId(node.getSourceFragmentIds().stream().map(PlanFragmentId::toString).collect(Collectors.toList()));
            return warningSource;
        }

        @Override
        public WarningSource visitTableScan(TableScanNode node, Void context)
        {
            WarningSource warningSource = new WarningSource();
            warningSource.addTableNames(Arrays.asList(node.getTable().toString()));
            return warningSource;
        }

        private long getOperatorstats(PlanNode planNode)
        {
            List<OperatorStats> operatorStats = operatorStatsList;
            for (OperatorStats operatorStat : operatorStats) {
                if (operatorStat.getPlanNodeId().equals(planNode.getId())) {
                    return operatorStat.getOutputPositions();
                }
            }
            return 0;
        }

        private void generateJoinWarnings(WarningSource left, WarningSource right, PlanNode leftNode, PlanNode rightNode, String joinLabel, String stageId)
        {
            long leftNumberOfRow = getOperatorstats(leftNode);
            long rightNumberOfRow = getOperatorstats(rightNode);

            if (!(leftNumberOfRow < rightNumberOfRow)) {
                return;
            }

            List<String> leftMessageBuilder = new ArrayList();
            List<String> rightMessageBuilder = new ArrayList();
            String leftMessage;
            String rightMessage;

            if (!left.getTableNames().isEmpty()) {
                leftMessageBuilder.add(format("Tablename: %s", left.getTableNames().toString()));
            }
            if (!left.getStageIds().isEmpty()) {
                leftMessageBuilder.add(format("Stage IDs: %s", left.getStageIds().toString()));
            }

            if (!right.getTableNames().isEmpty()) {
                rightMessageBuilder.add(format("Tablename: %s", right.getTableNames().toString()));
            }
            if (!right.getStageIds().isEmpty()) {
                rightMessageBuilder.add(format("Stage IDs: %s", right.getStageIds().toString()));
            }

            leftMessage = String.join(",", leftMessageBuilder);
            rightMessage = String.join(",", rightMessageBuilder);

            String warning = format("Try swapping the join ordering for: (%s) %s (%s) in StageId: %s",
                    leftMessage, joinLabel, rightMessage, stageId);

            if (stageToJoinWarnings.get(stageId) == null) {
                stageToJoinWarnings.put(stageId, new ArrayList<>());
            }
            List<String> stageWarnings = stageToJoinWarnings.get(stageId);
            stageWarnings.add(warning);
        }

        private WarningSource combineWarningSources(WarningSource left, WarningSource right)
        {
            left.addStageId(right.getStageIds());
            left.addTableNames(right.getTableNames());
            return left;
        }
    }

    private static class WarningSource
    {
        private final List<String> tableNames;
        private final List<String> stageIds;

        public WarningSource()
        {
            tableNames = new ArrayList<>();
            stageIds = new ArrayList<>();
        }

        public void addTableNames(List<String> tableNames)
        {
            tableNames.forEach(this.tableNames::add);
        }

        public List<String> getTableNames()
        {
            return tableNames;
        }

        public void addStageId(List<String> stageIds)
        {
            stageIds.forEach(this.stageIds::add);
        }

        public List<String> getStageIds()
        {
            return stageIds;
        }
    }
}
