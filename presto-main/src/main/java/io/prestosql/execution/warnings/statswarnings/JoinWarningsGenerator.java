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
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.spi.connector.StandardWarningCode.BETTER_JOIN_ORDERING;
import static java.lang.String.format;

public class JoinWarningsGenerator
        implements ExecutionStatisticsWarningsGenerator
{
//    This class will give join warnings if the number of rows on the
//    left side is less than the number of rows on the right side. It
//    will give you all the sources to help you identify the join. Sources
//    are table names and stage ids. For more complicated cases involving
//    multiple joins in a stage and multiple sources in the same stage, the
//    message will change slightly and give you all sources on the left and
//    right side.
//
//    Example:
//            Join
//
//        /           \
//
//    Table A        Table B
//
//    Try swapping the join ordering of Tables [A] InnerJoin Tables [B] in StageId 1
//
//
//            Join
//
//        /           \
//
//   fragmentId 2    Join
//
//                /         \
//
//            Table A     Table B
//
//    Try swapping the join in StageId 1 where the left side has sources StageIds [2]
//    and right side has sources Tables [Table A, Table B]

    @Override
    public List<PrestoWarning> generateExecutionStatisticsWarnings(QueryInfo queryInfo, Session session)
    {
        ImmutableList.Builder<PrestoWarning> allWarnings = new ImmutableList.Builder<>();
        processStage(queryInfo, allWarnings);
        return allWarnings.build();
    }

    private void processStage(QueryInfo queryInfo, ImmutableList.Builder<PrestoWarning> allWarnings)
    {
        if (queryInfo.getOutputStage().isPresent()) {
            processStageHelper(queryInfo.getOutputStage().get(), allWarnings);
        }
    }

    private void processStageHelper(StageInfo stage, ImmutableList.Builder<PrestoWarning> allWarnings)
    {
        for (StageInfo stageInfo : stage.getSubStages()) {
            processStageHelper(stageInfo, allWarnings);
        }
        Visitor visitor = new Visitor(allWarnings, stage.getStageStats().getOperatorSummaries(), stage.getStageId());
        PlanFragment planFragment = stage.getPlan();
        if (planFragment != null) {
            planFragment.getRoot().accept(visitor, null);
        }
    }

    private final class Visitor
            extends PlanVisitor<WarningSource, Void>
    {
        private final ImmutableList.Builder<PrestoWarning> allWarnings;
        private final List<OperatorStats> operatorStatsList;
        private final StageId stageId;

        private Visitor(
                ImmutableList.Builder<PrestoWarning> allWarnings,
                List<OperatorStats> operatorStatsList,
                StageId stageId)

        {
            this.allWarnings = allWarnings;
            this.operatorStatsList = operatorStatsList;
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
            if (!node.isCrossJoin()) {
                generateJoinWarnings(
                        left,
                        right,
                        node,
                        "LookupJoinOperator",
                        "HashBuilderOperator",
                        joinLabel,
                        id);
            }
            else {
                generateJoinWarnings(
                        left,
                        right,
                        node,
                        "NestedLoopJoinOperator",
                        "NestedLoopBuildOperator",
                        joinLabel,
                        id);
            }

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
            generateJoinWarnings(
                    left,
                    right,
                    node,
                    "SpatialJoinOperator",
                    "SpatialIndexBuilderOperator",
                    joinLabel,
                    id);
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
            generateJoinWarnings(
                    left,
                    right,
                    node,
                    "LookupJoinOperator",
                    "IndexSourceOperator",
                    joinLabel,
                    id);
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
            generateJoinWarnings(
                    left,
                    right,
                    node,
                    "HashSemiJoinOperator",
                    "SetBuilderOperator",
                    joinLabel,
                    id);
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

        private long getOperatorstats(PlanNode planNode, String operatorType)
        {
            for (OperatorStats operatorStat : operatorStatsList) {
                if (operatorStat.getPlanNodeId().equals(planNode.getId()) && operatorType.equals(operatorStat.getOperatorType())) {
                    return operatorStat.getInputPositions();
                }
            }
            return 0;
        }

        private void generateJoinWarnings(
                WarningSource left,
                WarningSource right,
                PlanNode joinNode,
                String leftOperatorType,
                String rightOperatorType,
                String joinLabel,
                String stageId)
        {
            long leftNumberOfRow = getOperatorstats(joinNode, leftOperatorType);
            long rightNumberOfRow = getOperatorstats(joinNode, rightOperatorType);

            if (!(leftNumberOfRow < rightNumberOfRow)) {
                return;
            }

            List<String> leftMessageBuilder = new ArrayList();
            List<String> rightMessageBuilder = new ArrayList();
            String leftMessage;
            String rightMessage;

            if (!left.getTableNames().isEmpty()) {
                leftMessageBuilder.add(format("Tables %s", left.getTableNames().toString()));
            }
            if (!left.getStageIds().isEmpty()) {
                leftMessageBuilder.add(format("Stage IDs %s", left.getStageIds().toString()));
            }

            if (!right.getTableNames().isEmpty()) {
                rightMessageBuilder.add(format("Tables %s", right.getTableNames().toString()));
            }
            if (!right.getStageIds().isEmpty()) {
                rightMessageBuilder.add(format("Stage IDs %s", right.getStageIds().toString()));
            }

            leftMessage = String.join(",", leftMessageBuilder);
            rightMessage = String.join(",", rightMessageBuilder);

            if (left.getTableNames().size() + left.getStageIds().size() == 1 && right.getTableNames().size() + right.getStageIds().size() == 1) {
                String warning = format("Try swapping the join ordering for %s %s %s in StageId %s",
                        leftMessage, joinLabel, rightMessage, stageId);
                allWarnings.add(new PrestoWarning(BETTER_JOIN_ORDERING, warning));
                return;
            }

            String warning = format("Try swapping the join in StageId %s where the left side has sources %s "
                    + "and right side has sources %s", stageId, leftMessage, rightMessage);

            allWarnings.add(new PrestoWarning(BETTER_JOIN_ORDERING, warning));
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
        private final List<String> tableNames = new ArrayList<>();
        private final List<String> stageIds = new ArrayList<>();

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
