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

package io.trino.sql.planner.iterative.rule;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;
import io.trino.cost.CostComparator;
import io.trino.cost.LocalCostEstimate;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.StatsProvider;
import io.trino.cost.TaskCountEstimator;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Unnest;

import java.util.ArrayList;
import java.util.List;

import static io.trino.SystemSessionProperties.getJoinDistributionType;
import static io.trino.SystemSessionProperties.getJoinMaxBroadcastTableSize;
import static io.trino.cost.CostCalculatorWithEstimatedExchanges.calculateJoinCostWithoutOutput;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.AUTOMATIC;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinNode.Type.FULL;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.util.MorePredicates.isInstanceOfAny;
import static java.util.Objects.requireNonNull;

public class DetermineJoinDistributionType
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join().matching(joinNode -> joinNode.getDistributionType().isEmpty());

    private final CostComparator costComparator;
    private final TaskCountEstimator taskCountEstimator;

    public DetermineJoinDistributionType(CostComparator costComparator, TaskCountEstimator taskCountEstimator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        JoinDistributionType joinDistributionType = getJoinDistributionType(context.getSession());
        if (joinDistributionType == AUTOMATIC) {
            return Result.ofPlanNode(getCostBasedJoin(joinNode, context));
        }
        return Result.ofPlanNode(getSyntacticOrderJoin(joinNode, context, joinDistributionType));
    }

    public static boolean canReplicate(JoinNode joinNode, Context context)
    {
        JoinDistributionType joinDistributionType = getJoinDistributionType(context.getSession());
        if (!joinDistributionType.canReplicate()) {
            return false;
        }

        DataSize joinMaxBroadcastTableSize = getJoinMaxBroadcastTableSize(context.getSession());

        PlanNode buildSide = joinNode.getRight();
        PlanNodeStatsEstimate buildSideStatsEstimate = context.getStatsProvider().getStats(buildSide);
        double buildSideSizeInBytes = buildSideStatsEstimate.getOutputSizeInBytes(buildSide.getOutputSymbols(), context.getSymbolAllocator().getTypes());
        return buildSideSizeInBytes <= joinMaxBroadcastTableSize.toBytes()
                || getSourceTablesSizeInBytes(buildSide, context) <= joinMaxBroadcastTableSize.toBytes();
    }

    public static double getSourceTablesSizeInBytes(PlanNode node, Context context)
    {
        return getSourceTablesSizeInBytes(node, context.getLookup(), context.getStatsProvider(), context.getSymbolAllocator().getTypes());
    }

    @VisibleForTesting
    static double getSourceTablesSizeInBytes(PlanNode node, Lookup lookup, StatsProvider statsProvider, TypeProvider typeProvider)
    {
        boolean hasExpandingNodes = PlanNodeSearcher.searchFrom(node, lookup)
                .where(isInstanceOfAny(JoinNode.class, Unnest.class))
                .matches();
        if (hasExpandingNodes) {
            return Double.NaN;
        }

        List<PlanNode> sourceNodes = PlanNodeSearcher.searchFrom(node, lookup)
                .where(isInstanceOfAny(TableScanNode.class, ValuesNode.class))
                .findAll();

        return sourceNodes.stream()
                .mapToDouble(sourceNode -> statsProvider.getStats(sourceNode).getOutputSizeInBytes(sourceNode.getOutputSymbols(), typeProvider))
                .sum();
    }

    private PlanNode getCostBasedJoin(JoinNode joinNode, Context context)
    {
        List<PlanNodeWithCost> possibleJoinNodes = new ArrayList<>();

        addJoinsWithDifferentDistributions(joinNode, possibleJoinNodes, context);
        addJoinsWithDifferentDistributions(joinNode.flipChildren(), possibleJoinNodes, context);

        if (possibleJoinNodes.stream().anyMatch(result -> result.getCost().hasUnknownComponents()) || possibleJoinNodes.isEmpty()) {
            return getSizeBasedJoin(joinNode, context);
        }

        // Using Ordering to facilitate rule determinism
        Ordering<PlanNodeWithCost> planNodeOrderings = costComparator.forSession(context.getSession()).onResultOf(PlanNodeWithCost::getCost);
        return planNodeOrderings.min(possibleJoinNodes).getPlanNode();
    }

    private JoinNode getSizeBasedJoin(JoinNode joinNode, Context context)
    {
        DataSize joinMaxBroadcastTableSize = getJoinMaxBroadcastTableSize(context.getSession());

        boolean isRightSideSmall = getSourceTablesSizeInBytes(joinNode.getRight(), context) <= joinMaxBroadcastTableSize.toBytes();
        if (isRightSideSmall && !mustPartition(joinNode)) {
            // choose right join side with small source tables as replicated build side
            return joinNode.withDistributionType(REPLICATED);
        }

        boolean isLeftSideSmall = getSourceTablesSizeInBytes(joinNode.getLeft(), context) <= joinMaxBroadcastTableSize.toBytes();
        if (isLeftSideSmall && !mustPartition(joinNode.flipChildren())) {
            // choose join left side with small source tables as replicated build side
            return joinNode.flipChildren().withDistributionType(REPLICATED);
        }

        if (isRightSideSmall) {
            // right side is small enough, but must be partitioned
            return joinNode.withDistributionType(PARTITIONED);
        }

        if (isLeftSideSmall) {
            // left side is small enough, but must be partitioned
            return joinNode.flipChildren().withDistributionType(PARTITIONED);
        }

        // neither side is small enough, choose syntactic join order
        return getSyntacticOrderJoin(joinNode, context, AUTOMATIC);
    }

    private void addJoinsWithDifferentDistributions(JoinNode joinNode, List<PlanNodeWithCost> possibleJoinNodes, Context context)
    {
        if (!mustPartition(joinNode) && canReplicate(joinNode, context)) {
            possibleJoinNodes.add(getJoinNodeWithCost(context, joinNode.withDistributionType(REPLICATED)));
        }
        if (!mustReplicate(joinNode, context)) {
            possibleJoinNodes.add(getJoinNodeWithCost(context, joinNode.withDistributionType(PARTITIONED)));
        }
    }

    private JoinNode getSyntacticOrderJoin(JoinNode joinNode, Context context, JoinDistributionType joinDistributionType)
    {
        if (mustPartition(joinNode)) {
            return joinNode.withDistributionType(PARTITIONED);
        }
        if (mustReplicate(joinNode, context)) {
            return joinNode.withDistributionType(REPLICATED);
        }
        if (joinDistributionType.canPartition()) {
            return joinNode.withDistributionType(PARTITIONED);
        }
        return joinNode.withDistributionType(REPLICATED);
    }

    private static boolean mustPartition(JoinNode joinNode)
    {
        JoinNode.Type type = joinNode.getType();
        // With REPLICATED, the unmatched rows from right-side would be duplicated.
        return type == RIGHT || type == FULL;
    }

    private static boolean mustReplicate(JoinNode joinNode, Context context)
    {
        JoinNode.Type type = joinNode.getType();
        if (joinNode.getCriteria().isEmpty() && (type == INNER || type == LEFT)) {
            // There is nothing to partition on
            return true;
        }
        return isAtMostScalar(joinNode.getRight(), context.getLookup());
    }

    private PlanNodeWithCost getJoinNodeWithCost(Context context, JoinNode possibleJoinNode)
    {
        TypeProvider types = context.getSymbolAllocator().getTypes();
        StatsProvider stats = context.getStatsProvider();
        boolean replicated = possibleJoinNode.getDistributionType().get() == REPLICATED;
        /*
         *   HACK!
         *
         *   Currently cost model always has to compute the total cost of an operation.
         *   For JOIN the total cost consist of 4 parts:
         *     - Cost of exchanges that have to be introduced to execute a JOIN
         *     - Cost of building a hash table
         *     - Cost of probing a hash table
         *     - Cost of building an output for matched rows
         *
         *   When output size for a JOIN cannot be estimated the cost model returns
         *   UNKNOWN cost for the join.
         *
         *   However assuming the cost of JOIN output is always the same, we can still make
         *   cost based decisions based on the input cost for different types of JOINs.
         *
         *   Although the side flipping can be made purely based on stats (smaller side
         *   always goes to the right), determining JOIN type is not that simple. As when
         *   choosing REPLICATED over REPARTITIONED join the cost of exchanging and building
         *   the hash table scales with the number of nodes where the build side is replicated.
         *
         *   TODO Decision about the distribution should be based on LocalCostEstimate only when PlanCostEstimate cannot be calculated. Otherwise cost comparator cannot take query.max-memory into account.
         */
        int estimatedSourceDistributedTaskCount = taskCountEstimator.estimateSourceDistributedTaskCount(context.getSession());
        LocalCostEstimate cost = calculateJoinCostWithoutOutput(
                possibleJoinNode.getLeft(),
                possibleJoinNode.getRight(),
                stats,
                types,
                replicated,
                estimatedSourceDistributedTaskCount);
        return new PlanNodeWithCost(cost.toPlanCost(), possibleJoinNode);
    }
}
