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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.cost.CostComparator;
import io.trino.cost.LocalCostEstimate;
import io.trino.cost.StatsProvider;
import io.trino.cost.TaskCountEstimator;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.SystemSessionProperties.getJoinMaxBroadcastTableSize;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isFaultTolerantExecutionAdaptiveBroadcastToPartitionedJoinEnabled;
import static io.trino.cost.CostCalculatorWithEstimatedExchanges.calculateJoinCostWithoutOutput;
import static io.trino.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteRepartitionCost;
import static io.trino.cost.LocalCostEstimate.addPartialComponents;
import static io.trino.cost.PlanNodeStatsEstimateMath.getSourceTablesSizeInBytes;
import static io.trino.matching.Pattern.any;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.sql.planner.plan.ExchangeNode.partitionedExchange;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.Patterns.Join.left;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

/**
 * Change the join type from broadcast to partitioned join based on runtime data size stats. This rule is only used
 * during Adaptive Planning in FTE.
 * <p>
 * <pre>
 * From:
 *  - Join (distribution: REPLICATED)
 *      - Partial Aggregation (optional)
 *          - Remote Exchange (distribution: RoundRobin, absent if source is RemoteSource)
 *              - left side or RemoteSource (distribution: REPARTITIONED)
 *      - Partial Aggregation (optional)
 *          - Local Exchange (optional)
 *              - Remote Exchange (distribution: REPLICATED, absent if source is RemoteSource)
 *                  - right side or RemoteSource (distribution: REPLICATED)
 * To:
 * - Join (distribution: PARTITIONED)
 *      - Partial Aggregation (optional)
 *          - Exchange (distribution: PARTITIONED, either change the existing one or add new if needed)
 *              - left side or RemoteSource (distribution: REPARTITIONED)
 *      - Partial Aggregation (optional)
 *          - Local Exchange (optional)
 *              - Remote Exchange (distribution: PARTITIONED, either change the existing one or add new if needed)
 *                  - right side or RemoteSource (distribution: REPARTITION)
 *</pre>
 *
 * <b>Note:</b>
 * We will try to not add extra Exchange nodes when possible. For example, if the left side is not
 * considered completed, and still has Exchange node with RoundRobin distribution, we will simply change the distribution
 * type of that exchange node to PARTITIONED instead of adding a new Exchange node. Same applies to the right side.
 * <p>
 * However, if the node is considered completed (i.e. RemoteSourceNode), we will need to add a new Exchange node
 * with partitioned distribution. We will try to consider the cost of adding an extra Exchange node while
 * making this decision.
 * <p>
 * TODO: We can make this rule more efficient by using the StreamPropertyDerivation mechanism to determine the
 *       RemoteExchangeNodes that can be reused instead of adding a new one. For instance, this will be helpful in
 *       cases where either side of the join has union nodes.
 *       https://github.com/trinodb/trino/issues/23372
 */
public class AdaptiveBroadcastToPartitionedJoin
        implements Rule<JoinNode>
{
    private static final Capture<ExchangeNode> LEFT_EXCHANGE_NODE = Capture.newCapture();
    private static final Capture<ExchangeNode> RIGHT_EXCHANGE_NODE = Capture.newCapture();

    private static final Pattern<JoinNode> PATTERN = join()
            .matching(joinNode -> joinNode.getDistributionType().equals(Optional.of(REPLICATED)))
            // Left side pattern
            .or(
                    // Partial aggregation + remote exchange pattern
                    prev -> prev.with(left()
                            .matching(aggregation().matching(node -> node.getStep() == PARTIAL)
                                    .with(source()
                                            .matching(exchange()
                                                    .matching(AdaptiveBroadcastToPartitionedJoin::isRoundRobinRemoteExchange)
                                                    .capturedAs(LEFT_EXCHANGE_NODE))))),
                    // remote exchange pattern
                    prev -> prev.with(left()
                            .matching(exchange().matching(AdaptiveBroadcastToPartitionedJoin::isRoundRobinRemoteExchange)
                                    .capturedAs(LEFT_EXCHANGE_NODE))),
                    // remote source node or any arbitrary node
                    prev -> prev.with(left().matching(any())))
            // Right side pattern
            .or(
                    // Partial aggregation + local exchange + remote exchange pattern
                    prev -> prev.with(right().matching(aggregation().matching(node -> node.getStep() == PARTIAL)
                            .with(source().matching(exchange().matching(node -> node.getScope() == LOCAL)
                                    .with(source().matching(exchange()
                                            .matching(AdaptiveBroadcastToPartitionedJoin::isBroadcastRemoteExchange)
                                            .capturedAs(RIGHT_EXCHANGE_NODE))))))),
                    // local exchange + remote exchange pattern
                    prev -> prev.with(right().matching(exchange().matching(node -> node.getScope() == LOCAL)
                            .with(source().matching(exchange()
                                    .matching(AdaptiveBroadcastToPartitionedJoin::isBroadcastRemoteExchange)
                                    .capturedAs(RIGHT_EXCHANGE_NODE))))),
                    // remote exchange pattern
                    prev -> prev.with(right().matching(exchange()
                            .matching(AdaptiveBroadcastToPartitionedJoin::isBroadcastRemoteExchange)
                            .capturedAs(RIGHT_EXCHANGE_NODE))),
                    // remote source node or any arbitrary node
                    prev -> prev.with(right().matching(any())));

    private final CostComparator costComparator;
    private final TaskCountEstimator taskCountEstimator;

    private static boolean isRoundRobinRemoteExchange(ExchangeNode node)
    {
        return node.getScope() == REMOTE
                && node.getType() == REPARTITION
                && node.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION)
                && node.getSources().size() == 1;
    }

    private static boolean isBroadcastRemoteExchange(ExchangeNode node)
    {
        return node.getScope() == REMOTE
                && node.getType() == REPLICATE
                && node.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_BROADCAST_DISTRIBUTION)
                && node.getSources().size() == 1;
    }

    public AdaptiveBroadcastToPartitionedJoin(CostComparator costComparator, TaskCountEstimator taskCountEstimator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public boolean isEnabled(Session session)
    {
        // This rule is only enabled in case of FTE
        return getRetryPolicy(session) == TASK
                && isFaultTolerantExecutionAdaptiveBroadcastToPartitionedJoinEnabled(session);
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        if (mustReplicate(node, context)) {
            return Result.empty();
        }
        // Check if we need to add extra remote exchange nodes to partition the data
        boolean isExtraRemoteExchangeNeededAtProbeSide = captures.getOptional(LEFT_EXCHANGE_NODE).isEmpty();
        boolean isExtraRemoteExchangeNeededAtBuildSide = captures.getOptional(RIGHT_EXCHANGE_NODE).isEmpty();
        boolean shouldChangeDistributionToPartitioned = changeDistributionToPartitioned(
                node,
                context,
                isExtraRemoteExchangeNeededAtProbeSide,
                isExtraRemoteExchangeNeededAtBuildSide);

        if (!shouldChangeDistributionToPartitioned) {
            return Result.empty();
        }

        return Result.ofPlanNode(convertToPartitionedJoin(node, context));
    }

    private static boolean mustReplicate(JoinNode joinNode, Context context)
    {
        JoinType type = joinNode.getType();
        if (joinNode.getCriteria().isEmpty() && (type == INNER || type == LEFT)) {
            // There is nothing to partition on
            return true;
        }
        return isAtMostScalar(joinNode.getRight(), context.getLookup());
    }

    private boolean changeDistributionToPartitioned(
            JoinNode joinNode,
            Context context,
            boolean isExtraRemoteExchangeNeededAtProbeSide,
            boolean isExtraRemoteExchangeNeededAtBuildSide)
    {
        DataSize joinMaxBroadcastTableSize = getJoinMaxBroadcastTableSize(context.getSession());

        PlanNodeWithCost replicatedJoinCost = getJoinNodeWithCost(
                context,
                joinNode,
                false,
                false);

        PlanNodeWithCost partitionedJoinCost = getJoinNodeWithCost(
                context,
                joinNode.withDistributionType(PARTITIONED),
                isExtraRemoteExchangeNeededAtProbeSide,
                isExtraRemoteExchangeNeededAtBuildSide);

        if (replicatedJoinCost.getCost().hasUnknownComponents() || partitionedJoinCost.getCost().hasUnknownComponents()) {
            // Take decision simply based on the stats
            return getSourceTablesSizeInBytes(joinNode.getRight(), context.getLookup(), context.getStatsProvider())
                    > joinMaxBroadcastTableSize.toBytes();
        }

        return costComparator.compare(
                context.getSession(),
                replicatedJoinCost.getCost(),
                partitionedJoinCost.getCost()) >= 0;
    }

    private JoinNode convertToPartitionedJoin(
            JoinNode joinNode,
            Context context)
    {
        // Left partitioned exchange node
        List<Symbol> probeSymbols = Lists.transform(joinNode.getCriteria(), JoinNode.EquiJoinClause::getLeft);
        ProbeSideRewriter probeSideRewriter = new ProbeSideRewriter(probeSymbols, context);
        PlanNode probeNode = rewriteWith(probeSideRewriter, context.getLookup().resolve(joinNode.getLeft()));

        // Change the join type to partitioned
        List<Symbol> buildSymbols = Lists.transform(joinNode.getCriteria(), JoinNode.EquiJoinClause::getRight);
        BuildSideRewriter buildSideRewriter = new BuildSideRewriter(buildSymbols, context);
        PlanNode buildNode = rewriteWith(buildSideRewriter, context.getLookup().resolve(joinNode.getRight()));

        return new JoinNode(
                joinNode.getId(),
                joinNode.getType(),
                probeNode,
                buildNode,
                joinNode.getCriteria(),
                joinNode.getLeftOutputSymbols(),
                joinNode.getRightOutputSymbols(),
                joinNode.isMaySkipOutputDuplicates(),
                joinNode.getFilter(),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                Optional.of(PARTITIONED),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters(),
                joinNode.getReorderJoinStatsAndCost());
    }

    private PlanNodeWithCost getJoinNodeWithCost(
            Context context,
            JoinNode joinNode,
            boolean isExtraRemoteExchangeNeededAtProbeSide,
            boolean isExtraRemoteExchangeNeededAtBuildSide)
    {
        StatsProvider stats = context.getStatsProvider();
        boolean replicated = joinNode.getDistributionType().get() == REPLICATED;
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
         *   cost based decisions for choosing between REPLICATED and PARTITIONED join.
         *
         *   TODO Decision about the distribution should be based on LocalCostEstimate only when PlanCostEstimate cannot be calculated. Otherwise cost comparator cannot take query.max-memory into account.
         */
        int estimatedSourceDistributedTaskCount = taskCountEstimator.estimateSourceDistributedTaskCount(context.getSession());
        LocalCostEstimate cost = calculateJoinCostWithoutOutput(
                joinNode.getLeft(),
                joinNode.getRight(),
                stats,
                replicated,
                estimatedSourceDistributedTaskCount);

        // In case we need to add extra remote exchange node to partition the data, we need
        // to consider the cost of adding them.
        if (isExtraRemoteExchangeNeededAtProbeSide) {
            double probeSizeInBytes = stats.getStats(joinNode.getLeft()).getOutputSizeInBytes(joinNode.getLeft().getOutputSymbols());
            LocalCostEstimate probeCost = calculateRemoteRepartitionCost(probeSizeInBytes);
            cost = addPartialComponents(cost, probeCost);
        }

        // In case we need to add extra remote exchange node to partition the data, we need
        // to consider the cost of adding them.
        if (isExtraRemoteExchangeNeededAtBuildSide) {
            double buildSizeInBytes = stats.getStats(joinNode.getRight()).getOutputSizeInBytes(joinNode.getRight().getOutputSymbols());
            LocalCostEstimate buildCost = calculateRemoteRepartitionCost(buildSizeInBytes);
            cost = addPartialComponents(cost, buildCost);
        }

        return new PlanNodeWithCost(cost.toPlanCost(), joinNode);
    }

    private static class ProbeSideRewriter
            extends SimplePlanRewriter<Void>
    {
        private final List<Symbol> probeSymbols;
        private final Context globalContext;

        private ProbeSideRewriter(List<Symbol> probeSymbols, Context globalContext)
        {
            this.probeSymbols = requireNonNull(probeSymbols, "probeSymbols is null");
            this.globalContext = requireNonNull(globalContext, "globalContext is null");
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            return partitionedExchange(
                    globalContext.getIdAllocator().getNextId(),
                    REMOTE,
                    node,
                    probeSymbols,
                    Optional.empty());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            if (node.getStep() == PARTIAL) {
                return rewriteSources(this, node, globalContext);
            }
            return visitPlan(node, context);
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            verify(node.getScope() == REMOTE && node.getType() == REPARTITION);
            return partitionedExchange(
                    globalContext.getIdAllocator().getNextId(),
                    REMOTE,
                    node.getSources().get(0),
                    probeSymbols,
                    Optional.empty());
        }

        @Override
        public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<Void> context)
        {
            verify(node.getExchangeType() == REPARTITION);
            return visitPlan(node, context);
        }
    }

    private static class BuildSideRewriter
            extends SimplePlanRewriter<Void>
    {
        private final List<Symbol> buildSymbols;
        private final Context globalContext;

        private BuildSideRewriter(List<Symbol> buildSymbols, Context globalContext)
        {
            this.buildSymbols = requireNonNull(buildSymbols, "buildSymbols is null");
            this.globalContext = requireNonNull(globalContext, "globalContext is null");
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            return partitionedExchange(
                    globalContext.getIdAllocator().getNextId(),
                    REMOTE,
                    node,
                    buildSymbols,
                    Optional.empty());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            if (node.getStep() == PARTIAL) {
                return rewriteSources(this, node, globalContext);
            }
            return visitPlan(node, context);
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            if (node.getScope() == LOCAL) {
                return rewriteSources(this, node, globalContext);
            }
            verify(node.getScope() == REMOTE && node.getType() == REPLICATE);
            return partitionedExchange(
                    globalContext.getIdAllocator().getNextId(),
                    REMOTE,
                    node.getSources().get(0),
                    buildSymbols,
                    Optional.empty());
        }

        @Override
        public PlanNode visitRemoteSource(RemoteSourceNode node, RewriteContext<Void> context)
        {
            verify(node.getExchangeType() == REPLICATE);
            RemoteSourceNode sourceNode = new RemoteSourceNode(
                    node.getId(),
                    node.getSourceFragmentIds(),
                    node.getOutputSymbols(),
                    node.getOrderingScheme(),
                    REPARTITION,
                    node.getRetryPolicy());
            return visitPlan(sourceNode, context);
        }
    }

    private static PlanNode rewriteSources(SimplePlanRewriter<Void> rewriter, PlanNode node, Context context)
    {
        ImmutableList.Builder<PlanNode> children = ImmutableList.builderWithExpectedSize(node.getSources().size());
        node.getSources().forEach(source -> children.add(rewriteWith(rewriter, context.getLookup().resolve(source))));
        return replaceChildren(node, children.build());
    }
}
