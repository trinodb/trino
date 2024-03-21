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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.cost.CachingStatsProvider;
import io.trino.cost.StatsCalculator;
import io.trino.cost.StatsProvider;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.sql.DynamicFilters;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.getSmallDynamicFilterMaxNdvCount;
import static io.trino.SystemSessionProperties.getSmallDynamicFilterMaxRowCount;
import static io.trino.SystemSessionProperties.getSmallDynamicFilterWaitTimeout;
import static io.trino.sql.DynamicFilters.getDescriptor;
import static io.trino.sql.DynamicFilters.replaceDynamicFilterTimeout;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class DeterminePreferredDynamicFilterTimeout
        implements PlanOptimizer
{
    private final StatsCalculator statsCalculator;

    public DeterminePreferredDynamicFilterTimeout(StatsCalculator statsCalculator)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Context context)
    {
        requireNonNull(plan, "plan is null");
        Session session = context.session();

        Duration smallDynamicFilterWaitTimeout = getSmallDynamicFilterWaitTimeout(session);
        long smallDynamicFilterMaxRowCount = getSmallDynamicFilterMaxRowCount(session);
        long smallDynamicFilterMaxNdvCount = getSmallDynamicFilterMaxNdvCount(session);

        if (smallDynamicFilterWaitTimeout.toMillis() == 0 || smallDynamicFilterMaxRowCount == 0 || smallDynamicFilterMaxNdvCount == 0) {
            return plan;
        }

        Map<DynamicFilterId, PlanNode> dynamicFilters = getDynamicFilterSources(plan);

        if (dynamicFilters.isEmpty()) {
            return plan;
        }
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, context.tableStatsProvider());

        return SimplePlanRewriter.rewriteWith(
                new DeterminePreferredDynamicFilterTimeout.Rewriter(
                        statsProvider,
                        smallDynamicFilterWaitTimeout,
                        smallDynamicFilterMaxRowCount,
                        smallDynamicFilterMaxNdvCount),
                plan,
                dynamicFilters);
    }

    private static Map<DynamicFilterId, PlanNode> getDynamicFilterSources(PlanNode plan)
    {
        return PlanNodeSearcher.searchFrom(plan)
                .findAll().stream()
                .flatMap(DeterminePreferredDynamicFilterTimeout::getDynamicFiltersMapping)
                .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private static Stream<SimpleEntry<DynamicFilterId, PlanNode>> getDynamicFiltersMapping(PlanNode planNode)
    {
        if (planNode instanceof JoinNode joinNode) {
            return joinNode.getDynamicFilters().keySet().stream()
                    .map(dynamicFilterId -> new SimpleEntry<>(dynamicFilterId, planNode));
        }
        if (planNode instanceof DynamicFilterSourceNode dynamicFilterSourceNode) {
            return dynamicFilterSourceNode.getDynamicFilters().keySet().stream()
                    .map(dynamicFilterId -> new SimpleEntry<>(dynamicFilterId, planNode));
        }
        if (planNode instanceof SemiJoinNode semiJoinNode && semiJoinNode.getDynamicFilterId().isPresent()) {
            return Stream.of(new SimpleEntry<>(semiJoinNode.getDynamicFilterId().get(), planNode));
        }
        return Stream.of();
    }

    private static class Rewriter
            extends SimplePlanRewriter<Map<DynamicFilterId, PlanNode>>
    {
        private final StatsProvider statsProvider;
        private final long smallDynamicFilterWaitTimeoutMillis;
        private final long smallDynamicFilterMaxRowCount;
        private final long smallDynamicFilterMaxNdvCount;
        private final Map<DynamicFilterId, DynamicFilterTimeout> dynamicFilterBuildSideStates = new HashMap<>();

        public Rewriter(
                StatsProvider statsProvider,
                Duration smallDynamicFilterWaitTimeout,
                long smallDynamicFilterMaxRowCount,
                long smallDynamicFilterMaxNdvCount)
        {
            this.statsProvider = statsProvider;
            this.smallDynamicFilterWaitTimeoutMillis = smallDynamicFilterWaitTimeout.toMillis();
            this.smallDynamicFilterMaxRowCount = smallDynamicFilterMaxRowCount;
            this.smallDynamicFilterMaxNdvCount = smallDynamicFilterMaxNdvCount;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, SimplePlanRewriter.RewriteContext<Map<DynamicFilterId, PlanNode>> rewriteContext)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                // SimplePlanRewriter is visiting all filter nodes, not only ones with dynamic filters.
                return visitPlan(node, rewriteContext);
            }

            Map<DynamicFilterId, PlanNode> dynamicFiltersContext = rewriteContext.get();
            List<Expression> conjuncts = extractConjuncts(node.getPredicate());

            ImmutableList.Builder<Expression> expressionBuilder = ImmutableList.builder();
            for (Expression conjunct : conjuncts) {
                Optional<DynamicFilters.Descriptor> descriptor = getDescriptor(conjunct);
                if (descriptor.isEmpty()) {
                    expressionBuilder.add(conjunct);
                    continue;
                }
                DynamicFilterId dynamicFilterId = descriptor.get().getId();
                PlanNode planNode = dynamicFiltersContext.get(dynamicFilterId);

                DynamicFilterTimeout dynamicFilterTimeout = dynamicFilterBuildSideStates.computeIfAbsent(dynamicFilterId, ignore -> getBuildSideState(getBuildSide(planNode), getDynamicFilterSymbol(planNode, dynamicFilterId)));
                switch (dynamicFilterTimeout) {
                    case USE_PREFERRED_TIMEOUT -> expressionBuilder.add(replaceDynamicFilterTimeout((Call) conjunct, smallDynamicFilterWaitTimeoutMillis));
                    case NO_WAIT -> expressionBuilder.add(replaceDynamicFilterTimeout((Call) conjunct, 0));
                    case UNESTIMATED -> expressionBuilder.add(conjunct);
                }
            }

            return new FilterNode(
                    node.getId(),
                    node.getSource(),
                    combineConjuncts(expressionBuilder.build()));
        }

        private static Symbol getDynamicFilterSymbol(PlanNode planNode, DynamicFilterId dynamicFilterId)
        {
            if (planNode instanceof JoinNode joinNode) {
                return joinNode.getDynamicFilters().get(dynamicFilterId);
            }
            if (planNode instanceof SemiJoinNode semiJoinNode) {
                return semiJoinNode.getFilteringSourceJoinSymbol();
            }
            if (planNode instanceof DynamicFilterSourceNode dynamicFilterSourceNode) {
                return dynamicFilterSourceNode.getDynamicFilters().get(dynamicFilterId);
            }
            throw new IllegalArgumentException("Plan node unsupported " + planNode.getClass().getSimpleName());
        }

        private static PlanNode getBuildSide(PlanNode planNode)
        {
            if (planNode instanceof JoinNode joinNode) {
                return joinNode.getRight();
            }
            else if (planNode instanceof SemiJoinNode semiJoinNode) {
                return semiJoinNode.getFilteringSource();
            }
            else if (planNode instanceof DynamicFilterSourceNode dynamicFilterSourceNode) {
                return dynamicFilterSourceNode.getSource();
            }
            throw new IllegalArgumentException("Plan node unsupported " + planNode.getClass().getSimpleName());
        }

        private DynamicFilterTimeout getBuildSideState(PlanNode planNode, Symbol dynamicFilterSymbol)
        {
            // Skip for expanding plan nodes like CROSS JOIN or UNNEST which can substantially increase the amount of data.
            if (isInputMultiplyingPlanNodePresent(planNode)) {
                return DynamicFilterTimeout.NO_WAIT;
            }
            SymbolStatsEstimate symbolStatsEstimate = statsProvider.getStats(planNode).getSymbolStatistics(dynamicFilterSymbol);
            if (!symbolStatsEstimate.isUnknown() && !isExpandingPlanNodePresent(planNode)) {
                if (symbolStatsEstimate.getDistinctValuesCount() < smallDynamicFilterMaxNdvCount) {
                    return DynamicFilterTimeout.USE_PREFERRED_TIMEOUT;
                }
            }

            double rowCount = getEstimatedMaxOutputRowCount(planNode, statsProvider);
            if (isNaN(rowCount)) {
                return DynamicFilterTimeout.UNESTIMATED;
            }
            if (rowCount < smallDynamicFilterMaxRowCount) {
                return DynamicFilterTimeout.USE_PREFERRED_TIMEOUT;
            }

            return DynamicFilterTimeout.NO_WAIT;
        }
    }

    private static boolean isInputMultiplyingPlanNodePresent(PlanNode root)
    {
        return PlanNodeSearcher.searchFrom(root)
                .where(DeterminePreferredDynamicFilterTimeout::isInputMultiplyingPlanNode)
                .matches();
    }

    private static boolean isExpandingPlanNodePresent(PlanNode root)
    {
        return PlanNodeSearcher.searchFrom(root)
                .where(DeterminePreferredDynamicFilterTimeout::isExpandingPlanNode)
                .matches();
    }

    private static boolean isInputMultiplyingPlanNode(PlanNode node)
    {
        if (node instanceof UnnestNode) {
            return true;
        }

        if (node instanceof JoinNode joinNode) {
            // Skip for cross join
            if (joinNode.isCrossJoin()) {
                // If any of the input node is scalar then there's no need to skip cross join
                return !isAtMostScalar(joinNode.getRight()) && !isAtMostScalar(joinNode.getLeft());
            }

            // Skip for joins with multi keys since output row count stats estimation can wrong due to
            // low correlation between multiple join keys.
            return joinNode.getCriteria().size() > 1;
        }

        return false;
    }

    private static Double getEstimatedMaxOutputRowCount(PlanNode plan, StatsProvider statsProvider)
    {
        // TODO: this and dependant functions are similar to DeterminePartitionCount and should be extracted to Utils class
        double sourceTablesRowCount = getSourceNodesOutputStats(plan, statsProvider);
        double expandingNodesMaxRowCount = getExpandingNodesMaxOutputStats(plan, statsProvider);

        return max(sourceTablesRowCount, expandingNodesMaxRowCount);
    }

    private static double getSourceNodesOutputStats(PlanNode root, StatsProvider statsProvider)
    {
        List<PlanNode> sourceNodes = PlanNodeSearcher.searchFrom(root)
                .whereIsInstanceOfAny(TableScanNode.class, ValuesNode.class)
                .findAll();

        return sourceNodes.stream()
                .mapToDouble(node -> statsProvider.getStats(node).getOutputRowCount())
                .sum();
    }

    private static double getExpandingNodesMaxOutputStats(PlanNode root, StatsProvider statsProvider)
    {
        List<PlanNode> expandingNodes = PlanNodeSearcher.searchFrom(root)
                .where(DeterminePreferredDynamicFilterTimeout::isExpandingPlanNode)
                .findAll();

        return expandingNodes.stream()
                .mapToDouble(node -> statsProvider.getStats(node).getOutputRowCount())
                .max()
                .orElse(0);
    }

    private static boolean isExpandingPlanNode(PlanNode node)
    {
        return node instanceof JoinNode
                // consider union node and exchange node with multiple sources as expanding since it merge the rows
                // from two different sources, thus more data is transferred over the network.
                || node instanceof UnionNode
                || (node instanceof ExchangeNode && node.getSources().size() > 1);
    }

    enum DynamicFilterTimeout
    {
        USE_PREFERRED_TIMEOUT,
        NO_WAIT,
        UNESTIMATED,
    }
}
