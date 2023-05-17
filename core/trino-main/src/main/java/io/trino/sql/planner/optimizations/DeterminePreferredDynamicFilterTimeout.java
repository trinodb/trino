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
import io.trino.cost.TableStatsProvider;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.DynamicFilters;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnionNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.getSmallDynamicFilterMaxRowCount;
import static io.trino.SystemSessionProperties.getSmallDynamicFilterWaitTimeout;
import static io.trino.sql.DynamicFilters.getDescriptor;
import static io.trino.sql.DynamicFilters.replaceDynamicFilterTimeout;
import static io.trino.sql.ExpressionUtils.combineConjuncts;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public class DeterminePreferredDynamicFilterTimeout
        implements PlanOptimizer
{
    private final PlannerContext plannerContext;
    private final StatsCalculator statsCalculator;

    public DeterminePreferredDynamicFilterTimeout(PlannerContext plannerContext, StatsCalculator statsCalculator)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            TypeProvider types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator,
            WarningCollector warningCollector,
            PlanOptimizersStatsCollector planOptimizersStatsCollector,
            TableStatsProvider tableStatsProvider)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(idAllocator, "idAllocator is null");

        Duration smallDynamicFilterWaitTimeout = getSmallDynamicFilterWaitTimeout(session);
        long smallDynamicFilterMaxRowCount = getSmallDynamicFilterMaxRowCount(session);

        if (smallDynamicFilterWaitTimeout.toMillis() == 0 || smallDynamicFilterMaxRowCount == 0) {
            return plan;
        }

        Map<DynamicFilterId, JoinNode> dynamicFilters = getDynamicFilterSources(plan);

        if (dynamicFilters.isEmpty()) {
            return plan;
        }
        StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, session, types, tableStatsProvider);

        return SimplePlanRewriter.rewriteWith(
                new DeterminePreferredDynamicFilterTimeout.Rewriter(
                        plannerContext,
                        statsProvider,
                        smallDynamicFilterWaitTimeout,
                        smallDynamicFilterMaxRowCount),
                plan,
                dynamicFilters);
    }

    private static Map<DynamicFilterId, JoinNode> getDynamicFilterSources(PlanNode plan)
    {
        return PlanNodeSearcher.searchFrom(plan)
                .where(planNode -> {
                    if (planNode instanceof JoinNode joinNode) {
                        return !joinNode.getDynamicFilters().isEmpty();
                    }
                    return false;
                }).findAll().stream()
                .map(JoinNode.class::cast)
                .flatMap(
                        joinNode -> joinNode.getDynamicFilters().keySet().stream()
                                .map(dynamicFilterId -> new SimpleEntry<>(dynamicFilterId, joinNode)))
                .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    private static class Rewriter
            extends SimplePlanRewriter<Map<DynamicFilterId, JoinNode>>
    {
        private final PlannerContext plannerContext;
        private final StatsProvider statsProvider;
        private final long smallDynamicFilterWaitTimeoutMillis;
        private final long smallDynamicFilterMaxRowCount;
        private final Map<DynamicFilterId, Boolean> dynamicFilterEstimations = new HashMap<>();

        public Rewriter(
                PlannerContext plannerContext,
                StatsProvider statsProvider,
                Duration smallDynamicFilterWaitTimeout,
                long smallDynamicFilterMaxRowCount)
        {
            this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
            this.statsProvider = statsProvider;
            this.smallDynamicFilterWaitTimeoutMillis = smallDynamicFilterWaitTimeout.toMillis();
            this.smallDynamicFilterMaxRowCount = smallDynamicFilterMaxRowCount;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, SimplePlanRewriter.RewriteContext<Map<DynamicFilterId, JoinNode>> rewriteContext)
        {
            if (!(node.getSource() instanceof TableScanNode)) {
                // SimplePlanRewriter is visiting all filter nodes, not only ones with dynamic filters.
                return visitPlan(node, rewriteContext);
            }

            Map<DynamicFilterId, JoinNode> dynamicFiltersContext = rewriteContext.get();
            List<Expression> conjuncts = extractConjuncts(node.getPredicate());
            boolean rewritten = false;

            ImmutableList.Builder<Expression> expressionBuilder = ImmutableList.builder();
            for (Expression conjunct : conjuncts) {
                Optional<DynamicFilters.Descriptor> descriptor = getDescriptor(conjunct);
                if (descriptor.isEmpty()) {
                    expressionBuilder.add(conjunct);
                    continue;
                }
                DynamicFilterId dynamicFilterId = descriptor.get().getId();
                JoinNode joinNode = dynamicFiltersContext.get(dynamicFilterId);

                Boolean isSmallOutput = dynamicFilterEstimations.computeIfAbsent(dynamicFilterId, ignore -> isSmallOutputRowCount(joinNode.getRight()));
                if (isSmallOutput) {
                    rewritten = true;
                    expressionBuilder.add(replaceDynamicFilterTimeout((FunctionCall) conjunct, smallDynamicFilterWaitTimeoutMillis));
                }
                else {
                    expressionBuilder.add(conjunct);
                }
            }

            if (!rewritten) {
                return node;
            }

            return new FilterNode(
                    node.getId(),
                    node.getSource(),
                    combineConjuncts(plannerContext.getMetadata(), expressionBuilder.build()));
        }

        private boolean isSmallOutputRowCount(PlanNode planNode)
        {
            // Skip for expanding plan nodes like CROSS JOIN or UNNEST which can substantially increase the amount of data.
            if (isInputMultiplyingPlanNodePresent(planNode)) {
                return false;
            }
            double rowCount = getEstimatedMaxOutputRowCount(planNode, statsProvider);

            return !isNaN(rowCount) && rowCount < smallDynamicFilterMaxRowCount;
        }
    }

    private static boolean isInputMultiplyingPlanNodePresent(PlanNode root)
    {
        return PlanNodeSearcher.searchFrom(root)
                .where(DeterminePreferredDynamicFilterTimeout::isInputMultiplyingPlanNode)
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
}
