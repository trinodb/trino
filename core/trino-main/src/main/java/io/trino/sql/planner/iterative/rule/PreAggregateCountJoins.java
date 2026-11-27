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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.operator.aggregation.CountJoinAggregation;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.PRE_AGGREGATE_COUNT_JOINS;
import static io.trino.SystemSessionProperties.getCharVarcharCoercion;
import static io.trino.cost.AggregationStatsRule.groupBy;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static java.lang.Double.isFinite;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Compress both sides of a COUNT(*) equijoin, retaining join keys and output grouping
 * keys. Each compressed match represents left_count * right_count original matches.
 * Runs on SINGLE aggregations, before exchange planning splits all three aggregations.
 */
public class PreAggregateCountJoins
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();

    private final Metadata metadata;

    public PreAggregateCountJoins(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return session.getSystemProperty(PRE_AGGREGATE_COUNT_JOINS, Boolean.class);
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        PlanNode source = context.getLookup().resolve(node.getSource());
        // Column pruning can leave identity projections between the aggregation and join.
        // Computed expressions are deliberately unsupported.
        while (source instanceof ProjectNode project && project.isIdentity()) {
            source = context.getLookup().resolve(project.getSource());
        }
        if (!(source instanceof JoinNode join)) {
            return Result.empty();
        }
        if (node.getStep() != SINGLE || node.getGroupingSetCount() != 1 || node.getGroupIdSymbol().isPresent()
                || node.isStreamable() || node.getAggregations().size() != 1
                || join.getType() != INNER || join.getCriteria().isEmpty() || join.getFilter().isPresent()
                || join.isMaySkipOutputDuplicates()) {
            return Result.empty();
        }

        Aggregation count = node.getAggregations().values().iterator().next();
        ResolvedFunction countFunction = metadata.resolveBuiltinFunction(getCharVarcharCoercion(context.getSession()), "count", ImmutableList.of());
        if (!count.getResolvedFunction().equals(countFunction) || !count.getArguments().isEmpty()
                || count.isDistinct() || count.getFilter().isPresent() || count.getMask().isPresent() || count.getOrderingScheme().isPresent()) {
            return Result.empty();
        }

        List<Symbol> leftKeys = groupingKeys(node, join.getLeft(), join.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft).collect(toImmutableList()));
        List<Symbol> rightKeys = groupingKeys(node, join.getRight(), join.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight).collect(toImmutableList()));
        PlanNodeStatsEstimate leftStats = context.getStatsProvider().getStats(join.getLeft());
        PlanNodeStatsEstimate rightStats = context.getStatsProvider().getStats(join.getRight());
        double joinRows = context.getStatsProvider().getStats(join).getOutputRowCount();
        // Require expansion and at least fourfold compression on EACH side. Unknown estimates
        // and high-NDV output grouping keys must not introduce two blocking aggregations.
        if (!isFinite(joinRows) || joinRows <= 1.1 * max(leftStats.getOutputRowCount(), rightStats.getOutputRowCount())
                || !isReducing(leftStats, leftKeys) || !isReducing(rightStats, rightKeys)) {
            return Result.empty();
        }

        Symbol leftCount = context.getSymbolAllocator().newSymbol("left_count", BIGINT);
        Symbol rightCount = context.getSymbolAllocator().newSymbol("right_count", BIGINT);
        AggregationNode left = singleAggregation(context.getIdAllocator().getNextId(), join.getLeft(), ImmutableMap.of(leftCount, count), singleGroupingSet(leftKeys));
        AggregationNode right = singleAggregation(context.getIdAllocator().getNextId(), join.getRight(), ImmutableMap.of(rightCount, count), singleGroupingSet(rightKeys));
        JoinNode compressedJoin = new JoinNode(
                context.getIdAllocator().getNextId(),
                INNER,
                left,
                right,
                join.getCriteria(),
                left.getOutputSymbols(),
                right.getOutputSymbols(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                join.getDynamicFilters(),
                Optional.empty());
        ResolvedFunction countJoin = metadata.resolveBuiltinFunction(getCharVarcharCoercion(context.getSession()), CountJoinAggregation.NAME, ImmutableList.of(BIGINT, BIGINT));
        Aggregation result = new Aggregation(countJoin, ImmutableList.of(leftCount.toSymbolReference(), rightCount.toSymbolReference()), false, Optional.empty(), Optional.empty(), Optional.empty());
        // The replacement is not COUNT(*), so the rule cannot repeatedly rewrite this join.
        return Result.ofPlanNode(AggregationNode.builderFrom(node)
                .setSource(compressedJoin)
                .setAggregations(ImmutableMap.of(node.getAggregations().keySet().iterator().next(), result))
                .setPreGroupedSymbols(ImmutableList.of())
                .build());
    }

    private static List<Symbol> groupingKeys(AggregationNode node, PlanNode source, List<Symbol> joinKeys)
    {
        return ImmutableSet.<Symbol>builder()
                .addAll(joinKeys)
                .addAll(node.getGroupingKeys().stream().filter(source.getOutputSymbols()::contains).collect(toImmutableList()))
                .build().asList();
    }

    private static boolean isReducing(PlanNodeStatsEstimate stats, List<Symbol> keys)
    {
        double rows = stats.getOutputRowCount();
        double groups = groupBy(stats, keys, ImmutableMap.of()).getOutputRowCount();
        return isFinite(rows) && rows > 0 && isFinite(groups) && groups <= rows / 4;
    }
}
