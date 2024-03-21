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
package io.trino.cost;

import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.cost.StatsCalculator.Context;
import io.trino.matching.Pattern;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Not;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SemiJoinNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.trino.cost.SemiJoinStatsCalculator.computeAntiJoin;
import static io.trino.cost.SemiJoinStatsCalculator.computeSemiJoin;
import static io.trino.sql.ir.IrUtils.combineConjuncts;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.plan.Patterns.filter;
import static java.util.Objects.requireNonNull;

/**
 * It is not yet proven whether this heuristic is any better or worse. Either this rule will be enhanced
 * in the future or it will be dropped altogether.
 */
public class SimpleFilterProjectSemiJoinStatsRule
        extends SimpleStatsRule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FilterStatsCalculator filterStatsCalculator;

    public SimpleFilterProjectSemiJoinStatsRule(StatsNormalizer normalizer, FilterStatsCalculator filterStatsCalculator)
    {
        super(normalizer);
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator cannot be null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(FilterNode node, Context context)
    {
        PlanNode nodeSource = context.lookup().resolve(node.getSource());
        SemiJoinNode semiJoinNode;
        if (nodeSource instanceof ProjectNode projectNode) {
            if (!projectNode.isIdentity()) {
                return Optional.empty();
            }
            PlanNode projectNodeSource = context.lookup().resolve(projectNode.getSource());
            if (!(projectNodeSource instanceof SemiJoinNode value)) {
                return Optional.empty();
            }
            semiJoinNode = value;
        }
        else if (nodeSource instanceof SemiJoinNode value) {
            semiJoinNode = value;
        }
        else {
            return Optional.empty();
        }

        return calculate(node, semiJoinNode, context.statsProvider(), context.session());
    }

    private Optional<PlanNodeStatsEstimate> calculate(FilterNode filterNode, SemiJoinNode semiJoinNode, StatsProvider statsProvider, Session session)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(semiJoinNode.getSource());
        PlanNodeStatsEstimate filteringSourceStats = statsProvider.getStats(semiJoinNode.getFilteringSource());
        Symbol filteringSourceJoinSymbol = semiJoinNode.getFilteringSourceJoinSymbol();
        Symbol sourceJoinSymbol = semiJoinNode.getSourceJoinSymbol();

        Optional<SemiJoinOutputFilter> semiJoinOutputFilter = extractSemiJoinOutputFilter(filterNode.getPredicate(), semiJoinNode.getSemiJoinOutput());

        if (semiJoinOutputFilter.isEmpty()) {
            return Optional.empty();
        }

        PlanNodeStatsEstimate semiJoinStats;
        if (semiJoinOutputFilter.get().isNegated()) {
            semiJoinStats = computeAntiJoin(sourceStats, filteringSourceStats, sourceJoinSymbol, filteringSourceJoinSymbol);
        }
        else {
            semiJoinStats = computeSemiJoin(sourceStats, filteringSourceStats, sourceJoinSymbol, filteringSourceJoinSymbol);
        }

        if (semiJoinStats.isOutputRowCountUnknown()) {
            return Optional.of(PlanNodeStatsEstimate.unknown());
        }

        // apply remaining predicate
        PlanNodeStatsEstimate filteredStats = filterStatsCalculator.filterStats(semiJoinStats, semiJoinOutputFilter.get().getRemainingPredicate(), session);
        if (filteredStats.isOutputRowCountUnknown()) {
            return Optional.of(semiJoinStats.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT));
        }
        return Optional.of(filteredStats);
    }

    private Optional<SemiJoinOutputFilter> extractSemiJoinOutputFilter(Expression predicate, Symbol semiJoinOutput)
    {
        List<Expression> conjuncts = extractConjuncts(predicate);
        List<Expression> semiJoinOutputReferences = conjuncts.stream()
                .filter(conjunct -> isSemiJoinOutputReference(conjunct, semiJoinOutput))
                .collect(toImmutableList());

        if (semiJoinOutputReferences.size() != 1) {
            return Optional.empty();
        }

        Expression semiJoinOutputReference = Iterables.getOnlyElement(semiJoinOutputReferences);
        Expression remainingPredicate = combineConjuncts(conjuncts.stream()
                .filter(conjunct -> conjunct != semiJoinOutputReference)
                .collect(toImmutableList()));
        boolean negated = semiJoinOutputReference instanceof Not;
        return Optional.of(new SemiJoinOutputFilter(negated, remainingPredicate));
    }

    private static boolean isSemiJoinOutputReference(Expression conjunct, Symbol semiJoinOutput)
    {
        Reference semiJoinOutputSymbolReference = semiJoinOutput.toSymbolReference();
        return conjunct.equals(semiJoinOutputSymbolReference) || (conjunct instanceof Not not && not.value().equals(semiJoinOutputSymbolReference));
    }

    private static class SemiJoinOutputFilter
    {
        private final boolean negated;
        private final Expression remainingPredicate;

        public SemiJoinOutputFilter(boolean negated, Expression remainingPredicate)
        {
            this.negated = negated;
            this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate cannot be null");
        }

        public boolean isNegated()
        {
            return negated;
        }

        public Expression getRemainingPredicate()
        {
            return remainingPredicate;
        }
    }
}
