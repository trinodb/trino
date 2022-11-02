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
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.tree.Expression;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.isRewriteFilteringSemiJoinToInnerJoin;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.ExpressionUtils.and;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.plan.AggregationNode.singleAggregation;
import static io.trino.sql.planner.plan.AggregationNode.singleGroupingSet;
import static io.trino.sql.planner.plan.JoinNode.Type.INNER;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.semiJoin;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.function.Predicate.not;

/**
 * Rewrite filtering semi-join to inner join.
 * <p/>
 * Transforms:
 * <pre>
 * - Filter (semiJoinSymbol AND predicate)
 *    - SemiJoin (semiJoinSymbol <- (a IN b))
 *        source: plan A producing symbol a
 *        filtering source: plan B producing symbol b
 * </pre>
 * <p/>
 * Into:
 * <pre>
 * - Project (semiJoinSymbol <- TRUE)
 *    - Join INNER on (a = b), joinFilter (predicate with semiJoinSymbol replaced with TRUE)
 *       - source
 *       - Aggregation distinct(b)
 *          - filtering source
 * </pre>
 */
public class TransformFilteringSemiJoinToInnerJoin
        implements Rule<FilterNode>
{
    private static final Capture<SemiJoinNode> SEMI_JOIN = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(semiJoin().capturedAs(SEMI_JOIN)));

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRewriteFilteringSemiJoinToInnerJoin(session);
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        SemiJoinNode semiJoin = captures.get(SEMI_JOIN);

        // Do not transform semi-join in context of DELETE
        if (PlanNodeSearcher.searchFrom(semiJoin.getSource(), context.getLookup())
                .where(node -> node instanceof TableScanNode && ((TableScanNode) node).isUpdateTarget())
                .matches()) {
            return Result.empty();
        }

        Symbol semiJoinSymbol = semiJoin.getSemiJoinOutput();
        Predicate<Expression> isSemiJoinSymbol = expression -> expression.equals(semiJoinSymbol.toSymbolReference());

        List<Expression> conjuncts = extractConjuncts(filterNode.getPredicate());
        if (conjuncts.stream().noneMatch(isSemiJoinSymbol)) {
            return Result.empty();
        }
        Expression filteredPredicate = and(conjuncts.stream()
                .filter(not(isSemiJoinSymbol))
                .collect(toImmutableList()));

        Expression simplifiedPredicate = inlineSymbols(symbol -> {
            if (symbol.equals(semiJoinSymbol)) {
                return TRUE_LITERAL;
            }
            return symbol.toSymbolReference();
        }, filteredPredicate);

        Optional<Expression> joinFilter = simplifiedPredicate.equals(TRUE_LITERAL) ? Optional.empty() : Optional.of(simplifiedPredicate);

        PlanNode filteringSourceDistinct = singleAggregation(
                context.getIdAllocator().getNextId(),
                semiJoin.getFilteringSource(),
                ImmutableMap.of(),
                singleGroupingSet(ImmutableList.of(semiJoin.getFilteringSourceJoinSymbol())));

        JoinNode innerJoin = new JoinNode(
                semiJoin.getId(),
                INNER,
                semiJoin.getSource(),
                filteringSourceDistinct,
                ImmutableList.of(new EquiJoinClause(semiJoin.getSourceJoinSymbol(), semiJoin.getFilteringSourceJoinSymbol())),
                semiJoin.getSource().getOutputSymbols(),
                ImmutableList.of(),
                false,
                joinFilter,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                semiJoin.getDynamicFilterId()
                        .map(id -> ImmutableMap.of(id, semiJoin.getFilteringSourceJoinSymbol()))
                        .orElse(ImmutableMap.of()),
                Optional.empty());

        ProjectNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                innerJoin,
                Assignments.builder()
                        .putIdentities(innerJoin.getOutputSymbols())
                        .put(semiJoinSymbol, TRUE_LITERAL)
                        .build());

        return Result.ofPlanNode(project);
    }
}
