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
package io.prestosql.sql.planner.iterative.rule;

import io.prestosql.Session;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.cost.StatsProvider;
import io.prestosql.cost.SymbolStatsEstimate;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.JoinNode.EquiJoinClause;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.prestosql.SystemSessionProperties.getOptimizeNullsThreshold;
import static io.prestosql.SystemSessionProperties.isOptimizeNullsInJoin;
import static io.prestosql.sql.planner.plan.JoinNode.Type.FULL;
import static io.prestosql.sql.planner.plan.Patterns.join;

public class InferNotNullThroughJoin
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(joinNode -> joinNode.getType() != FULL && !joinNode.getCriteria().isEmpty());

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isOptimizeNullsInJoin(session);
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        Expression filter = joinNode.getFilter().orElse(null);
        HashSet<Symbol> filteredSymbols = new HashSet<>();
        extractNotNullSymbols(filteredSymbols, filter);
        List<Symbol> leftSymbols = joinKeys(joinNode, EquiJoinClause::getLeft, filteredSymbols);
        List<Symbol> rightSymbols = joinKeys(joinNode, EquiJoinClause::getRight, filteredSymbols);

        StatsProvider statsProvider = context.getStatsProvider();
        PlanNodeStatsEstimate leftStatsEstimate = statsProvider.getStats(joinNode.getLeft());
        PlanNodeStatsEstimate rightStatsEstimate = statsProvider.getStats(joinNode.getRight());

        double threshold = getOptimizeNullsThreshold(context.getSession());
        Expression newFilter = filter;
        switch (joinNode.getType()) {
            case INNER:
                newFilter = inferPredicatesIfNecessary(newFilter, leftSymbols, leftStatsEstimate, threshold);
                newFilter = inferPredicatesIfNecessary(newFilter, rightSymbols, rightStatsEstimate, threshold);
                break;
            case LEFT:
                newFilter = inferPredicatesIfNecessary(newFilter, rightSymbols, rightStatsEstimate, threshold);
                break;
            case RIGHT:
                newFilter = inferPredicatesIfNecessary(newFilter, leftSymbols, leftStatsEstimate, threshold);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + joinNode.getType());
        }
        if (newFilter == filter) {
            return Result.empty();
        }
        else {
            return Result.ofPlanNode(joinNode.withFilter(newFilter));
        }
    }

    private List<Symbol> joinKeys(JoinNode joinNode, Function<EquiJoinClause, Symbol> extractor, Set<Symbol> filteredSymbols)
    {
        return joinNode.getCriteria().stream()
                .map(extractor)
                .filter(key -> !filteredSymbols.contains(key))
                .collect(Collectors.toList());
    }

    private Expression inferPredicatesIfNecessary(Expression filter, List<Symbol> symbols, PlanNodeStatsEstimate statsEstimate, double threshold)
    {
        Expression newFilter = filter;
        for (Symbol symbol : symbols) {
            SymbolStatsEstimate symbolStats = statsEstimate.getSymbolStatistics(symbol);
            double nullsFraction = symbolStats.isUnknown() ? 0 : symbolStats.getNullsFraction();
            if (nullsFraction >= threshold) {
                IsNotNullPredicate predicate = new IsNotNullPredicate(new SymbolReference(symbol.getName()));
                newFilter = newFilter == null ? predicate : LogicalBinaryExpression.and(newFilter, predicate);
            }
        }
        return newFilter;
    }

    private void extractNotNullSymbols(HashSet<Symbol> result, Expression expression)
    {
        if (expression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression logicalBinaryExpression = (LogicalBinaryExpression) expression;
            Expression left = logicalBinaryExpression.getLeft();
            Expression right = logicalBinaryExpression.getRight();

            extractNotNullSymbols(result, left);
            extractNotNullSymbols(result, right);
        }
        else if (expression instanceof IsNotNullPredicate) {
            Expression predicateExpression = ((IsNotNullPredicate) expression).getValue();
            if (predicateExpression instanceof SymbolReference) {
                String symbolName = ((SymbolReference) predicateExpression).getName();
                result.add(new Symbol(symbolName));
            }
        }
    }
}
