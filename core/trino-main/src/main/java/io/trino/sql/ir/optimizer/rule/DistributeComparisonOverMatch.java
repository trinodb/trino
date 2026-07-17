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
package io.trino.sql.ir.optimizer.rule;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Match;
import io.trino.sql.ir.MatchClause;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.matchComparison;

/**
 * Transforms:
 * <pre>{@code op(v, Match(x, When(c1, r1), When(c2, r2), ..)))}</pre>
 *
 * into:
 * <pre>{@code Match(x, When(c1, op(v, r1)), When(c2, op(v, r2)), ..)))}</pre>
 */
public class DistributeComparisonOverMatch
        implements IrOptimizerRule
{
    private final Metadata metadata;

    public DistributeComparisonOverMatch(PlannerContext context)
    {
        this.metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(matchComparison(expression) instanceof Comparison comparison)) {
            return Optional.empty();
        }

        if (comparison.left() instanceof Match match && (comparison.right() instanceof Reference || comparison.right() instanceof Constant)) {
            return Optional.of(distribute(comparison.operator(), match, comparison.right()));
        }

        if (comparison.right() instanceof Match match && (comparison.left() instanceof Reference || comparison.left() instanceof Constant)) {
            return Optional.of(distribute(flipOperator(comparison.operator()), match, comparison.left()));
        }

        return Optional.empty();
    }

    private ComparisonOperator flipOperator(ComparisonOperator operator)
    {
        return switch (operator) {
            case IDENTICAL, EQUAL, NOT_EQUAL -> operator;
            case LESS_THAN -> GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL;
            case GREATER_THAN -> LESS_THAN;
            case GREATER_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL;
        };
    }

    private Expression distribute(ComparisonOperator operator, Match match, Expression target)
    {
        return new Match(
                match.operand(),
                match.clauses().stream()
                        .map(clause -> new MatchClause(
                                clause.predicate(),
                                comparison(metadata, operator, clause.result(), target)))
                        .toList(),
                comparison(metadata, operator, match.defaultValue(), target));
    }
}
