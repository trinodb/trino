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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
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
import static io.trino.sql.ir.IrExpressions.matchNullIf;

/**
 * Transforms:
 * <pre>{@code op(v, Case(When(c1, r1), When(c2, r2), ..)))}</pre>
 *
 * into:
 * <pre>{@code Case(When(c1, op(v, r1)), When(c2, op(v, r2)), ..)))}</pre>
 */
public class DistributeComparisonOverCase
        implements IrOptimizerRule
{
    private final Metadata metadata;

    public DistributeComparisonOverCase(PlannerContext context)
    {
        this.metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, SymbolAllocator symbolAllocator, Map<Symbol, Expression> bindings)
    {
        if (!(matchComparison(expression) instanceof Comparison comparison)) {
            return Optional.empty();
        }

        if (comparison.left() instanceof Case caseTerm && (comparison.right() instanceof Reference || comparison.right() instanceof Constant)) {
            // Leave a desugared NULLIF intact so it stays recognizable for connector pushdown.
            if (matchNullIf(caseTerm) != null) {
                return Optional.empty();
            }
            return Optional.of(distribute(comparison.operator(), caseTerm, comparison.right()));
        }

        if (comparison.right() instanceof Case caseTerm && (comparison.left() instanceof Reference || comparison.left() instanceof Constant)) {
            if (matchNullIf(caseTerm) != null) {
                return Optional.empty();
            }
            return Optional.of(distribute(flipOperator(comparison.operator()), caseTerm, comparison.left()));
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

    private Expression distribute(ComparisonOperator operator, Case caseTerm, Expression target)
    {
        return new Case(
                caseTerm.whenClauses().stream()
                        .map(clause -> new WhenClause(
                                clause.getOperand(),
                                comparison(metadata, operator, clause.getResult(), target)))
                        .toList(),
                comparison(metadata, operator, caseTerm.defaultValue(), target));
    }
}
