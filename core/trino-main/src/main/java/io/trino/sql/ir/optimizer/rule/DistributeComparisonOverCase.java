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
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN_OR_EQUAL;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;

/**
 * Transforms:
 * <pre>{@code Comparison(op, v, Case(When(c1, r1), When(c2, r2), ..)))}</pre>
 *
 * into:
 * <pre>{@code Case(When(c1, Comparison(op, v, r1)), When(c2, Comparison(op, v, r2)), ..)))}</pre>
 */
public class DistributeComparisonOverCase
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (expression instanceof Comparison(Comparison.Operator operator, Case caseTerm, Expression target) &&
                (target instanceof Reference || target instanceof Constant)) {
            return Optional.of(distribute(operator, caseTerm, target));
        }

        if (expression instanceof Comparison(Comparison.Operator operator, Expression target, Case caseTerm) &&
                (target instanceof Reference || target instanceof Constant)) {
            return Optional.of(distribute(flipOperator(operator), caseTerm, target));
        }

        return Optional.empty();
    }

    private Comparison.Operator flipOperator(Comparison.Operator operator)
    {
        return switch (operator) {
            case IDENTICAL, EQUAL, NOT_EQUAL -> operator;
            case LESS_THAN -> GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL;
            case GREATER_THAN -> LESS_THAN;
            case GREATER_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL;
        };
    }

    private Expression distribute(Comparison.Operator operator, Case caseTerm, Expression target)
    {
        return new Case(
                caseTerm.whenClauses().stream()
                        .map(clause -> new WhenClause(
                                clause.getOperand(),
                                new Comparison(operator, clause.getResult(), target)))
                        .toList(),
                new Comparison(operator, caseTerm.defaultValue(), target));
    }
}
