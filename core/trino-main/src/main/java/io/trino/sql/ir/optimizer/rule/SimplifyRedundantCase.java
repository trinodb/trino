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
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

/**
 * Simplify CASE expressions with constant TRUE/FALSE results:
 *
 * <ul>
 *     <li>{@code Case([When(a, true), When(b, false), When(c, true)], false) -> $identical(Or(a, c), true)}
 *     <li>{@code Case([When(a, true), When(b, false), When(c, false)], true) -> $not($identical(Or(b, c), true)}
 * </ul>
 */
public class SimplifyRedundantCase
        implements IrOptimizerRule
{
    private final Metadata metadata;

    public SimplifyRedundantCase(PlannerContext context)
    {
        metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Case caseTerm)) {
            return Optional.empty();
        }

        Expression defaultValue = caseTerm.defaultValue();
        if (!caseTerm.whenClauses().stream().map(WhenClause::getResult).allMatch(result -> result.equals(TRUE) || result.equals(FALSE)) ||
                (!defaultValue.equals(TRUE) && !defaultValue.equals(FALSE)) ||
                caseTerm.whenClauses().stream().map(WhenClause::getOperand).anyMatch(e -> !isDeterministic(e))) {
            return Optional.empty();
        }

        if (defaultValue.equals(FALSE)) {
            List<Expression> operands = caseTerm.whenClauses().stream()
                    .filter(clause -> clause.getResult().equals(TRUE))
                    .map(WhenClause::getOperand)
                    .toList();

            return Optional.of(new Comparison(Comparison.Operator.IDENTICAL, IrUtils.or(operands), TRUE));
        }
        else {
            List<Expression> operands = caseTerm.whenClauses().stream()
                    .filter(clause -> clause.getResult().equals(FALSE))
                    .map(WhenClause::getOperand)
                    .toList();

            return Optional.of(IrExpressions.not(metadata, new Comparison(Comparison.Operator.IDENTICAL, IrUtils.or(operands), TRUE)));
        }
    }
}
