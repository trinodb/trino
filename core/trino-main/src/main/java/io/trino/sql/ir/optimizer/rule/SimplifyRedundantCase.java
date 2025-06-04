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

import com.google.common.collect.ImmutableList;
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

import java.util.ArrayList;
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

        return transformRecursive(0, caseTerm.whenClauses(), defaultValue)
                .or(() -> Optional.<Expression>of(FALSE));
    }

    private Optional<Expression> transformRecursive(int start, List<WhenClause> clauses, Expression defaultExpression)
    {
        // An expression such as:
        // CASE
        //   WHEN a1 THEN false
        //   WHEN a2 THEN false
        //   WHEN a3 THEN true
        //   WHEN a4 THEN false
        //   WHEN a5 THEN true
        //   WHEN a6 THEN false
        //   ELSE true
        // END
        //
        // can be transformed to:
        //     (a1 ≢ true AND a2 ≢ true AND a3 ≡ true) OR
        //     (a1 ≢ true AND a2 ≢ true AND a3 ≢ true AND a4 ≢ true AND a5 ≡ true) OR
        //     (a1 ≢ true AND a2 ≢ true AND a3 ≢ true AND a4 ≢ true AND a5 ≢ true AND a6 ≢ true)
        //
        // which can be further simplified to:
        //
        //     a1 ≢ true AND a2 ≢ true AND (a3 ≡ true OR (a4 ≢ true AND (a5 ≡ true OR a6 ≢ true)))
        //
        // This method constructs the simplified expression recursively.

        int end = start;
        while (end < clauses.size() && clauses.get(end).getResult().equals(FALSE)) {
            end++;
        }

        List<Expression> falseTerms = clauses.subList(start, end).stream()
                .map(clause -> IrExpressions.not(metadata, new Comparison(Comparison.Operator.IDENTICAL, clause.getOperand(), TRUE)))
                .toList();

        if (end < clauses.size()) {
            List<Expression> terms = new ArrayList<>();
            terms.add(new Comparison(Comparison.Operator.IDENTICAL, clauses.get(end).getOperand(), TRUE));
            transformRecursive(end + 1, clauses, defaultExpression).ifPresent(terms::add);

            return Optional.of(IrUtils.and(
                    ImmutableList.<Expression>builder()
                            .addAll(falseTerms)
                            .add(IrUtils.or(terms))
                            .build()));
        }
        else if (defaultExpression.equals(TRUE)) {
            return Optional.of(IrUtils.and(falseTerms));
        }
        else {
            return Optional.empty();
        }
    }
}
