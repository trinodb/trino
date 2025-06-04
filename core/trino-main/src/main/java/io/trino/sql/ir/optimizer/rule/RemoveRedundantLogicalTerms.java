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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

/**
 * Remove duplicated and redundant terms in logical expressions. E.g.,
 * <ul>
 *     <li>{@code And(a, b, a, true, c) -> And(a, b, c)}
 *     <li>{@code Or(a, b, a, false, c) -> Or(a, b, c)}
 *     <li>{@code And(a, a) -> a}
 * </ul>
 */
public class RemoveRedundantLogicalTerms
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Logical logical)) {
            return Optional.empty();
        }

        ImmutableList.Builder<Expression> terms = ImmutableList.builder();

        boolean removed = false;
        Set<Expression> seen = new HashSet<>();
        for (Expression term : logical.terms()) {
            if (logical.operator() == AND && term.equals(TRUE) || logical.operator() == OR && term.equals(FALSE) || seen.contains(term)) {
                removed = true;
            }
            else {
                terms.add(term);

                if (isDeterministic(term)) {
                    seen.add(term);
                }
            }
        }

        if (!removed) {
            return Optional.empty();
        }

        List<Expression> newTerms = terms.build();
        if (newTerms.size() == 1) {
            return Optional.of(newTerms.getFirst());
        }

        return Optional.of(new Logical(logical.operator(), newTerms));
    }
}
