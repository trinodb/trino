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
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;

/**
 * Evaluates a constant logical expression. E.g.,
 * <ul>
 *     <li>{@code And(..., false, ...) -> false}</li>
 *     <li>{@code Or(..., true, ...) -> true}</li>
 * </ul>
 */
public class EvaluateLogical
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Logical(Logical.Operator operator, List<Expression> terms))) {
            return Optional.empty();
        }

        Expression shortCircuit = switch (operator) {
            case AND -> FALSE;
            case OR -> TRUE;
        };

        Expression ignore = switch (operator) {
            case AND -> TRUE;
            case OR -> FALSE;
        };

        if (terms.stream().anyMatch(shortCircuit::equals)) {
            return Optional.of(shortCircuit);
        }

        List<Expression> residuals = terms.stream()
                .filter(term -> !term.equals(ignore))
                .toList();

        if (residuals.isEmpty()) {
            return Optional.of(ignore);
        }

        if (residuals.stream().allMatch(IrExpressions::isConstantNull)) {
            return Optional.of(NULL_BOOLEAN);
        }

        return Optional.empty();
    }
}
