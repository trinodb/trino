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
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.Booleans.TRUE;

/**
 * Evaluates a constant Case expression: {@code Case(When(TRUE, r), When(...), When(...)} -> {@code r}
 */
public class EvaluateCase
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Case(List<WhenClause> whenClauses, Expression defaultValue))) {
            return Optional.empty();
        }

        for (WhenClause whenClause : whenClauses) {
            Expression operand = whenClause.getOperand();

            if (operand.equals(TRUE)) {
                return Optional.of(whenClause.getResult());
            }
            else if (!(operand instanceof Constant)) {
                return Optional.empty();
            }
        }

        return Optional.of(defaultValue);
    }
}
