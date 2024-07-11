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
import io.trino.spi.function.FunctionNullability;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.ir.IrExpressions.isConstantNull;

/**
 * Evaluates a constant Call expression where at least one of the arguments is null
 */
public class EvaluateCallWithNullInput
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Call call)) {
            return Optional.empty();
        }

        FunctionNullability nullability = call.function().functionNullability();
        for (int i = 0; i < call.arguments().size(); i++) {
            if (isConstantNull(call.arguments().get(i)) && !nullability.isArgumentNullable(i)) {
                return Optional.of(new Constant(expression.type(), null));
            }
        }

        return Optional.empty();
    }
}
