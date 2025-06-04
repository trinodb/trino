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
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;

/**
 * Simplify stacked Not expressions. E.g,
 * <ul>
 *     <li>{@code $not($not(x)) -> x}
 * </ul>
 */
public class SimplifyStackedNot
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (expression instanceof Call outer &&
                outer.function().name().equals(builtinFunctionName("$not")) &&
                outer.arguments().getFirst() instanceof Call inner &&
                inner.function().name().equals(builtinFunctionName("$not"))) {
            return Optional.of(inner.arguments().getFirst());
        }

        return Optional.empty();
    }
}
