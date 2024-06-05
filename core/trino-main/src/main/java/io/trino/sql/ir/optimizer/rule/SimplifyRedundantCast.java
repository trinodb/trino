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
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;

/**
 * Remove casts where the target type matches the type of the input expression. E.g,
 * <ul>
 *     <li>{@code Cast(x::bigint, bigint) -> x}
 * </ul>
 */
public class SimplifyRedundantCast
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (expression instanceof Cast cast && cast.type().equals(cast.expression().type())) {
            return Optional.of(cast.expression());
        }

        return Optional.empty();
    }
}
