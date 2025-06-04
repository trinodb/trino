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
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Flatten nested coalesce expressions. E.g,
 * <ul>
 *     <li>{@code Coalesce(Coalesce(a, b), Coalesce(c, d), ...) -> Coalesce(a, b, c, d, ...)}
 * </ul>
 */
public class FlattenCoalesce
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Coalesce coalesce)) {
            return Optional.empty();
        }

        if (coalesce.operands().stream().noneMatch(e -> e instanceof Coalesce)) {
            return Optional.empty();
        }

        return Optional.of(new Coalesce(flatten(coalesce).toList()));
    }

    private Stream<Expression> flatten(Coalesce coalesce)
    {
        return coalesce.operands().stream()
                .flatMap(this::flatten);
    }

    private Stream<Expression> flatten(Expression expression)
    {
        if (expression instanceof Coalesce coalesce) {
            return flatten(coalesce);
        }
        else {
            return Stream.of(expression);
        }
    }
}
