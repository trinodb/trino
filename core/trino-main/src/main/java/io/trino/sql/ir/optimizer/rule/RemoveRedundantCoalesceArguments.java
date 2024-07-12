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
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

/**
 * Remove duplicate deterministic arguments and any argument after the first
 * non-null constant. E.g,
 * <ul>
 *     <li>{@code Coalesce(a, b, c, a, d) -> Coalesce(a, b, c, d)}
 *     <li>{@code Coalesce(a, b, 'hello', c, d) -> Coalesce(a, b, 'hello')}
 * </ul>
 */
public class RemoveRedundantCoalesceArguments
        implements IrOptimizerRule
{
    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Coalesce coalesce)) {
            return Optional.empty();
        }

        ImmutableList.Builder<Expression> arguments = ImmutableList.builder();

        boolean removed = false;
        Set<Expression> seen = new HashSet<>();
        int last = 0;
        for (int i = 0; i < coalesce.operands().size(); i++) {
            Expression argument = coalesce.operands().get(i);
            last = i;

            if (seen.contains(argument) || (argument instanceof Constant constant && constant.value() == null)) {
                removed = true;
            }
            else {
                arguments.add(argument);
                if (isDeterministic(argument)) {
                    seen.add(argument);
                }
            }

            if (argument instanceof Constant constant && constant.value() != null) {
                break;
            }
        }

        if (!removed && last == coalesce.operands().size() - 1) {
            return Optional.empty();
        }

        List<Expression> newArguments = arguments.build();
        if (newArguments.isEmpty()) {
            return Optional.of(new Constant(expression.type(), null));
        }
        else if (newArguments.size() == 1) {
            return Optional.of(newArguments.getFirst());
        }

        return Optional.of(new Coalesce(newArguments));
    }
}
