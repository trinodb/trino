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
import io.trino.spi.TrinoException;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonList;

/**
 * Simplifies IN expression with a Cast on value by attempting to cast the values list to the value type. E.g.,
 * <ul>
 *     <li>{@code $in($cast(x, varchar), ['1', '2', '3', '4']) -> $in(x, 1, 2, 3, 4)}
 * </ul>
 */
public class UnwrapInCast
        implements IrOptimizerRule
{
    private final Metadata metadata;
    private final InterpretedFunctionInvoker functionInvoker;

    public UnwrapInCast(PlannerContext context)
    {
        metadata = context.getMetadata();
        functionInvoker = new InterpretedFunctionInvoker(context.getFunctionManager());
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof In(Cast cast, List<Expression> list))) {
            return Optional.empty();
        }

        ImmutableList.Builder<Expression> builder = ImmutableList.builder();
        for (Expression item : list) {
            if (item instanceof Constant constant) {
                try {
                    builder.add(new Constant(
                            cast.expression().type(),
                            functionInvoker.invoke(
                                    metadata.getCoercion(constant.type(), cast.expression().type()),
                                    session.toConnectorSession(),
                                    singletonList(constant.value()))));
                }
                catch (TrinoException _) {
                    return Optional.empty();
                }
            }
            else {
                return Optional.empty();
            }
        }

        return Optional.of(new In(cast.expression(), builder.build()));
    }
}
