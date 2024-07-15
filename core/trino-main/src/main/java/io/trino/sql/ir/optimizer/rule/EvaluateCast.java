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
import io.trino.metadata.Metadata;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonList;

/**
 * Evaluates a constant Cast expression
 */
public class EvaluateCast
        implements IrOptimizerRule
{
    private final InterpretedFunctionInvoker functionInvoker;
    private final Metadata metadata;

    public EvaluateCast(PlannerContext context)
    {
        metadata = context.getMetadata();
        functionInvoker = new InterpretedFunctionInvoker(context.getFunctionManager());
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (expression instanceof Cast(Constant constant, Type type)) {
            try {
                return Optional.of(new Constant(
                        type,
                        functionInvoker.invoke(
                                metadata.getCoercion(constant.type(), type),
                                session.toConnectorSession(),
                                singletonList(constant.value()))));
            }
            catch (TrinoException _) {
            }
        }

        return Optional.empty();
    }
}
