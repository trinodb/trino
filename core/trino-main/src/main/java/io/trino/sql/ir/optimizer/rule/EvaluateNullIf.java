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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.NullIf;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;
import io.trino.type.TypeCoercion;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.sql.ir.IrExpressions.isConstantNull;

/**
 * Evaluates a constant NullIf expression
 */
public class EvaluateNullIf
        implements IrOptimizerRule
{
    private final TypeCoercion typeCoercion;
    private final Metadata metadata;
    private final InterpretedFunctionInvoker functionInvoker;

    public EvaluateNullIf(PlannerContext plannerContext)
    {
        this.metadata = plannerContext.getMetadata();
        this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
        this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof NullIf(Expression first, Expression second))) {
            return Optional.empty();
        }

        if (isConstantNull(first)) {
            return Optional.of(new Constant(first.type(), null));
        }

        if (isConstantNull(second)) {
            return Optional.of(first);
        }

        ConnectorSession connectorSession = session.toConnectorSession();
        if (first instanceof Constant(Type firstType, Object firstValue) && second instanceof Constant(Type secondType, Object secondValue)) {
            Type commonType = typeCoercion.getCommonSuperType(firstType, secondType).orElseThrow();

            // cast(first as <common type>) == cast(second as <common type>)
            boolean equal = Boolean.TRUE.equals(
                    functionInvoker.invoke(
                            metadata.resolveOperator(EQUAL, ImmutableList.of(commonType, commonType)),
                            connectorSession,
                            ImmutableList.of(
                                    functionInvoker.invoke(metadata.getCoercion(firstType, commonType), connectorSession, ImmutableList.of(firstValue)),
                                    functionInvoker.invoke(metadata.getCoercion(secondType, commonType), connectorSession, ImmutableList.of(secondValue)))));

            return Optional.of(equal ? new Constant(firstType, null) : first);
        }

        return Optional.empty();
    }
}
