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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.function.OperatorType.EQUAL;

/**
 * Evaluates a constant Switch expression
 */
public class EvaluateSwitch
        implements IrOptimizerRule
{
    private final Metadata metadata;
    private final InterpretedFunctionInvoker functionInvoker;

    public EvaluateSwitch(PlannerContext context)
    {
        metadata = context.getMetadata();
        functionInvoker = new InterpretedFunctionInvoker(context.getFunctionManager());
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Switch(Expression operand, List<WhenClause> whenClauses, Expression defaultValue))) {
            return Optional.empty();
        }

        if (!(operand instanceof Constant constantOperand)) {
            return Optional.empty();
        }

        if (constantOperand.value() == null) {
            return Optional.of(defaultValue);
        }

        ConnectorSession connectorSession = session.toConnectorSession();
        ResolvedFunction equals = metadata.resolveOperator(EQUAL, ImmutableList.of(operand.type(), operand.type()));

        for (WhenClause whenClause : whenClauses) {
            if (!(whenClause.getOperand() instanceof Constant candidate)) {
                return Optional.empty();
            }

            if (Boolean.TRUE.equals(functionInvoker.invoke(equals, connectorSession, Arrays.asList(constantOperand.value(), candidate.value())))) {
                return Optional.of(whenClause.getResult());
            }
        }

        return Optional.of(defaultValue);
    }
}
