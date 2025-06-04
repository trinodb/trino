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
import io.trino.sql.ir.In;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;

/**
 * Evaluates a constant IN expression
 */
public class EvaluateIn
        implements IrOptimizerRule
{
    private final Metadata metadata;
    private final InterpretedFunctionInvoker functionInvoker;

    public EvaluateIn(PlannerContext context)
    {
        metadata = context.getMetadata();
        functionInvoker = new InterpretedFunctionInvoker(context.getFunctionManager());
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof In(Constant value, List<Expression> list))) {
            return Optional.empty();
        }

        if (!list.stream().allMatch(Constant.class::isInstance)) {
            return Optional.empty();
        }

        if (list.isEmpty()) {
            return Optional.of(FALSE);
        }

        if (value.value() == null) {
            return Optional.of(NULL_BOOLEAN);
        }

        ResolvedFunction equalsOperator = metadata.resolveOperator(EQUAL, ImmutableList.of(value.type(), value.type()));
        ConnectorSession connectorSession = session.toConnectorSession();

        boolean nullMatch = false;
        for (Expression item : list) {
            Constant constantItem = (Constant) item;
            Boolean equal = (Boolean) functionInvoker.invoke(equalsOperator, connectorSession, value.value(), constantItem.value());
            if (Boolean.TRUE.equals(equal)) {
                return Optional.of(TRUE);
            }
            else if (equal == null) {
                nullMatch = true;
            }
        }

        return Optional.of(nullMatch ? NULL_BOOLEAN : FALSE);
    }
}
