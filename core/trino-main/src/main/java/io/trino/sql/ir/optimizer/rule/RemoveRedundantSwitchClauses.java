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
import io.trino.spi.function.OperatorType;
import io.trino.sql.InterpretedFunctionInvoker;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

/**
 * Remove duplicated and redundant clauses in Switch. E.g.,
 * <ul>
 *     <li>{@code Switch(x, [When(a, r1), When(b, r2), When(a, r3)], d) -> Switch(x, [When(a, r1), When(b, r2)], d)}
 *     <li>{@code Switch(x, [When(a, r1), When(x, r2), When(b, r3)], d) -> Switch(x, [When(a, r1)], r2)}
 *     <li>{@code Switch(x, [When(x, r)], d) -> r}
 * </ul>
 */
public class RemoveRedundantSwitchClauses
        implements IrOptimizerRule
{
    private final Metadata metadata;
    private final InterpretedFunctionInvoker functionInvoker;

    public RemoveRedundantSwitchClauses(PlannerContext context)
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

        if (!isDeterministic(operand)) {
            return Optional.empty();
        }

        List<WhenClause> newClauses = new ArrayList<>();
        Expression newDefault = defaultValue;

        ResolvedFunction equals = metadata.resolveOperator(OperatorType.EQUAL, ImmutableList.of(operand.type(), operand.type()));

        Set<Expression> seen = new HashSet<>();
        boolean changed = false;
        for (WhenClause whenClause : whenClauses) {
            Expression candidate = whenClause.getOperand();

            if (seen.contains(candidate)) {
                changed = true;
            }
            else if (operand.equals(candidate)) {
                changed = true;
                newDefault = whenClause.getResult();
                break;
            }
            else if (operand instanceof Constant constantOperand && candidate instanceof Constant constantCandidate) {
                changed = true;
                if (TRUE.equals(functionInvoker.invoke(equals, session.toConnectorSession(), constantOperand.value(), constantCandidate.value()))) {
                    newDefault = whenClause.getResult();
                    break;
                }
            }
            else {
                newClauses.add(whenClause);

                if (isDeterministic(candidate)) {
                    seen.add(candidate);
                }
            }
        }

        if (!changed) {
            return Optional.empty();
        }

        if (newClauses.isEmpty()) {
            return Optional.of(newDefault);
        }

        return Optional.of(new Switch(operand, newClauses, newDefault));
    }
}
