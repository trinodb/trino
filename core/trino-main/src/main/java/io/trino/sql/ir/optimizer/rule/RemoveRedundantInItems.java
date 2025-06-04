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
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.In;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.IrExpressions.ifExpression;
import static io.trino.sql.ir.IrExpressions.mayFail;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static java.util.Collections.singletonList;

public class RemoveRedundantInItems
        implements IrOptimizerRule
{
    private final PlannerContext context;
    private final Metadata metadata;

    public RemoveRedundantInItems(PlannerContext context)
    {
        this.context = context;
        metadata = context.getMetadata();
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof In(Expression value, List<Expression> list))) {
            return Optional.empty();
        }

        List<Expression> cannotFail = new ArrayList<>();
        List<Expression> mayFail = new ArrayList<>();

        boolean exactMatchFound = false;
        boolean removed = false;
        Set<Expression> seen = new HashSet<>();
        for (Expression item : list) {
            if (seen.contains(item)) {
                removed = true;
            }
            else {
                if (mayFail(context, item)) {
                    mayFail.add(item);
                }
                else {
                    cannotFail.add(item);
                }

                if (isDeterministic(item)) {
                    exactMatchFound = exactMatchFound || value.equals(item);
                    seen.add(item);
                }
            }
        }

        if (exactMatchFound && mayFail.isEmpty()) {
            ResolvedFunction indeterminate = metadata.resolveOperator(INDETERMINATE, ImmutableList.of(value.type()));
            return Optional.of(ifExpression(new Call(indeterminate, singletonList(value)), NULL_BOOLEAN, TRUE));
        }

        if (!removed && list.size() > 1) {
            return Optional.empty();
        }

        List<Expression> newItems = ImmutableList.<Expression>builder()
                .addAll(cannotFail)
                .addAll(mayFail)
                .build();

        if (newItems.size() == 1) {
            return Optional.of(new Comparison(Comparison.Operator.EQUAL, value, newItems.getFirst()));
        }

        return Optional.of(new In(value, newItems));
    }
}
