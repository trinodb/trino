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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.optimizer.IrExpressionEvaluator;
import io.trino.sql.ir.optimizer.IrOptimizerRule;
import io.trino.sql.planner.Symbol;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metadata.GlobalFunctionCatalog.builtinFunctionName;
import static io.trino.sql.DynamicFilters.isDynamicFilter;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;

/**
 * Evaluates a constant Call expression
 */
public class EvaluateCall
        implements IrOptimizerRule
{
    private static final CatalogSchemaFunctionName FAIL_NAME = builtinFunctionName("fail");
    private final IrExpressionEvaluator evaluator;

    public EvaluateCall(PlannerContext context)
    {
        evaluator = new IrExpressionEvaluator(context);
    }

    @Override
    public Optional<Expression> apply(Expression expression, Session session, Map<Symbol, Expression> bindings)
    {
        if (!(expression instanceof Call(ResolvedFunction function, List<Expression> arguments))) {
            return Optional.empty();
        }

        if (!arguments.stream().allMatch(argument -> argument instanceof Constant || (argument instanceof Lambda && isDeterministic(argument)))) {
            return Optional.empty();
        }

        if (!function.deterministic()) {
            return Optional.empty();
        }

        if (isDynamicFilter(expression)) {
            return Optional.empty();
        }

        if (function.name().equals(FAIL_NAME)) {
            return Optional.empty();
        }

        try {
            return Optional.of(new Constant(expression.type(), evaluator.evaluate(expression, session, ImmutableMap.of())));
        }
        catch (TrinoException e) {
            return Optional.empty();
        }
    }
}
