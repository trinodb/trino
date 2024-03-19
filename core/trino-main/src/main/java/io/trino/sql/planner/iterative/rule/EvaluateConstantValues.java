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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.NodeRef;
import io.trino.sql.planner.IrExpressionInterpreter;
import io.trino.sql.planner.IrTypeAnalyzer;
import io.trino.sql.planner.NoOpSymbolResolver;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.List;
import java.util.Map;

import static io.trino.sql.planner.plan.Patterns.values;
import static java.util.Objects.requireNonNull;

public class EvaluateConstantValues
        implements Rule<ValuesNode>
{
    private static final Pattern<ValuesNode> PATTERN = values();

    private final PlannerContext plannerContext;
    private final IrTypeAnalyzer typeAnalyzer;

    public EvaluateConstantValues(PlannerContext plannerContext, IrTypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ValuesNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ValuesNode values, Captures captures, Context context)
    {
        if (values.getRows().isEmpty()) {
            return Result.empty();
        }

        List<Expression> rows = values.getRows().get();

        ImmutableList.Builder<Expression> result = ImmutableList.builder();
        for (Expression row : rows) {
            Map<NodeRef<Expression>, Type> types = typeAnalyzer.getTypes(context.getSymbolAllocator().getTypes(), row);
            IrExpressionInterpreter interpreter = new IrExpressionInterpreter(row, plannerContext, context.getSession(), types);

            if (!SymbolsExtractor.extractAll(row).isEmpty()) {
                Object optimized = interpreter.optimize(NoOpSymbolResolver.INSTANCE);
                result.add(switch (optimized) {
                    case Expression e -> e;
                    default -> new Constant(types.get(NodeRef.of(row)), optimized);
                });
            }
            else {
                Object optimized = interpreter.evaluate(NoOpSymbolResolver.INSTANCE);
                result.add(new Constant(types.get(NodeRef.of(row)), optimized));
            }
        }

        List<Expression> newRows = result.build();

        if (rows.equals(newRows)) {
            return Result.empty();
        }

        return Result.ofPlanNode(new ValuesNode(values.getId(), values.getOutputSymbols(), newRows));
    }
}
