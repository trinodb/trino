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
package io.trino.operator.project;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Analyzes an Expression to determine which input channels (page columns) it references
 * and creates a compact parameter mapping for bytecode generation.
 * <p>
 * Also tracks which channels are unconditionally evaluated (eagerly loaded) vs
 * conditionally evaluated (lazily loaded). This distinction avoids loading and
 * decoding columns that may not be needed — e.g., the second operand of AND
 * is only evaluated if the first operand is true.
 */
public final class PageFieldsToInputParametersRewriter
{
    private PageFieldsToInputParametersRewriter() {}

    public static Result rewritePageFieldsToInputParameters(Expression expression, Map<Symbol, Integer> layout)
    {
        // Collect all unique channel indices referenced by the expression,
        // and track which are unconditionally evaluated
        Set<Integer> referencedChannels = new LinkedHashSet<>();
        Set<Integer> eagerlyLoadedChannels = new HashSet<>();
        collectReferencedChannels(expression, layout, true, referencedChannels, eagerlyLoadedChannels);

        // Create compact mapping: original channel index → parameter index (0, 1, 2, ...)
        List<Integer> inputChannelsList = new ArrayList<>(referencedChannels);
        Map<Integer, Integer> channelToParameter = new HashMap<>();
        for (int i = 0; i < inputChannelsList.size(); i++) {
            channelToParameter.put(inputChannelsList.get(i), i);
        }

        // Create compact layout: symbol → compact parameter index
        ImmutableMap.Builder<Symbol, Integer> compactLayout = ImmutableMap.builder();
        for (Map.Entry<Symbol, Integer> entry : layout.entrySet()) {
            Integer parameterIndex = channelToParameter.get(entry.getValue());
            if (parameterIndex != null) {
                compactLayout.put(entry.getKey(), parameterIndex);
            }
        }

        InputChannels inputChannels = new InputChannels(inputChannelsList, ImmutableSet.copyOf(eagerlyLoadedChannels));
        return new Result(compactLayout.buildOrThrow(), inputChannels);
    }

    private static void collectReferencedChannels(
            Expression expression,
            Map<Symbol, Integer> layout,
            boolean unconditionallyEvaluated,
            Set<Integer> channels,
            Set<Integer> eagerlyLoadedChannels)
    {
        if (expression instanceof Reference reference) {
            Integer channel = layout.get(Symbol.from(reference));
            if (channel != null) {
                channels.add(channel);
                if (unconditionallyEvaluated) {
                    eagerlyLoadedChannels.add(channel);
                }
            }
            return;
        }

        switch (expression) {
            // Short-circuit expressions: only the first operand is unconditionally evaluated
            case Logical logical -> {
                List<Expression> terms = logical.terms();
                for (int i = 0; i < terms.size(); i++) {
                    collectReferencedChannels(terms.get(i), layout, i == 0 && unconditionallyEvaluated, channels, eagerlyLoadedChannels);
                }
            }
            case Coalesce coalesce -> {
                List<Expression> operands = coalesce.operands();
                for (int i = 0; i < operands.size(); i++) {
                    collectReferencedChannels(operands.get(i), layout, i == 0 && unconditionallyEvaluated, channels, eagerlyLoadedChannels);
                }
            }
            case Case caseExpr -> {
                List<WhenClause> whenClauses = caseExpr.whenClauses();
                // First condition is unconditional; everything else is conditional
                if (!whenClauses.isEmpty()) {
                    collectReferencedChannels(whenClauses.getFirst().getOperand(), layout, unconditionallyEvaluated, channels, eagerlyLoadedChannels);
                    collectReferencedChannels(whenClauses.getFirst().getResult(), layout, false, channels, eagerlyLoadedChannels);
                    for (int i = 1; i < whenClauses.size(); i++) {
                        collectReferencedChannels(whenClauses.get(i).getOperand(), layout, false, channels, eagerlyLoadedChannels);
                        collectReferencedChannels(whenClauses.get(i).getResult(), layout, false, channels, eagerlyLoadedChannels);
                    }
                }
                collectReferencedChannels(caseExpr.defaultValue(), layout, false, channels, eagerlyLoadedChannels);
            }
            case Switch switchExpr -> {
                // Operand and first when-clause operand are unconditional; remaining clauses and default are conditional
                collectReferencedChannels(switchExpr.operand(), layout, unconditionallyEvaluated, channels, eagerlyLoadedChannels);
                List<WhenClause> whenClauses = switchExpr.whenClauses();
                if (!whenClauses.isEmpty()) {
                    collectReferencedChannels(whenClauses.getFirst().getOperand(), layout, unconditionallyEvaluated, channels, eagerlyLoadedChannels);
                    collectReferencedChannels(whenClauses.getFirst().getResult(), layout, false, channels, eagerlyLoadedChannels);
                    for (int i = 1; i < whenClauses.size(); i++) {
                        collectReferencedChannels(whenClauses.get(i).getOperand(), layout, false, channels, eagerlyLoadedChannels);
                        collectReferencedChannels(whenClauses.get(i).getResult(), layout, false, channels, eagerlyLoadedChannels);
                    }
                }
                collectReferencedChannels(switchExpr.defaultValue(), layout, false, channels, eagerlyLoadedChannels);
            }
            case Between between -> {
                // Value and min are unconditional; max is conditional on the second comparison
                collectReferencedChannels(between.value(), layout, unconditionallyEvaluated, channels, eagerlyLoadedChannels);
                collectReferencedChannels(between.min(), layout, unconditionallyEvaluated, channels, eagerlyLoadedChannels);
                collectReferencedChannels(between.max(), layout, false, channels, eagerlyLoadedChannels);
            }
            case Lambda lambda -> {
                // Lambda parameters are locally bound — exclude them from the layout so they are not
                // mistaken for input channel references when traversing the body
                Map<Symbol, Integer> bodyLayout = Maps.filterKeys(layout, key -> !lambda.arguments().contains(key));
                collectReferencedChannels(lambda.body(), bodyLayout, unconditionallyEvaluated, channels, eagerlyLoadedChannels);
            }
            case Call call -> {
                for (Expression argument : call.arguments()) {
                    collectReferencedChannels(argument, layout, unconditionallyEvaluated && !(argument instanceof Lambda), channels, eagerlyLoadedChannels);
                }
            }
            // All other expressions: children inherit the parent's unconditional context
            default -> {
                for (Expression child : expression.children()) {
                    collectReferencedChannels(child, layout, unconditionallyEvaluated, channels, eagerlyLoadedChannels);
                }
            }
        }
    }

    public record Result(Map<Symbol, Integer> compactLayout, InputChannels inputChannels) {}
}
