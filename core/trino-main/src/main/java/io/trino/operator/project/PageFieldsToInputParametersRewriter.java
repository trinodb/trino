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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.sql.ir.Between;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrVisitor;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Switch;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;

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
        Visitor visitor = new Visitor(layout);
        visitor.process(expression, true);

        // Create compact mapping: original channel index → parameter index (0, 1, 2, ...)
        List<Integer> inputChannels = visitor.getInputChannels();
        Map<Integer, Integer> channelToParameter = new HashMap<>();
        for (int i = 0; i < inputChannels.size(); i++) {
            channelToParameter.put(inputChannels.get(i), i);
        }

        // Create compact layout: symbol → compact parameter index
        ImmutableMap.Builder<Symbol, Integer> compactLayout = ImmutableMap.builder();
        for (Map.Entry<Symbol, Integer> entry : layout.entrySet()) {
            Integer parameterIndex = channelToParameter.get(entry.getValue());
            if (parameterIndex != null) {
                compactLayout.put(entry.getKey(), parameterIndex);
            }
        }

        return new Result(compactLayout.buildOrThrow(), new InputChannels(inputChannels, visitor.getEagerlyLoadedChannels()));
    }

    private static final class Visitor
            extends IrVisitor<Void, Boolean>
    {
        private final Set<Integer> inputChannels = new LinkedHashSet<>();
        private final Set<Integer> eagerlyLoadedChannels = new HashSet<>();
        private Map<Symbol, Integer> layout;

        private Visitor(Map<Symbol, Integer> layout)
        {
            this.layout = layout;
        }

        public List<Integer> getInputChannels()
        {
            return ImmutableList.copyOf(inputChannels);
        }

        public Set<Integer> getEagerlyLoadedChannels()
        {
            return ImmutableSet.copyOf(eagerlyLoadedChannels);
        }

        @Override
        protected Void visitReference(Reference node, Boolean unconditionallyEvaluated)
        {
            Integer channel = layout.get(Symbol.from(node));
            if (channel != null) {
                inputChannels.add(channel);
                if (unconditionallyEvaluated) {
                    eagerlyLoadedChannels.add(channel);
                }
            }
            return null;
        }

        @Override
        protected Void visitLogical(Logical node, Boolean unconditionallyEvaluated)
        {
            // Short-circuit: only the first term is unconditionally evaluated
            List<Expression> terms = node.terms();
            for (int i = 0; i < terms.size(); i++) {
                process(terms.get(i), i == 0 && unconditionallyEvaluated);
            }
            return null;
        }

        @Override
        protected Void visitCoalesce(Coalesce node, Boolean unconditionallyEvaluated)
        {
            List<Expression> operands = node.operands();
            for (int i = 0; i < operands.size(); i++) {
                process(operands.get(i), i == 0 && unconditionallyEvaluated);
            }
            return null;
        }

        @Override
        protected Void visitCase(Case node, Boolean unconditionallyEvaluated)
        {
            // First condition is unconditional; everything else is conditional
            List<WhenClause> whenClauses = node.whenClauses();
            if (!whenClauses.isEmpty()) {
                process(whenClauses.getFirst().getOperand(), unconditionallyEvaluated);
                process(whenClauses.getFirst().getResult(), false);
                for (int i = 1; i < whenClauses.size(); i++) {
                    process(whenClauses.get(i).getOperand(), false);
                    process(whenClauses.get(i).getResult(), false);
                }
            }
            process(node.defaultValue(), false);
            return null;
        }

        @Override
        protected Void visitSwitch(Switch node, Boolean unconditionallyEvaluated)
        {
            // Operand and first when-clause operand are unconditional; remaining clauses and default are conditional
            process(node.operand(), unconditionallyEvaluated);
            List<WhenClause> whenClauses = node.whenClauses();
            if (!whenClauses.isEmpty()) {
                process(whenClauses.getFirst().getOperand(), unconditionallyEvaluated);
                process(whenClauses.getFirst().getResult(), false);
                for (int i = 1; i < whenClauses.size(); i++) {
                    process(whenClauses.get(i).getOperand(), false);
                    process(whenClauses.get(i).getResult(), false);
                }
            }
            process(node.defaultValue(), false);
            return null;
        }

        @Override
        protected Void visitBetween(Between node, Boolean unconditionallyEvaluated)
        {
            // Value and min are unconditional; max is conditional on the second comparison
            process(node.value(), unconditionallyEvaluated);
            process(node.min(), unconditionallyEvaluated);
            process(node.max(), false);
            return null;
        }

        @Override
        protected Void visitLambda(Lambda node, Boolean unconditionallyEvaluated)
        {
            // Lambda parameters are locally bound — exclude them from the layout so they are not
            // mistaken for input channel references when traversing the body
            Map<Symbol, Integer> previousLayout = layout;
            layout = Maps.filterKeys(layout, key -> !node.arguments().contains(key));
            try {
                process(node.body(), unconditionallyEvaluated);
            }
            finally {
                layout = previousLayout;
            }
            return null;
        }

        @Override
        protected Void visitCall(Call node, Boolean unconditionallyEvaluated)
        {
            for (Expression argument : node.arguments()) {
                process(argument, unconditionallyEvaluated && !(argument instanceof Lambda));
            }
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, Boolean unconditionallyEvaluated)
        {
            // All other expressions: children inherit the parent's unconditional context
            for (Expression child : node.children()) {
                process(child, unconditionallyEvaluated);
            }
            return null;
        }
    }

    public record Result(Map<Symbol, Integer> compactLayout, InputChannels inputChannels) {}
}
