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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.ApplyNode;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.Assignments.Assignment;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QuantifiedComparisonExpression;
import io.trino.sql.tree.SubscriptExpression;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.trino.sql.planner.plan.Patterns.applyNode;
import static java.util.Objects.requireNonNull;

/**
 * Given x::row(t) and y::row(t), converts assignments of the form
 *
 * <p><code>x IN (y...)</code> => <code>x[1] IN (y[1]...)</code>
 *
 * <p>and</p>
 *
 * <p> <code>x &lt;comparison&gt; &lt;quantifier&gt; (y...)</code></p> => <code>x[1] &lt;comparison&gt; &lt;quantifier&gt; (y[1]...)</code></p>
 *
 * <p>In particular, it transforms a plan with the following shape:</p>
 *
 * <pre>
 * - Apply x IN y
 *   - S [x :: row(T)]
 *   - Q [y :: row(T)]
 * </pre>
 * <p>
 * into
 *
 * <pre>
 * - Project (to preserve the outputs of Apply)
 *   - Apply x' IN y'
 *     - Project [x' :: T]
 *         x' = x[1]
 *       - S [x :: row(T)]
 *     - Project [y' :: T]
 *         y' = y[1]
 *       - Q [y :: row(T)]
 * </pre>
 */
public class UnwrapSingleColumnRowInApply
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode();

    private final TypeAnalyzer typeAnalyzer;

    public UnwrapSingleColumnRowInApply(TypeAnalyzer typeAnalyzer)
    {
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode node, Captures captures, Context context)
    {
        Assignments.Builder inputAssignments = Assignments.builder()
                .putIdentities(node.getInput().getOutputSymbols());

        Assignments.Builder nestedPlanAssignments = Assignments.builder()
                .putIdentities(node.getSubquery().getOutputSymbols());

        boolean applied = false;
        Assignments.Builder applyAssignments = Assignments.builder();
        for (Map.Entry<Symbol, Expression> assignment : node.getSubqueryAssignments().entrySet()) {
            Symbol output = assignment.getKey();
            Expression expression = assignment.getValue();

            Optional<Unwrapping> unwrapped = Optional.empty();
            if (expression instanceof InPredicate predicate) {
                unwrapped = unwrapSingleColumnRow(
                        context,
                        predicate.getValue(),
                        predicate.getValueList(),
                        (value, list) -> new InPredicate(value.toSymbolReference(), list.toSymbolReference()));
            }
            else if (expression instanceof QuantifiedComparisonExpression comparison) {
                unwrapped = unwrapSingleColumnRow(
                        context,
                        comparison.getValue(),
                        comparison.getSubquery(),
                        (value, list) -> new QuantifiedComparisonExpression(comparison.getOperator(), comparison.getQuantifier(), value.toSymbolReference(), list.toSymbolReference()));
            }

            if (unwrapped.isPresent()) {
                applied = true;

                Unwrapping unwrapping = unwrapped.get();
                inputAssignments.add(unwrapping.getInputAssignment());
                nestedPlanAssignments.add(unwrapping.getNestedPlanAssignment());
                applyAssignments.put(output, unwrapping.getExpression());
            }
            else {
                applyAssignments.put(assignment);
            }
        }

        if (!applied) {
            return Result.empty();
        }

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new ApplyNode(
                                node.getId(),
                                new ProjectNode(context.getIdAllocator().getNextId(), node.getInput(), inputAssignments.build()),
                                new ProjectNode(context.getIdAllocator().getNextId(), node.getSubquery(), nestedPlanAssignments.build()),
                                applyAssignments.build(),
                                node.getCorrelation(),
                                node.getOriginSubquery()),
                        Assignments.identity(node.getOutputSymbols())));
    }

    private Optional<Unwrapping> unwrapSingleColumnRow(Context context, Expression value, Expression list, BiFunction<Symbol, Symbol, Expression> function)
    {
        Type type = typeAnalyzer.getType(context.getSession(), context.getSymbolAllocator().getTypes(), value);
        if (type instanceof RowType rowType) {
            if (rowType.getFields().size() == 1) {
                Type elementType = rowType.getTypeParameters().get(0);

                Symbol valueSymbol = context.getSymbolAllocator().newSymbol("input", elementType);
                Symbol listSymbol = context.getSymbolAllocator().newSymbol("subquery", elementType);

                Assignment inputAssignment = new Assignment(valueSymbol, new SubscriptExpression(value, new LongLiteral("1")));
                Assignment nestedPlanAssignment = new Assignment(listSymbol, new SubscriptExpression(list, new LongLiteral("1")));
                Expression comparison = function.apply(valueSymbol, listSymbol);

                return Optional.of(new Unwrapping(comparison, inputAssignment, nestedPlanAssignment));
            }
        }

        return Optional.empty();
    }

    private static class Unwrapping
    {
        private final Expression expression;
        private final Assignment inputAssignment;
        private final Assignment nestedPlanAssignment;

        public Unwrapping(Expression expression, Assignment inputAssignment, Assignment nestedPlanAssignment)
        {
            this.expression = requireNonNull(expression, "expression is null");
            this.inputAssignment = requireNonNull(inputAssignment, "inputAssignment is null");
            this.nestedPlanAssignment = requireNonNull(nestedPlanAssignment, "nestedPlanAssignment is null");
        }

        public Expression getExpression()
        {
            return expression;
        }

        public Assignment getInputAssignment()
        {
            return inputAssignment;
        }

        public Assignment getNestedPlanAssignment()
        {
            return nestedPlanAssignment;
        }
    }
}
