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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getPushFilterIntoValuesMaxRowCount;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.ir.Booleans.FALSE;
import static io.trino.sql.ir.Booleans.NULL_BOOLEAN;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.optimizer.IrExpressionOptimizer.newOptimizer;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.SymbolsExtractor.extractUnique;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.values;
import static java.util.Objects.requireNonNull;

public class PushFilterIntoValues
        implements Rule<FilterNode>
{
    private static final Capture<ValuesNode> VALUES = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(values()
                    .matching(PushFilterIntoValues::isSupportedValues)
                    .capturedAs(VALUES)));

    private final PlannerContext plannerContext;

    public PushFilterIntoValues(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode node, Captures captures, Context context)
    {
        ValuesNode valuesNode = captures.get(VALUES);

        if (valuesNode.getRows().orElseThrow().size() > getPushFilterIntoValuesMaxRowCount(context.getSession())) {
            return Result.empty();
        }

        Expression predicate = node.getPredicate();
        if (!isDeterministic(predicate)) {
            return Result.empty();
        }

        if (valuesNode.getRows().orElseThrow().isEmpty()) {
            // empty values node, filter is redundant
            return Result.ofPlanNode(valuesNode);
        }

        if (valuesNode.getRows().orElseThrow().stream()
                .anyMatch(row -> !isDeterministic(row))) {
            return Result.empty();
        }

        Set<Symbol> valuesExpressionsSymbols = valuesNode.getRows().orElseThrow().stream()
                .map(SymbolsExtractor::extractUnique)
                .flatMap(Set::stream)
                .collect(toImmutableSet());
        if (!valuesExpressionsSymbols.isEmpty()) {
            // unresolved correlation
            return Result.empty();
        }

        Set<Symbol> valuesOutputSymbols = ImmutableSet.copyOf(valuesNode.getOutputSymbols());
        Set<Symbol> predicateSymbols = extractUnique(predicate);
        if (!valuesOutputSymbols.containsAll(predicateSymbols)) {
            // filter is correlated
            return Result.empty();
        }

        ImmutableList.Builder<Expression> filteredRows = ImmutableList.builder();
        boolean optimized = false;
        boolean keepFilter = false;
        for (Expression expression : valuesNode.getRows().orElseThrow()) {
            Row row = (Row) expression;
            ImmutableMap.Builder<Symbol, Expression> mapping = ImmutableMap.builder();
            for (int i = 0; i < valuesNode.getOutputSymbols().size(); i++) {
                mapping.put(valuesNode.getOutputSymbols().get(i), row.items().get(i));
            }
            Expression rewrittenPredicate = inlineSymbols(mapping.buildOrThrow(), predicate);
            Optional<Expression> optimizedPredicate = newOptimizer(plannerContext).process(rewrittenPredicate, context.getSession(), ImmutableMap.of());

            if (optimizedPredicate.isPresent() && optimizedPredicate.get().equals(TRUE)) {
                filteredRows.add(row);
            }
            else if (optimizedPredicate.isPresent() && (optimizedPredicate.get().equals(FALSE) || optimizedPredicate.get().equals(NULL_BOOLEAN))) {
                // skip row
                optimized = true;
            }
            else {
                // could not evaluate the predicate for the row
                filteredRows.add(row);
                keepFilter = true;
            }
        }

        if (!optimized && keepFilter) {
            return Result.empty();
        }

        PlanNode optimizedValues = new ValuesNode(
                valuesNode.getId(),
                valuesNode.getOutputSymbols(),
                filteredRows.build());

        if (keepFilter) {
            return Result.ofPlanNode(node.replaceChildren(ImmutableList.of(optimizedValues)));
        }

        return Result.ofPlanNode(optimizedValues);
    }

    private static boolean isSupportedValues(ValuesNode valuesNode)
    {
        // If valuesNode.getRows().isEmpty() then values has no outputs. Filter predicate should be evaluated
        // to a constant true or false literal, and plan should be optimized by RemoveTrivialFilters,
        // so we don't need to handle this case here.
        // Also, do not optimize if any values row is not a Row instance, because we cannot easily inline
        // the columns of non-row expressions in the filter predicate.
        return valuesNode.getRows().isPresent() && valuesNode.getRows().get().stream().allMatch(Row.class::isInstance);
    }
}
