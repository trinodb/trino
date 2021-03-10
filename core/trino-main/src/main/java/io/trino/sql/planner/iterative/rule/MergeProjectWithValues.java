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
import com.google.common.collect.Sets;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Row;
import io.trino.sql.tree.SymbolReference;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.isMergeProjectWithValues;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.values;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;

/**
 * Transforms:
 * <pre>
 * - Project
 *      a <- a
 *      d <- b
 *      e <- f(b)
 *      f <- 1
 *      - Values(a, b, c)
 *          expr_a_1, expr_b_1, expr_c_1
 *          expr_a_2, expr_b_2, expr_c_2
 * </pre>
 * into:
 * <pre>
 * - Values (a, d, e, f)
 *      expr_a_1, expr_b_1, f(expr_b_1), 1
 *      expr_a_2, expr_b_2, f(expr_b_2), 1
 * </pre>
 * Note: this rule does not fire if ValuesNode contains a non-deterministic
 * expression and it is referenced more than once in ProjectNode's assignments.
 * This is to prevent incorrect results in the following case:
 * <pre>
 * - project
 *      row <- ROW(rand, rand)
 *      - Values(rand)
 *          rand()
 * </pre>
 * The expected result of the projection is a row with both fields equal.
 * However, if the non-deterministic expression rand() was inlined, we would
 * get two independent random values.
 */
public class MergeProjectWithValues
        implements Rule<ProjectNode>
{
    private static final Capture<ValuesNode> VALUES = newCapture();
    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(values()
                    .matching(MergeProjectWithValues::isSupportedValues)
                    .capturedAs(VALUES)));

    private final Metadata metadata;

    public MergeProjectWithValues(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isMergeProjectWithValues(session);
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        ValuesNode valuesNode = captures.get(VALUES);

        // handle projection which prunes all symbols
        if (node.getOutputSymbols().isEmpty()) {
            return Result.ofPlanNode(new ValuesNode(valuesNode.getId(), valuesNode.getRowCount()));
        }

        // fix iteration order over ProjectNode's assignments
        List<Map.Entry<Symbol, Expression>> assignments = ImmutableList.copyOf(node.getAssignments().entrySet());
        List<Symbol> outputs = assignments.stream()
                .map(Map.Entry::getKey)
                .collect(toImmutableList());
        List<Expression> expressions = assignments.stream()
                .map(Map.Entry::getValue)
                .collect(toImmutableList());

        // handle values with no output symbols
        if (valuesNode.getOutputSymbols().isEmpty()) {
            return Result.ofPlanNode(new ValuesNode(
                    valuesNode.getId(),
                    outputs,
                    nCopies(valuesNode.getRowCount(), new Row(ImmutableList.copyOf(expressions)))));
        }

        // do not proceed if ValuesNode contains a non-deterministic expression and it is referenced more than once by the projection
        Set<Symbol> nonDeterministicValuesOutputs = new HashSet<>();
        for (Expression rowExpression : valuesNode.getRows().get()) {
            Row row = (Row) rowExpression;
            for (int i = 0; i < valuesNode.getOutputSymbols().size(); i++) {
                if (!isDeterministic(row.getItems().get(i), metadata)) {
                    nonDeterministicValuesOutputs.add(valuesNode.getOutputSymbols().get(i));
                }
            }
        }
        Set<Symbol> multipleReferencedSymbols = expressions.stream()
                .flatMap(expression -> SymbolsExtractor.extractAll(expression).stream())
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet().stream()
                .filter(entry -> entry.getValue() > 1)
                .map(Map.Entry::getKey)
                .collect(toImmutableSet());
        if (!Sets.intersection(nonDeterministicValuesOutputs, multipleReferencedSymbols).isEmpty()) {
            return Result.empty();
        }

        // inline values expressions into projection's assignments
        ImmutableList.Builder<Expression> projectedRows = ImmutableList.builder();
        for (Expression rowExpression : valuesNode.getRows().get()) {
            Map<SymbolReference, Expression> mapping = buildMappings(valuesNode.getOutputSymbols(), (Row) rowExpression);
            Row projectedRow = new Row(expressions.stream()
                    .map(expression -> replaceExpression(expression, mapping))
                    .collect(toImmutableList()));
            projectedRows.add(projectedRow);
        }
        return Result.ofPlanNode(new ValuesNode(valuesNode.getId(), outputs, projectedRows.build()));
    }

    private static boolean isSupportedValues(ValuesNode valuesNode)
    {
        return valuesNode.getRows().isEmpty() || valuesNode.getRows().get().stream().allMatch(Row.class::isInstance);
    }

    private Map<SymbolReference, Expression> buildMappings(List<Symbol> symbols, Row row)
    {
        ImmutableMap.Builder<SymbolReference, Expression> mappingBuilder = ImmutableMap.builder();
        for (int i = 0; i < row.getItems().size(); i++) {
            mappingBuilder.put(symbols.get(i).toSymbolReference(), row.getItems().get(i));
        }
        return mappingBuilder.build();
    }
}
