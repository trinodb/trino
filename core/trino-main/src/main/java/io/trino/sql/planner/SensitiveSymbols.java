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
package io.trino.sql.planner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.SensitiveExpression;
import io.trino.sql.ir.SensitiveExpressions;
import io.trino.sql.planner.iterative.GroupReference;
import io.trino.sql.planner.plan.AdaptivePlanNode;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AssignUniqueId;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.DynamicFilterSourceNode;
import io.trino.sql.planner.plan.EnforceSingleRowNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.ExplainAnalyzeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.IndexJoinNode;
import io.trino.sql.planner.plan.IndexSourceNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.MergeProcessorNode;
import io.trino.sql.planner.plan.MergeWriterNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.RemoteSourceNode;
import io.trino.sql.planner.plan.RowNumberNode;
import io.trino.sql.planner.plan.SampleNode;
import io.trino.sql.planner.plan.SemiJoinNode;
import io.trino.sql.planner.plan.SetOperationNode;
import io.trino.sql.planner.plan.SimpleTableExecuteNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.SpatialJoinNode;
import io.trino.sql.planner.plan.StatisticsWriterNode;
import io.trino.sql.planner.plan.TableDeleteNode;
import io.trino.sql.planner.plan.TableExecuteNode;
import io.trino.sql.planner.plan.TableFinishNode;
import io.trino.sql.planner.plan.TableFunctionNode;
import io.trino.sql.planner.plan.TableFunctionProcessorNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TableUpdateNode;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.TopNRankingNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.ir.IrUtils.preOrder;
import static java.util.Collections.disjoint;

/**
 * Finds the symbols whose values a {@link SensitiveExpression} constrains or computes:
 * <ul>
 * <li>the symbols a sensitive expression references, and the symbols they copy, since a constraint on a copy
 * constrains the original
 * <li>the symbols a sensitive expression computes, like the output of a column mask
 * <li>the symbols that copy or are computed from any of these
 * </ul>
 * A computed symbol does not make its inputs sensitive. Projected references, set operations, exchanges and grouping
 * columns copy symbols, and unnest computes the unnested values from their input. For any other node, an output that
 * no source produces is treated as computed from all source outputs.
 * <p>
 * Join criteria and other node properties do not propagate. Per-fragment callers rely on a sensitive expression
 * staying in its scan's fragment: {@code PushPredicateIntoTableScan} keeps it as the filter directly above the scan,
 * and fragments split only at remote exchanges.
 */
public final class SensitiveSymbols
{
    private SensitiveSymbols() {}

    /**
     * Symbols that the sensitive expressions within {@code expression} reference.
     */
    public static Set<Symbol> referencedIn(Expression expression)
    {
        return preOrder(expression)
                .filter(SensitiveExpression.class::isInstance)
                .flatMap(sensitive -> SymbolsExtractor.extractUnique(sensitive).stream())
                .collect(toImmutableSet());
    }

    /**
     * Sensitive symbols of the plan, as described in the class documentation.
     */
    public static Set<Symbol> inPlan(PlanNode root)
    {
        Constraints constraints = new Constraints();
        collectConstraints(root, constraints);
        if (!constraints.sensitive) {
            return ImmutableSet.of();
        }
        Set<Symbol> sensitive = constraints.originals();
        propagate(root, sensitive);
        return ImmutableSet.copyOf(sensitive);
    }

    // Collects what sensitive expressions reference and which symbols copy which
    private static void collectConstraints(PlanNode node, Constraints constraints)
    {
        if (node instanceof GroupReference) {
            // Unresolved memo plans are only printed for debugging
            return;
        }
        for (Expression expression : nodeExpressions(node).orElseGet(ImmutableList::of)) {
            if (SensitiveExpressions.isPresent(expression)) {
                constraints.sensitive = true;
                constraints.referenced.addAll(referencedIn(expression));
            }
        }
        switch (node) {
            case ProjectNode project -> project.getAssignments().forEach((output, expression) -> {
                if (expression instanceof Reference reference) {
                    constraints.copies.put(output, Symbol.from(reference));
                }
            });
            case SetOperationNode setOperation -> constraints.copies.putAll(setOperation.getSymbolMapping());
            case ExchangeNode exchange -> exchange.getInputs().forEach(inputs -> {
                for (int i = 0; i < inputs.size(); i++) {
                    constraints.copies.put(exchange.getOutputSymbols().get(i), inputs.get(i));
                }
            });
            case GroupIdNode groupId -> groupId.getGroupingColumns().forEach(constraints.copies::put);
            default -> {}
        }
        sources(node).forEach(source -> collectConstraints(source, constraints));
    }

    // Marks, bottom-up, the outputs that copy or are computed from a sensitive symbol
    private static void propagate(PlanNode node, Set<Symbol> sensitive)
    {
        if (node instanceof GroupReference) {
            return;
        }
        sources(node).forEach(source -> propagate(source, sensitive));
        switch (node) {
            case ProjectNode project -> project.getAssignments().forEach((output, expression) -> {
                if (SensitiveExpressions.isPresent(expression) || !disjoint(sensitive, SymbolsExtractor.extractUnique(expression))) {
                    sensitive.add(output);
                }
            });
            case SetOperationNode setOperation -> setOperation.getSymbolMapping().asMap().forEach((output, inputs) -> {
                if (!disjoint(sensitive, inputs)) {
                    sensitive.add(output);
                }
            });
            case ExchangeNode exchange -> exchange.getInputs().forEach(inputs -> {
                for (int i = 0; i < inputs.size(); i++) {
                    if (sensitive.contains(inputs.get(i))) {
                        sensitive.add(exchange.getOutputSymbols().get(i));
                    }
                }
            });
            case GroupIdNode groupId -> groupId.getGroupingColumns().forEach((output, input) -> {
                if (sensitive.contains(input)) {
                    sensitive.add(output);
                }
            });
            case UnnestNode unnest -> unnest.getMappings().forEach(mapping -> {
                if (sensitive.contains(mapping.getInput())) {
                    sensitive.addAll(mapping.getOutputs());
                }
            });
            case ValuesNode values -> values.getRows().ifPresent(rows -> rows.forEach(row -> {
                if (row instanceof Row items) {
                    for (int i = 0; i < items.items().size(); i++) {
                        if (SensitiveExpressions.isPresent(items.items().get(i))) {
                            sensitive.add(values.getOutputSymbols().get(i));
                        }
                    }
                }
                else if (SensitiveExpressions.isPresent(row)) {
                    sensitive.addAll(values.getOutputSymbols());
                }
            }));
            default -> {
                Set<Symbol> sourceOutputs = sources(node).stream()
                        .flatMap(source -> source.getOutputSymbols().stream())
                        .collect(toImmutableSet());
                if (!disjoint(sensitive, sourceOutputs)) {
                    node.getOutputSymbols().stream()
                            .filter(output -> !sourceOutputs.contains(output))
                            .forEach(sensitive::add);
                }
            }
        }
    }

    // AdaptivePlanNode reports its initial plan, which is not a source
    static List<PlanNode> sources(PlanNode node)
    {
        if (node instanceof AdaptivePlanNode adaptive) {
            return ImmutableList.of(adaptive.getInitialPlan(), adaptive.getCurrentPlan());
        }
        return node.getSources();
    }

    /**
     * Returns every expression {@code node} holds, or empty if this class does not know the node type.
     */
    static Optional<List<Expression>> nodeExpressions(PlanNode node)
    {
        return switch (node) {
            case FilterNode filter -> Optional.of(ImmutableList.of(filter.getPredicate()));
            case ProjectNode project -> Optional.of(ImmutableList.copyOf(project.getAssignments().expressions()));
            case JoinNode join -> Optional.of(join.getFilter().map(ImmutableList::of).orElseGet(ImmutableList::of));
            case SpatialJoinNode spatialJoin -> Optional.of(ImmutableList.of(spatialJoin.getFilter()));
            case ValuesNode values -> Optional.of(values.getRows().orElseGet(ImmutableList::of));
            case AggregationNode aggregation -> Optional.of(aggregationArguments(aggregation.getAggregations()));
            case WindowNode window -> Optional.of(arguments(window.getWindowFunctions()));
            case PatternRecognitionNode patternRecognition -> {
                ImmutableList.Builder<Expression> expressions = ImmutableList.<Expression>builder()
                        .addAll(arguments(patternRecognition.getWindowFunctions()));
                patternRecognition.getMeasures().values().forEach(measure -> addExpressions(expressions, measure.getExpressionAndValuePointers()));
                patternRecognition.getVariableDefinitions().values().forEach(definition -> addExpressions(expressions, definition));
                yield Optional.of(expressions.build());
            }
            case ExchangeNode exchange -> Optional.of(partitioningArguments(Optional.of(exchange.getPartitioningScheme())));
            case TableWriterNode writer -> Optional.of(ImmutableList.<Expression>builder()
                    .addAll(partitioningArguments(writer.getPartitioningScheme()))
                    .addAll(writer.getStatisticsAggregation().map(statistics -> aggregationArguments(statistics.getAggregations())).orElseGet(ImmutableList::of))
                    .build());
            case TableFinishNode finish -> Optional.of(finish.getStatisticsAggregation().map(statistics -> aggregationArguments(statistics.getAggregations())).orElseGet(ImmutableList::of));
            case MergeWriterNode writer -> Optional.of(partitioningArguments(writer.getPartitioningScheme()));
            case TableExecuteNode execute -> Optional.of(partitioningArguments(execute.getPartitioningScheme()));
            case AdaptivePlanNode _, AssignUniqueId _, DistinctLimitNode _, DynamicFilterSourceNode _, EnforceSingleRowNode _,
                 ExplainAnalyzeNode _, GroupIdNode _, IndexJoinNode _, IndexSourceNode _, LimitNode _, MarkDistinctNode _, MergeProcessorNode _,
                 OutputNode _, RemoteSourceNode _, RowNumberNode _, SampleNode _, SemiJoinNode _, SetOperationNode _, SimpleTableExecuteNode _,
                 SortNode _, StatisticsWriterNode _, TableDeleteNode _, TableFunctionNode _, TableFunctionProcessorNode _, TableScanNode _,
                 TableUpdateNode _, TopNNode _, TopNRankingNode _, UnnestNode _ -> Optional.of(ImmutableList.of());
            default -> Optional.empty();
        };
    }

    private static List<Expression> aggregationArguments(Map<Symbol, AggregationNode.Aggregation> aggregations)
    {
        return aggregations.values().stream()
                .flatMap(aggregation -> aggregation.getArguments().stream())
                .collect(toImmutableList());
    }

    private static List<Expression> partitioningArguments(Optional<PartitioningScheme> partitioningScheme)
    {
        return partitioningScheme.stream()
                .flatMap(scheme -> scheme.getPartitioning().getArguments().stream())
                .filter(argument -> !argument.isConstant())
                .map(Partitioning.ArgumentBinding::getExpression)
                .collect(toImmutableList());
    }

    private static List<Expression> arguments(Map<Symbol, WindowNode.Function> functions)
    {
        return functions.values().stream()
                .flatMap(function -> function.getArguments().stream())
                .collect(toImmutableList());
    }

    private static void addExpressions(ImmutableList.Builder<Expression> expressions, ExpressionAndValuePointers expression)
    {
        expressions.add(expression.getExpression());
        expression.getAssignments().forEach(assignment -> {
            if (assignment.valuePointer() instanceof AggregationValuePointer aggregation) {
                expressions.addAll(aggregation.getArguments());
            }
        });
    }

    private static final class Constraints
    {
        private boolean sensitive;
        private final Set<Symbol> referenced = new HashSet<>();
        // Output symbol to the symbols it copies
        private final SetMultimap<Symbol, Symbol> copies = HashMultimap.create();

        // The referenced symbols and the symbols they copy, transitively
        public Set<Symbol> originals()
        {
            Set<Symbol> result = new HashSet<>(referenced);
            Deque<Symbol> pending = new ArrayDeque<>(referenced);
            while (!pending.isEmpty()) {
                for (Symbol original : copies.get(pending.pop())) {
                    if (result.add(original)) {
                        pending.push(original);
                    }
                }
            }
            return result;
        }
    }
}
