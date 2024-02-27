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
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolsExtractor;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.rowpattern.AggregationValuePointer;
import io.trino.sql.planner.rowpattern.ClassifierValuePointer;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.MatchNumberValuePointer;
import io.trino.sql.planner.rowpattern.ScalarValuePointer;
import io.trino.sql.planner.rowpattern.ValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.trino.sql.planner.plan.Patterns.patternRecognition;

/**
 * Aggregate functions in pattern recognition context have special semantics.
 * It is allowed to use `CLASSIFIER()` and `MATCH_NUMBER()` functions
 * in aggregations arguments. Those calls are evaluated at runtime,
 * as they depend on the pattern matching state.
 * <p>
 * As a consequence, some aggregation arguments cannot be pre-projected
 * and replaced with single symbols. These are the "runtime-evaluated arguments".
 * <p>
 * The purpose of this rule is to identify and pre-project all arguments which
 * are not runtime-evaluated.
 * <p>
 * Example:
 * `array_agg(CLASSIFIER(A))` -> the argument `CLASSIFIER(A)` cannot be pre-projected
 * `avg(A.price + A.tax)` -> we can pre-project the expression `price + tax`, and
 * replace the argument with a single symbol. The label 'A', which prefixes column
 * references, has been already extracted from the expression in the LogicalIndexExtractor.
 */
public class PushDownProjectionsFromPatternRecognition
        implements Rule<PatternRecognitionNode>
{
    private static final Pattern<PatternRecognitionNode> PATTERN = patternRecognition();

    @Override
    public Pattern<PatternRecognitionNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(PatternRecognitionNode node, Captures captures, Context context)
    {
        Assignments.Builder assignments = Assignments.builder();

        Map<IrLabel, ExpressionAndValuePointers> rewrittenVariableDefinitions = rewriteVariableDefinitions(node.getVariableDefinitions(), assignments, context);
        Map<Symbol, Measure> rewrittenMeasureDefinitions = rewriteMeasureDefinitions(node.getMeasures(), assignments, context);

        if (assignments.build().isEmpty()) {
            return Result.empty();
        }

        assignments.putIdentities(node.getSource().getOutputSymbols());

        ProjectNode projectNode = new ProjectNode(
                context.getIdAllocator().getNextId(),
                node.getSource(),
                assignments.build());

        PatternRecognitionNode patternRecognitionNode = new PatternRecognitionNode(
                node.getId(),
                projectNode,
                node.getSpecification(),
                node.getHashSymbol(),
                node.getPrePartitionedInputs(),
                node.getPreSortedOrderPrefix(),
                node.getWindowFunctions(),
                rewrittenMeasureDefinitions,
                node.getCommonBaseFrame(),
                node.getRowsPerMatch(),
                node.getSkipToLabels(),
                node.getSkipToPosition(),
                node.isInitial(),
                node.getPattern(),
                rewrittenVariableDefinitions);

        return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), patternRecognitionNode, ImmutableSet.copyOf(node.getOutputSymbols())).orElse(patternRecognitionNode));
    }

    private static Map<IrLabel, ExpressionAndValuePointers> rewriteVariableDefinitions(Map<IrLabel, ExpressionAndValuePointers> variableDefinitions, Assignments.Builder assignments, Context context)
    {
        return variableDefinitions.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> rewrite(entry.getValue(), assignments, context)));
    }

    private static Map<Symbol, Measure> rewriteMeasureDefinitions(Map<Symbol, Measure> measureDefinitions, Assignments.Builder assignments, Context context)
    {
        return measureDefinitions.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> new Measure(rewrite(entry.getValue().getExpressionAndValuePointers(), assignments, context), entry.getValue().getType())));
    }

    private static ExpressionAndValuePointers rewrite(ExpressionAndValuePointers expression, Assignments.Builder assignments, Context context)
    {
        ImmutableList.Builder<ExpressionAndValuePointers.Assignment> rewrittenAssignments = ImmutableList.builder();
        for (ExpressionAndValuePointers.Assignment assignment : expression.getAssignments()) {
            ValuePointer valuePointer = assignment.valuePointer();

            rewrittenAssignments.add(new ExpressionAndValuePointers.Assignment(
                    assignment.symbol(),
                    switch (valuePointer) {
                        case ClassifierValuePointer pointer -> pointer;
                        case MatchNumberValuePointer pointer -> pointer;
                        case ScalarValuePointer pointer -> pointer;
                        case AggregationValuePointer pointer -> {
                            Set<Symbol> runtimeEvaluatedSymbols = ImmutableSet.of(pointer.getClassifierSymbol(), pointer.getMatchNumberSymbol()).stream()
                                    .filter(Optional::isPresent)
                                    .map(Optional::get)
                                    .collect(toImmutableSet());
                            List<Type> argumentTypes = pointer.getFunction().getSignature().getArgumentTypes();

                            ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();
                            for (int i = 0; i < pointer.getArguments().size(); i++) {
                                Expression argument = pointer.getArguments().get(i);
                                if (argument instanceof SymbolReference || SymbolsExtractor.extractUnique(argument).stream()
                                        .anyMatch(runtimeEvaluatedSymbols::contains)) {
                                    rewrittenArguments.add(argument);
                                }
                                else {
                                    Symbol symbol = context.getSymbolAllocator().newSymbol(argument, argumentTypes.get(i));
                                    assignments.put(symbol, argument);
                                    rewrittenArguments.add(symbol.toSymbolReference());
                                }
                            }

                            yield new AggregationValuePointer(
                                    pointer.getFunction(),
                                    pointer.getSetDescriptor(),
                                    rewrittenArguments.build(),
                                    pointer.getClassifierSymbol(),
                                    pointer.getMatchNumberSymbol());
                        }
                    }));
        }

        return new ExpressionAndValuePointers(expression.getExpression(), rewrittenAssignments.build());
    }
}
