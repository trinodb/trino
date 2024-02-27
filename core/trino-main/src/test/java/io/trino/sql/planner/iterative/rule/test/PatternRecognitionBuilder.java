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
package io.trino.sql.planner.iterative.rule.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.type.Type;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PatternRecognitionNode.Measure;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.RowsPerMatch;
import io.trino.sql.planner.plan.SkipToPosition;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import io.trino.sql.tree.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.planner.plan.RowsPerMatch.ONE;
import static io.trino.sql.planner.plan.SkipToPosition.PAST_LAST;

public class PatternRecognitionBuilder
{
    private PlanNode source;
    private List<Symbol> partitionBy = ImmutableList.of();
    private Optional<OrderingScheme> orderBy = Optional.empty();
    private final Map<Symbol, WindowNode.Function> windowFunctions = new HashMap<>();
    private final Map<Symbol, Measure> measures = new HashMap<>();
    private Optional<WindowNode.Frame> commonBaseFrame = Optional.empty();
    private RowsPerMatch rowsPerMatch = ONE;
    private Set<IrLabel> skipToLabels = ImmutableSet.of();
    private SkipToPosition skipToPosition = PAST_LAST;
    private boolean initial = true;
    private IrRowPattern pattern;
    private final Map<IrLabel, Set<IrLabel>> subsets = new HashMap<>();
    private final Map<IrLabel, ExpressionAndValuePointers> variableDefinitions = new HashMap<>();

    public PatternRecognitionBuilder source(PlanNode source)
    {
        this.source = source;
        return this;
    }

    public PatternRecognitionBuilder partitionBy(List<Symbol> partitionBy)
    {
        this.partitionBy = partitionBy;
        return this;
    }

    public PatternRecognitionBuilder orderBy(OrderingScheme orderingScheme)
    {
        this.orderBy = Optional.of(orderingScheme);
        return this;
    }

    public PatternRecognitionBuilder addWindowFunction(Symbol symbol, WindowNode.Function function)
    {
        this.windowFunctions.put(symbol, function);
        return this;
    }

    public PatternRecognitionBuilder addMeasure(Symbol symbol, Expression expression, Map<String, ValuePointer> pointers, Type type)
    {
        List<ExpressionAndValuePointers.Assignment> assignments = pointers.entrySet().stream()
                .map(entry -> new ExpressionAndValuePointers.Assignment(new Symbol(entry.getKey()), entry.getValue()))
                .toList();

        this.measures.put(symbol, new Measure(new ExpressionAndValuePointers(expression, assignments), type));
        return this;
    }

    public PatternRecognitionBuilder addMeasure(Symbol symbol, Expression expression, Type type)
    {
        return addMeasure(symbol, expression, ImmutableMap.of(), type);
    }

    public PatternRecognitionBuilder frame(WindowNode.Frame frame)
    {
        this.commonBaseFrame = Optional.of(frame);
        return this;
    }

    public PatternRecognitionBuilder rowsPerMatch(RowsPerMatch rowsPerMatch)
    {
        this.rowsPerMatch = rowsPerMatch;
        return this;
    }

    public PatternRecognitionBuilder skipTo(SkipToPosition position, Set<IrLabel> labels)
    {
        this.skipToPosition = position;
        this.skipToLabels = ImmutableSet.copyOf(labels);
        return this;
    }

    public PatternRecognitionBuilder skipTo(SkipToPosition position)
    {
        this.skipToPosition = position;
        return this;
    }

    public PatternRecognitionBuilder seek()
    {
        this.initial = false;
        return this;
    }

    public PatternRecognitionBuilder pattern(IrRowPattern pattern)
    {
        this.pattern = pattern;
        return this;
    }

    public PatternRecognitionBuilder addSubset(IrLabel name, Set<IrLabel> elements)
    {
        this.subsets.put(name, elements);
        return this;
    }

    public PatternRecognitionBuilder addVariableDefinition(IrLabel name, Expression expression, Map<String, ValuePointer> pointers)
    {
        List<ExpressionAndValuePointers.Assignment> assignments = pointers.entrySet().stream()
                .map(entry -> new ExpressionAndValuePointers.Assignment(new Symbol(entry.getKey()), entry.getValue()))
                .toList();

        this.variableDefinitions.put(name, new ExpressionAndValuePointers(expression, assignments));
        return this;
    }

    public PatternRecognitionBuilder addVariableDefinition(IrLabel name, Expression expression)
    {
        return addVariableDefinition(name, expression, ImmutableMap.of());
    }

    public PatternRecognitionNode build(PlanNodeIdAllocator idAllocator)
    {
        return new PatternRecognitionNode(
                idAllocator.getNextId(),
                source,
                new DataOrganizationSpecification(partitionBy, orderBy),
                Optional.empty(),
                ImmutableSet.of(),
                0,
                windowFunctions,
                measures,
                commonBaseFrame,
                rowsPerMatch,
                skipToLabels,
                skipToPosition,
                initial,
                pattern,
                variableDefinitions);
    }
}
