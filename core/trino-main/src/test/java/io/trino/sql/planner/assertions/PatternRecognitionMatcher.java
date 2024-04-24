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
package io.trino.sql.planner.assertions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.RowsPerMatch;
import io.trino.sql.planner.plan.SkipToPosition;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointers.Assignment;
import io.trino.sql.planner.rowpattern.ValuePointer;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.plan.RowsPerMatch.ONE;
import static io.trino.sql.planner.plan.SkipToPosition.PAST_LAST;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Objects.requireNonNull;

public class PatternRecognitionMatcher
        implements Matcher
{
    private final Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification;
    private final Optional<WindowNode.Frame> frame;
    private final RowsPerMatch rowsPerMatch;
    private final Set<IrLabel> skipToLabels;
    private final SkipToPosition skipToPosition;
    private final boolean initial;
    private final IrRowPattern pattern;
    private final Map<IrLabel, ExpressionAndValuePointers> variableDefinitions;

    private PatternRecognitionMatcher(
            Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification,
            Optional<WindowNode.Frame> frame,
            RowsPerMatch rowsPerMatch,
            Set<IrLabel> skipToLabels,
            SkipToPosition skipToPosition,
            boolean initial,
            IrRowPattern pattern,
            Map<IrLabel, ExpressionAndValuePointers> variableDefinitions)
    {
        this.specification = requireNonNull(specification, "specification is null");
        this.frame = requireNonNull(frame, "frame is null");
        this.rowsPerMatch = requireNonNull(rowsPerMatch, "rowsPerMatch is null");
        this.skipToLabels = ImmutableSet.copyOf(skipToLabels);
        this.skipToPosition = requireNonNull(skipToPosition, "skipToPosition is null");
        this.initial = initial;
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.variableDefinitions = requireNonNull(variableDefinitions, "variableDefinitions is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof PatternRecognitionNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());

        PatternRecognitionNode patternRecognitionNode = (PatternRecognitionNode) node;

        boolean specificationMatches = specification.map(expected -> expected.getExpectedValue(symbolAliases).equals(patternRecognitionNode.getSpecification())).orElse(true);
        if (!specificationMatches) {
            return NO_MATCH;
        }

        if (frame.isPresent()) {
            if (patternRecognitionNode.getCommonBaseFrame().isEmpty()) {
                return NO_MATCH;
            }
            if (!WindowFrameMatcher.matches(frame.get(), patternRecognitionNode.getCommonBaseFrame().get(), symbolAliases)) {
                return NO_MATCH;
            }
        }

        if (rowsPerMatch != patternRecognitionNode.getRowsPerMatch()) {
            return NO_MATCH;
        }

        if (!skipToLabels.equals(patternRecognitionNode.getSkipToLabels())) {
            return NO_MATCH;
        }

        if (skipToPosition != patternRecognitionNode.getSkipToPosition()) {
            return NO_MATCH;
        }

        if (initial != patternRecognitionNode.isInitial()) {
            return NO_MATCH;
        }

        if (!pattern.equals(patternRecognitionNode.getPattern())) {
            return NO_MATCH;
        }

        if (variableDefinitions.size() != patternRecognitionNode.getVariableDefinitions().size()) {
            return NO_MATCH;
        }

        for (Map.Entry<IrLabel, ExpressionAndValuePointers> entry : variableDefinitions.entrySet()) {
            IrLabel name = entry.getKey();
            ExpressionAndValuePointers actual = patternRecognitionNode.getVariableDefinitions().get(name);
            if (actual == null) {
                return NO_MATCH;
            }

            if (!ExpressionAndValuePointersMatcher.matches(entry.getValue(), actual, symbolAliases)) {
                return NO_MATCH;
            }
        }

        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("specification", specification.orElse(null))
                .add("frame", frame.orElse(null))
                .add("rowsPerMatch", rowsPerMatch)
                .add("skipToLabels", skipToLabels)
                .add("skipToPosition", skipToPosition)
                .add("initial", initial)
                .add("pattern", pattern)
                .add("variableDefinitions", variableDefinitions)
                .toString();
    }

    public static class Builder
    {
        private final PlanMatchPattern source;
        private Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification = Optional.empty();
        private final List<AliasMatcher> windowFunctionMatchers = new LinkedList<>();
        private final Map<String, TypedExpressionAndPointers> measures = new HashMap<>();
        private Optional<WindowNode.Frame> frame = Optional.empty();
        private RowsPerMatch rowsPerMatch = ONE;
        private Set<IrLabel> skipToLabels = ImmutableSet.of();
        private SkipToPosition skipToPosition = PAST_LAST;
        private boolean initial = true;
        private IrRowPattern pattern;
        private final Map<IrLabel, Set<IrLabel>> subsets = new HashMap<>();
        private final Map<IrLabel, ExpressionAndValuePointers> variableDefinitions = new HashMap<>();

        Builder(PlanMatchPattern source)
        {
            this.source = requireNonNull(source, "source is null");
        }

        @CanIgnoreReturnValue
        public Builder specification(ExpectedValueProvider<DataOrganizationSpecification> specification)
        {
            this.specification = Optional.of(specification);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addFunction(String outputAlias, ExpectedValueProvider<WindowFunction> functionCall)
        {
            windowFunctionMatchers.add(new AliasMatcher(Optional.of(outputAlias), new WindowFunctionMatcher(functionCall)));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addMeasure(String outputAlias, Expression expression, Type type)
        {
            measures.put(outputAlias, new TypedExpressionAndPointers(new ExpressionAndValuePointers(expression, ImmutableList.of()), type));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addMeasure(String outputAlias, Expression expression, Map<String, ValuePointer> pointers, Type type)
        {
            List<Assignment> assignments = pointers.entrySet().stream()
                    .map(entry -> new Assignment(new Symbol(UNKNOWN, entry.getKey()), entry.getValue()))
                    .toList();

            measures.put(outputAlias, new TypedExpressionAndPointers(new ExpressionAndValuePointers(expression, assignments), type));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder frame(WindowNode.Frame frame)
        {
            this.frame = Optional.of(frame);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder rowsPerMatch(RowsPerMatch rowsPerMatch)
        {
            this.rowsPerMatch = rowsPerMatch;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder skipTo(SkipToPosition position, IrLabel label)
        {
            this.skipToLabels = ImmutableSet.of(label);
            this.skipToPosition = position;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder skipTo(SkipToPosition position)
        {
            this.skipToPosition = position;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder seek()
        {
            this.initial = false;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder pattern(IrRowPattern pattern)
        {
            this.pattern = pattern;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addSubset(IrLabel name, Set<IrLabel> elements)
        {
            subsets.put(name, elements);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addVariableDefinition(IrLabel name, Expression expression)
        {
            this.variableDefinitions.put(name, new ExpressionAndValuePointers(expression, ImmutableList.of()));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addVariableDefinition(IrLabel name, Expression expression, Map<String, ValuePointer> pointers)
        {
            List<ExpressionAndValuePointers.Assignment> assignments = pointers.entrySet().stream()
                    .map(entry -> new Assignment(new Symbol(BOOLEAN, entry.getKey()), entry.getValue()))
                    .toList();

            this.variableDefinitions.put(name, new ExpressionAndValuePointers(expression, assignments));
            return this;
        }

        public PlanMatchPattern build()
        {
            PlanMatchPattern result = node(PatternRecognitionNode.class, source).with(
                    new PatternRecognitionMatcher(
                            specification,
                            frame,
                            rowsPerMatch,
                            skipToLabels,
                            skipToPosition,
                            initial,
                            pattern,
                            variableDefinitions));
            windowFunctionMatchers.forEach(result::with);
            measures.entrySet().stream()
                    .map(entry -> {
                        String name = entry.getKey();
                        TypedExpressionAndPointers value = entry.getValue();
                        return new AliasMatcher(Optional.of(name), new MeasureMatcher(value.expression(), subsets, value.type()));
                    })
                    .forEach(result::with);
            return result;
        }
    }

    record TypedExpressionAndPointers(ExpressionAndValuePointers expression, Type type) {}
}
