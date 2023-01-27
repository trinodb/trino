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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointersEquivalence;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch;
import io.trino.sql.tree.SkipTo;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.PatternRecognitionExpressionRewriter.rewrite;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ONE;
import static io.trino.sql.tree.SkipTo.Position.PAST_LAST;
import static java.util.Objects.requireNonNull;

public class PatternRecognitionMatcher
        implements Matcher
{
    private final Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification;
    private final Optional<ExpectedValueProvider<WindowNode.Frame>> frame;
    private final RowsPerMatch rowsPerMatch;
    private final Optional<IrLabel> skipToLabel;
    private final SkipTo.Position skipToPosition;
    private final boolean initial;
    private final IrRowPattern pattern;
    private final Map<IrLabel, Set<IrLabel>> subsets;
    private final Map<IrLabel, ExpressionAndValuePointers> variableDefinitions;

    private PatternRecognitionMatcher(
            Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification,
            Optional<ExpectedValueProvider<WindowNode.Frame>> frame,
            RowsPerMatch rowsPerMatch,
            Optional<IrLabel> skipToLabel,
            SkipTo.Position skipToPosition,
            boolean initial,
            IrRowPattern pattern,
            Map<IrLabel, Set<IrLabel>> subsets,
            Map<IrLabel, ExpressionAndValuePointers> variableDefinitions)
    {
        this.specification = requireNonNull(specification, "specification is null");
        this.frame = requireNonNull(frame, "frame is null");
        this.rowsPerMatch = requireNonNull(rowsPerMatch, "rowsPerMatch is null");
        this.skipToLabel = requireNonNull(skipToLabel, "skipToLabel is null");
        this.skipToPosition = requireNonNull(skipToPosition, "skipToPosition is null");
        this.initial = initial;
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.subsets = requireNonNull(subsets, "subsets is null");
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
            if (!frame.get().getExpectedValue(symbolAliases).equals(patternRecognitionNode.getCommonBaseFrame().get())) {
                return NO_MATCH;
            }
        }

        if (rowsPerMatch != patternRecognitionNode.getRowsPerMatch()) {
            return NO_MATCH;
        }

        if (!skipToLabel.equals(patternRecognitionNode.getSkipToLabel())) {
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

        if (!subsets.equals(patternRecognitionNode.getSubsets())) {
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
            ExpressionAndValuePointers expected = entry.getValue();
            ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
            if (!ExpressionAndValuePointersEquivalence.equivalent(
                    actual,
                    expected,
                    (actualSymbol, expectedSymbol) -> verifier.process(actualSymbol.toSymbolReference(), expectedSymbol.toSymbolReference()))) {
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
                .add("skipToLabel", skipToLabel.orElse(null))
                .add("skipToPosition", skipToPosition)
                .add("initial", initial)
                .add("pattern", pattern)
                .add("subsets", subsets)
                .add("variableDefinitions", variableDefinitions)
                .toString();
    }

    public static class Builder
    {
        private final PlanMatchPattern source;
        private Optional<ExpectedValueProvider<DataOrganizationSpecification>> specification = Optional.empty();
        private final List<AliasMatcher> windowFunctionMatchers = new LinkedList<>();
        private final Map<String, Map.Entry<String, Type>> measures = new HashMap<>();
        private Optional<ExpectedValueProvider<WindowNode.Frame>> frame = Optional.empty();
        private RowsPerMatch rowsPerMatch = ONE;
        private Optional<IrLabel> skipToLabel = Optional.empty();
        private SkipTo.Position skipToPosition = PAST_LAST;
        private boolean initial = true;
        private IrRowPattern pattern;
        private final Map<IrLabel, Set<IrLabel>> subsets = new HashMap<>();
        private final Map<IrLabel, String> variableDefinitionsBySql = new HashMap<>();
        private final Map<IrLabel, Expression> variableDefinitionsByExpression = new HashMap<>();

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
        public Builder addFunction(String outputAlias, ExpectedValueProvider<FunctionCall> functionCall)
        {
            windowFunctionMatchers.add(new AliasMatcher(Optional.of(outputAlias), new WindowFunctionMatcher(functionCall, Optional.empty(), Optional.empty())));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addMeasure(String outputAlias, String expression, Type type)
        {
            measures.put(outputAlias, new AbstractMap.SimpleEntry<>(expression, type));
            return this;
        }

        @CanIgnoreReturnValue
        public Builder frame(ExpectedValueProvider<WindowNode.Frame> frame)
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
        public Builder skipTo(SkipTo.Position position, IrLabel label)
        {
            this.skipToLabel = Optional.of(label);
            this.skipToPosition = position;
            return this;
        }

        @CanIgnoreReturnValue
        public Builder skipTo(SkipTo.Position position)
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
        public Builder addVariableDefinition(IrLabel name, String expression)
        {
            this.variableDefinitionsBySql.put(name, expression);
            return this;
        }

        @CanIgnoreReturnValue
        public Builder addVariableDefinition(IrLabel name, Expression expression)
        {
            this.variableDefinitionsByExpression.put(name, expression);
            return this;
        }

        public PlanMatchPattern build()
        {
            ImmutableMap.Builder<IrLabel, ExpressionAndValuePointers> variableDefinitions = ImmutableMap.<IrLabel, ExpressionAndValuePointers>builder()
                    .putAll(variableDefinitionsBySql.entrySet().stream()
                            .collect(toImmutableMap(Map.Entry::getKey, entry -> rewrite(entry.getValue(), subsets))))
                    .putAll(variableDefinitionsByExpression.entrySet().stream()
                            .collect(toImmutableMap(Map.Entry::getKey, entry -> rewrite(entry.getValue(), subsets))));

            PlanMatchPattern result = node(PatternRecognitionNode.class, source).with(
                    new PatternRecognitionMatcher(
                            specification,
                            frame,
                            rowsPerMatch,
                            skipToLabel,
                            skipToPosition,
                            initial,
                            pattern,
                            subsets,
                            variableDefinitions.buildOrThrow()));
            windowFunctionMatchers.forEach(result::with);
            measures.entrySet().stream()
                    .map(entry -> {
                        String name = entry.getKey();
                        Map.Entry<String, Type> definition = entry.getValue();
                        return new AliasMatcher(Optional.of(name), new MeasureMatcher(definition.getKey(), subsets, definition.getValue()));
                    })
                    .forEach(result::with);
            return result;
        }
    }
}
