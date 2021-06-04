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
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.metadata.Metadata;
import io.trino.spi.type.Type;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.plan.PatternRecognitionNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.WindowNode;
import io.trino.sql.planner.rowpattern.ExpressionAndValuePointersEquivalence;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor;
import io.trino.sql.planner.rowpattern.LogicalIndexExtractor.ExpressionAndValuePointers;
import io.trino.sql.planner.rowpattern.ir.IrLabel;
import io.trino.sql.planner.rowpattern.ir.IrRowPattern;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch;
import io.trino.sql.tree.SkipTo;
import io.trino.sql.tree.SymbolReference;

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
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.trino.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.trino.sql.planner.assertions.MatchResult.match;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.iterative.rule.test.PatternRecognitionBuilder.rewriteIdentifiers;
import static io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch.ONE;
import static io.trino.sql.tree.SkipTo.Position.PAST_LAST;
import static java.util.Objects.requireNonNull;

public class PatternRecognitionMatcher
        implements Matcher
{
    private final Optional<ExpectedValueProvider<WindowNode.Specification>> specification;
    private final Optional<ExpectedValueProvider<WindowNode.Frame>> frame;
    private final RowsPerMatch rowsPerMatch;
    private final Optional<IrLabel> skipToLabel;
    private final SkipTo.Position skipToPosition;
    private final boolean initial;
    private final IrRowPattern pattern;
    private final Map<IrLabel, Set<IrLabel>> subsets;
    private final Map<IrLabel, String> variableDefinitions;

    private PatternRecognitionMatcher(
            Optional<ExpectedValueProvider<WindowNode.Specification>> specification,
            Optional<ExpectedValueProvider<WindowNode.Frame>> frame,
            RowsPerMatch rowsPerMatch,
            Optional<IrLabel> skipToLabel,
            SkipTo.Position skipToPosition,
            boolean initial,
            IrRowPattern pattern,
            Map<IrLabel, Set<IrLabel>> subsets,
            Map<IrLabel, String> variableDefinitions)
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

        for (Map.Entry<IrLabel, String> entry : variableDefinitions.entrySet()) {
            IrLabel name = entry.getKey();
            ExpressionAndValuePointers actual = patternRecognitionNode.getVariableDefinitions().get(name);
            if (actual == null) {
                return NO_MATCH;
            }
            ExpressionAndValuePointers rewritten = rewrite(entry.getValue(), subsets);
            ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
            if (!ExpressionAndValuePointersEquivalence.equivalent(
                    actual,
                    rewritten,
                    (actualSymbol, expectedSymbol) -> verifier.process(actualSymbol.toSymbolReference(), expectedSymbol.toSymbolReference()))) {
                return NO_MATCH;
            }
        }

        return match();
    }

    static ExpressionAndValuePointers rewrite(String definition, Map<IrLabel, Set<IrLabel>> subsets)
    {
        Expression expression = rewriteIdentifiers(new SqlParser().createExpression(definition, new ParsingOptions()));
        Map<Symbol, Type> types = extractExpressions(ImmutableList.of(expression), SymbolReference.class).stream()
                .collect(toImmutableMap(Symbol::from, reference -> BIGINT));
        return LogicalIndexExtractor.rewrite(expression, subsets, new SymbolAllocator(types));
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
        private Optional<ExpectedValueProvider<WindowNode.Specification>> specification = Optional.empty();
        private final List<AliasMatcher> windowFunctionMatchers = new LinkedList<>();
        private final Map<String, Map.Entry<String, Type>> measures = new HashMap<>();
        private Optional<ExpectedValueProvider<WindowNode.Frame>> frame = Optional.empty();
        private RowsPerMatch rowsPerMatch = ONE;
        private Optional<IrLabel> skipToLabel = Optional.empty();
        private SkipTo.Position skipToPosition = PAST_LAST;
        private boolean initial = true;
        private IrRowPattern pattern;
        private final Map<IrLabel, Set<IrLabel>> subsets = new HashMap<>();
        private final Map<IrLabel, String> variableDefinitions = new HashMap<>();

        Builder(PlanMatchPattern source)
        {
            this.source = requireNonNull(source, "source is null");
        }

        public Builder specification(ExpectedValueProvider<WindowNode.Specification> specification)
        {
            this.specification = Optional.of(specification);
            return this;
        }

        public Builder addFunction(String outputAlias, ExpectedValueProvider<FunctionCall> functionCall)
        {
            windowFunctionMatchers.add(new AliasMatcher(Optional.of(outputAlias), new WindowFunctionMatcher(functionCall, Optional.empty(), Optional.empty())));
            return this;
        }

        public Builder addMeasure(String outputAlias, String expression, Type type)
        {
            measures.put(outputAlias, new AbstractMap.SimpleEntry<>(expression, type));
            return this;
        }

        public Builder frame(ExpectedValueProvider<WindowNode.Frame> frame)
        {
            this.frame = Optional.of(frame);
            return this;
        }

        public Builder rowsPerMatch(RowsPerMatch rowsPerMatch)
        {
            this.rowsPerMatch = rowsPerMatch;
            return this;
        }

        public Builder skipTo(SkipTo.Position position, IrLabel label)
        {
            this.skipToLabel = Optional.of(label);
            this.skipToPosition = position;
            return this;
        }

        public Builder skipTo(SkipTo.Position position)
        {
            this.skipToPosition = position;
            return this;
        }

        public Builder seek()
        {
            this.initial = false;
            return this;
        }

        public Builder pattern(IrRowPattern pattern)
        {
            this.pattern = pattern;
            return this;
        }

        public Builder addSubset(IrLabel name, Set<IrLabel> elements)
        {
            subsets.put(name, elements);
            return this;
        }

        public Builder addVariableDefinition(IrLabel name, String expression)
        {
            this.variableDefinitions.put(name, expression);
            return this;
        }

        public PlanMatchPattern build()
        {
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
                            variableDefinitions));
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
