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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import io.trino.sql.analyzer.Analysis.Range;
import io.trino.sql.tree.AnchorPattern;
import io.trino.sql.tree.ExcludedPattern;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.MeasureDefinition;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.PatternRecognitionRelation;
import io.trino.sql.tree.PatternRecognitionRelation.RowsPerMatch;
import io.trino.sql.tree.PatternSearchMode;
import io.trino.sql.tree.RangeQuantifier;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.SkipTo;
import io.trino.sql.tree.SubsetDefinition;
import io.trino.sql.tree.VariableDefinition;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.INVALID_LABEL;
import static io.trino.spi.StandardErrorCode.INVALID_PATTERN_RECOGNITION_FUNCTION;
import static io.trino.spi.StandardErrorCode.INVALID_PROCESSING_MODE;
import static io.trino.spi.StandardErrorCode.INVALID_RANGE;
import static io.trino.spi.StandardErrorCode.INVALID_ROW_PATTERN;
import static io.trino.spi.StandardErrorCode.NESTED_ROW_PATTERN_RECOGNITION;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractExpressions;
import static io.trino.sql.analyzer.SemanticExceptions.semanticException;
import static io.trino.sql.tree.ProcessingMode.Mode.FINAL;
import static io.trino.sql.util.AstUtils.preOrder;
import static java.util.Objects.requireNonNull;

public class PatternRecognitionAnalyzer
{
    private PatternRecognitionAnalyzer() {}

    public static PatternRecognitionAnalysis analyze(
            List<SubsetDefinition> subsets,
            List<VariableDefinition> variableDefinitions,
            List<MeasureDefinition> measures,
            RowPattern pattern,
            Optional<SkipTo> skipTo)
    {
        // extract label names (Identifiers) from PATTERN and SUBSET clauses. create labels respecting SQL identifier semantics
        Set<String> primaryLabels = extractExpressions(ImmutableList.of(pattern), Identifier.class).stream()
                .map(PatternRecognitionAnalyzer::label)
                .collect(toImmutableSet());
        List<String> unionLabels = subsets.stream()
                .map(SubsetDefinition::getName)
                .map(PatternRecognitionAnalyzer::label)
                .collect(toImmutableList());

        // analyze SUBSET
        Set<String> unique = new HashSet<>();
        for (SubsetDefinition subset : subsets) {
            String label = label(subset.getName());
            if (primaryLabels.contains(label)) {
                throw semanticException(INVALID_LABEL, subset.getName(), "union pattern variable name: %s is a duplicate of primary pattern variable name", subset.getName());
            }
            if (!unique.add(label)) {
                throw semanticException(INVALID_LABEL, subset.getName(), "union pattern variable name: %s is declared twice", subset.getName());
            }
            for (Identifier element : subset.getIdentifiers()) {
                // TODO can there be repetitions in the list of subset elements? (currently repetitions are supported)
                if (!primaryLabels.contains(label(element))) {
                    throw semanticException(INVALID_LABEL, element, "subset element: %s is not a primary pattern variable", element);
                }
            }
        }

        // analyze DEFINE
        unique = new HashSet<>();
        for (VariableDefinition definition : variableDefinitions) {
            String label = label(definition.getName());
            if (!primaryLabels.contains(label)) {
                throw semanticException(INVALID_LABEL, definition.getName(), "defined variable: %s is not a primary pattern variable", definition.getName());
            }
            if (!unique.add(label)) {
                throw semanticException(INVALID_LABEL, definition.getName(), "pattern variable with name: %s is defined twice", definition.getName());
            }
            // DEFINE clause only supports RUNNING semantics which is default
            Expression expression = definition.getExpression();
            extractExpressions(ImmutableList.of(expression), FunctionCall.class).stream()
                    .filter(functionCall -> functionCall.getProcessingMode().map(mode -> mode.getMode() == FINAL).orElse(false))
                    .findFirst()
                    .ifPresent(functionCall -> {
                        throw semanticException(INVALID_PROCESSING_MODE, functionCall.getProcessingMode().get(), "FINAL semantics is not supported in DEFINE clause");
                    });
        }
        // record primary labels without definitions. they are implicitly associated with `true` condition
        Set<String> undefinedLabels = Sets.difference(primaryLabels, unique);

        // validate pattern quantifiers
        ImmutableMap.Builder<NodeRef<RangeQuantifier>, Range> ranges = ImmutableMap.builder();
        preOrder(pattern)
                .filter(RangeQuantifier.class::isInstance)
                .map(RangeQuantifier.class::cast)
                .forEach(quantifier -> {
                    Optional<Long> atLeast = quantifier.getAtLeast().map(LongLiteral::getParsedValue);
                    atLeast.ifPresent(value -> {
                        if (value < 0) {
                            throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, quantifier, "Pattern quantifier lower bound must be greater than or equal to 0");
                        }
                        if (value > Integer.MAX_VALUE) {
                            throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, quantifier, "Pattern quantifier lower bound must not exceed " + Integer.MAX_VALUE);
                        }
                    });
                    Optional<Long> atMost = quantifier.getAtMost().map(LongLiteral::getParsedValue);
                    atMost.ifPresent(value -> {
                        if (value < 1) {
                            throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, quantifier, "Pattern quantifier upper bound must be greater than or equal to 1");
                        }
                        if (value > Integer.MAX_VALUE) {
                            throw semanticException(NUMERIC_VALUE_OUT_OF_RANGE, quantifier, "Pattern quantifier upper bound must not exceed " + Integer.MAX_VALUE);
                        }
                    });
                    if (atLeast.isPresent() && atMost.isPresent()) {
                        if (atLeast.get() > atMost.get()) {
                            throw semanticException(INVALID_RANGE, quantifier, "Pattern quantifier lower bound must not exceed upper bound");
                        }
                    }
                    ranges.put(NodeRef.of(quantifier), new Range(atLeast.map(Math::toIntExact), atMost.map(Math::toIntExact)));
                });

        // validate AFTER MATCH SKIP
        Set<String> allLabels = ImmutableSet.<String>builder()
                .addAll(primaryLabels)
                .addAll(unionLabels)
                .build();
        skipTo.flatMap(SkipTo::getIdentifier)
                .ifPresent(identifier -> {
                    String label = label(identifier);
                    if (!allLabels.contains(label)) {
                        throw semanticException(INVALID_LABEL, identifier, "%s is not a primary or union pattern variable", identifier);
                    }
                });

        // check no prohibited nesting: cannot nest one row pattern recognition within another
        List<Expression> expressions = Streams.concat(
                measures.stream()
                        .map(MeasureDefinition::getExpression),
                variableDefinitions.stream()
                        .map(VariableDefinition::getExpression))
                .collect(toImmutableList());
        expressions.forEach(expression -> preOrder(expression)
                .filter(child -> child instanceof PatternRecognitionRelation || child instanceof RowPattern)
                .findFirst()
                .ifPresent(nested -> {
                    throw semanticException(NESTED_ROW_PATTERN_RECOGNITION, nested, "nested row pattern recognition in row pattern recognition");
                }));

        return new PatternRecognitionAnalysis(allLabels, undefinedLabels, ranges.buildOrThrow());
    }

    public static void validateNoPatternSearchMode(Optional<PatternSearchMode> patternSearchMode)
    {
        patternSearchMode.ifPresent(mode -> {
            throw semanticException(NOT_SUPPORTED, mode, "Pattern search modifier: %s is not allowed in MATCH_RECOGNIZE clause", mode.getMode());
        });
    }

    public static void validatePatternExclusions(Optional<RowsPerMatch> rowsPerMatch, RowPattern pattern)
    {
        // exclusion syntax is not allowed in row pattern if ALL ROWS PER MATCH WITH UNMATCHED ROWS is specified
        if (rowsPerMatch.isPresent() && rowsPerMatch.get().isUnmatchedRows()) {
            preOrder(pattern)
                    .filter(ExcludedPattern.class::isInstance)
                    .findFirst()
                    .ifPresent(exclusion -> {
                        throw semanticException(INVALID_ROW_PATTERN, exclusion, "Pattern exclusion syntax is not allowed when ALL ROWS PER MATCH WITH UNMATCHED ROWS is specified");
                    });
        }
    }

    public static void validateNoPatternAnchors(RowPattern pattern)
    {
        preOrder(pattern)
                .filter(AnchorPattern.class::isInstance)
                .findFirst()
                .ifPresent(anchor -> {
                    throw semanticException(INVALID_ROW_PATTERN, anchor, "Anchor pattern syntax is not allowed in window");
                });
    }

    public static void validateNoMatchNumber(List<MeasureDefinition> measures, List<VariableDefinition> variableDefinitions, Set<NodeRef<FunctionCall>> patternRecognitionFunctions)
    {
        List<Expression> expressions = Streams.concat(
                measures.stream()
                        .map(MeasureDefinition::getExpression),
                variableDefinitions.stream()
                        .map(VariableDefinition::getExpression))
                .collect(toImmutableList());
        expressions.forEach(expression -> preOrder(expression)
                .filter(child -> patternRecognitionFunctions.contains(NodeRef.of(child)))
                .filter(child -> ((FunctionCall) child).getName().getSuffix().equalsIgnoreCase("MATCH_NUMBER"))
                .findFirst()
                .ifPresent(matchNumber -> {
                    throw semanticException(INVALID_PATTERN_RECOGNITION_FUNCTION, matchNumber, "MATCH_NUMBER function is not supported in window");
                }));
    }

    private static String label(Identifier identifier)
    {
        return identifier.getCanonicalValue();
    }

    public static class PatternRecognitionAnalysis
    {
        private final Set<String> allLabels;
        private final Set<String> undefinedLabels;
        private final Map<NodeRef<RangeQuantifier>, Range> ranges;

        public PatternRecognitionAnalysis(Set<String> allLabels, Set<String> undefinedLabels, Map<NodeRef<RangeQuantifier>, Range> ranges)
        {
            this.allLabels = requireNonNull(allLabels, "allLabels is null");
            this.undefinedLabels = ImmutableSet.copyOf(requireNonNull(undefinedLabels, "undefinedLabels is null"));
            this.ranges = ImmutableMap.copyOf(requireNonNull(ranges, "ranges is null"));
        }

        public Set<String> getAllLabels()
        {
            return allLabels;
        }

        public Set<String> getUndefinedLabels()
        {
            return undefinedLabels;
        }

        public Map<NodeRef<RangeQuantifier>, Range> getRanges()
        {
            return ranges;
        }
    }
}
