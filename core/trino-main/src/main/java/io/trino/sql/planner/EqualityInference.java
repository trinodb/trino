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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import io.trino.metadata.Metadata;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.SymbolReference;
import io.trino.util.DisjointSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ExpressionUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.NullabilityAnalyzer.mayReturnNullOnNonNullInput;
import static java.util.Objects.requireNonNull;

/**
 * Makes equality based inferences to rewrite Expressions and generate equality sets in terms of specified symbol scopes
 */
public class EqualityInference
{
    // Comparator used to determine Expression preference when determining canonicals
    private final Comparator<Expression> canonicalComparator;
    private final Multimap<Expression, Expression> equalitySets; // Indexed by canonical expression
    private final Map<Expression, Expression> canonicalMap; // Map each known expression to canonical expression
    private final Set<Expression> derivedExpressions;
    private final Map<Expression, List<Expression>> expressionCache = new HashMap<>();
    private final Map<Expression, List<Symbol>> symbolsCache = new HashMap<>();
    private final Map<Expression, Set<Symbol>> uniqueSymbolsCache = new HashMap<>();

    public EqualityInference(Metadata metadata, Expression... expressions)
    {
        this(metadata, Arrays.asList(expressions));
    }

    public EqualityInference(Metadata metadata, Collection<Expression> expressions)
    {
        DisjointSet<Expression> equalities = new DisjointSet<>();
        expressions.stream()
                .flatMap(expression -> extractConjuncts(expression).stream())
                .filter(expression -> isInferenceCandidate(metadata, expression))
                .forEach(expression -> {
                    ComparisonExpression comparison = (ComparisonExpression) expression;
                    Expression expression1 = comparison.getLeft();
                    Expression expression2 = comparison.getRight();

                    equalities.findAndUnion(expression1, expression2);
                });

        Collection<Set<Expression>> equivalentClasses = equalities.getEquivalentClasses();

        // Map every expression to the set of equivalent expressions
        Map<Expression, Set<Expression>> byExpression = new LinkedHashMap<>();
        for (Set<Expression> equivalence : equivalentClasses) {
            equivalence.forEach(expression -> byExpression.put(expression, equivalence));
        }

        // For every non-derived expression, extract the sub-expressions and see if they can be rewritten as other expressions. If so,
        // use this new information to update the known equalities.
        Set<Expression> derivedExpressions = new LinkedHashSet<>();
        for (Expression expression : byExpression.keySet()) {
            if (derivedExpressions.contains(expression)) {
                continue;
            }

            extractSubExpressions(expression)
                    .stream()
                    .filter(e -> !e.equals(expression))
                    .forEach(subExpression -> byExpression.getOrDefault(subExpression, ImmutableSet.of())
                            .stream()
                            .filter(e -> !e.equals(subExpression))
                            .forEach(equivalentSubExpression -> {
                                Expression rewritten = replaceExpression(expression, ImmutableMap.of(subExpression, equivalentSubExpression));
                                equalities.findAndUnion(expression, rewritten);
                                derivedExpressions.add(rewritten);
                            }));
        }

        Comparator<Expression> canonicalComparator = Comparator
                // Current cost heuristic:
                // 1) Prefer fewer input symbols
                // 2) Prefer smaller expression trees
                // 3) Sort the expressions alphabetically - creates a stable consistent ordering (extremely useful for unit testing)
                // TODO: be more precise in determining the cost of an expression
                .comparingInt((ToIntFunction<Expression>) (expression -> extractAllSymbols(expression).size()))
                .thenComparingLong(expression -> extractSubExpressions(expression).size())
                .thenComparing(Expression::toString);

        Multimap<Expression, Expression> equalitySets = makeEqualitySets(equalities, canonicalComparator);

        ImmutableMap.Builder<Expression, Expression> canonicalMappings = ImmutableMap.builder();
        for (Map.Entry<Expression, Expression> entry : equalitySets.entries()) {
            Expression canonical = entry.getKey();
            Expression expression = entry.getValue();
            canonicalMappings.put(expression, canonical);
        }

        this.equalitySets = equalitySets;
        this.canonicalMap = canonicalMappings.buildOrThrow();
        this.derivedExpressions = derivedExpressions;
        this.canonicalComparator = canonicalComparator;
    }

    /**
     * Attempts to rewrite an Expression in terms of the symbols allowed by the symbol scope
     * given the known equalities. Returns null if unsuccessful.
     */
    public Expression rewrite(Expression expression, Set<Symbol> scope)
    {
        return rewrite(expression, scope::contains, true);
    }

    /**
     * Dumps the inference equalities as equality expressions that are partitioned by the symbolScope.
     * All stored equalities are returned in a compact set and will be classified into three groups as determined by the symbol scope:
     * <ol>
     * <li>equalities that fit entirely within the symbol scope</li>
     * <li>equalities that fit entirely outside of the symbol scope</li>
     * <li>equalities that straddle the symbol scope</li>
     * </ol>
     * <pre>
     * Example:
     *   Stored Equalities:
     *     a = b = c
     *     d = e = f = g
     *
     *   Symbol Scope:
     *     a, b, d, e
     *
     *   Output EqualityPartition:
     *     Scope Equalities:
     *       a = b
     *       d = e
     *     Complement Scope Equalities
     *       f = g
     *     Scope Straddling Equalities
     *       a = c
     *       d = f
     * </pre>
     */
    public EqualityPartition generateEqualitiesPartitionedBy(Set<Symbol> scope)
    {
        ImmutableSet.Builder<Expression> scopeEqualities = ImmutableSet.builder();
        ImmutableSet.Builder<Expression> scopeComplementEqualities = ImmutableSet.builder();
        ImmutableSet.Builder<Expression> scopeStraddlingEqualities = ImmutableSet.builder();

        for (Collection<Expression> equalitySet : equalitySets.asMap().values()) {
            Set<Expression> scopeExpressions = new LinkedHashSet<>();
            Set<Expression> scopeComplementExpressions = new LinkedHashSet<>();
            Set<Expression> scopeStraddlingExpressions = new LinkedHashSet<>();

            // Try to push each non-derived expression into one side of the scope
            equalitySet.stream()
                    .filter(candidate -> !derivedExpressions.contains(candidate))
                    .forEach(candidate -> {
                        Expression scopeRewritten = rewrite(candidate, scope::contains, false);
                        if (scopeRewritten != null) {
                            scopeExpressions.add(scopeRewritten);
                        }
                        Expression scopeComplementRewritten = rewrite(candidate, symbol -> !scope.contains(symbol), false);
                        if (scopeComplementRewritten != null) {
                            scopeComplementExpressions.add(scopeComplementRewritten);
                        }
                        if (scopeRewritten == null && scopeComplementRewritten == null) {
                            scopeStraddlingExpressions.add(candidate);
                        }
                    });
            // Compile the equality expressions on each side of the scope
            Expression matchingCanonical = getCanonical(scopeExpressions.stream());
            if (scopeExpressions.size() >= 2) {
                scopeExpressions.stream()
                        .filter(expression -> !expression.equals(matchingCanonical))
                        .map(expression -> new ComparisonExpression(ComparisonExpression.Operator.EQUAL, matchingCanonical, expression))
                        .forEach(scopeEqualities::add);
            }
            Expression complementCanonical = getCanonical(scopeComplementExpressions.stream());
            if (scopeComplementExpressions.size() >= 2) {
                scopeComplementExpressions.stream()
                        .filter(expression -> !expression.equals(complementCanonical))
                        .map(expression -> new ComparisonExpression(ComparisonExpression.Operator.EQUAL, complementCanonical, expression))
                        .forEach(scopeComplementEqualities::add);
            }

            // Compile the scope straddling equality expressions
            List<Expression> connectingExpressions = new ArrayList<>();
            connectingExpressions.add(matchingCanonical);
            connectingExpressions.add(complementCanonical);
            connectingExpressions.addAll(scopeStraddlingExpressions);
            connectingExpressions = connectingExpressions.stream()
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            Expression connectingCanonical = getCanonical(connectingExpressions.stream());
            if (connectingCanonical != null) {
                connectingExpressions.stream()
                        .filter(expression -> !expression.equals(connectingCanonical))
                        .map(expression -> new ComparisonExpression(ComparisonExpression.Operator.EQUAL, connectingCanonical, expression))
                        .forEach(scopeStraddlingEqualities::add);
            }
        }

        return new EqualityPartition(scopeEqualities.build(), scopeComplementEqualities.build(), scopeStraddlingEqualities.build());
    }

    /**
     * Determines whether an Expression may be successfully applied to the equality inference
     */
    public static boolean isInferenceCandidate(Metadata metadata, Expression expression)
    {
        if (expression instanceof ComparisonExpression comparison &&
                isDeterministic(expression, metadata) &&
                !mayReturnNullOnNonNullInput(expression)) {
            if (comparison.getOperator() == ComparisonExpression.Operator.EQUAL) {
                // We should only consider equalities that have distinct left and right components
                return !comparison.getLeft().equals(comparison.getRight());
            }
        }
        return false;
    }

    /**
     * Provides a convenience Stream of Expression conjuncts which have not been added to the inference
     */
    public static Stream<Expression> nonInferrableConjuncts(Metadata metadata, Expression expression)
    {
        return extractConjuncts(expression).stream()
                .filter(e -> !isInferenceCandidate(metadata, e));
    }

    private Expression rewrite(Expression expression, Predicate<Symbol> symbolScope, boolean allowFullReplacement)
    {
        Map<Expression, Expression> expressionRemap = new HashMap<>();
        extractSubExpressions(expression)
                .stream()
                .filter(allowFullReplacement
                        ? subExpression -> true
                        : subExpression -> !subExpression.equals(expression))
                .forEach(subExpression -> {
                    Expression canonical = getScopedCanonical(subExpression, symbolScope);
                    if (canonical != null) {
                        expressionRemap.putIfAbsent(subExpression, canonical);
                    }
                });

        // Perform a naive single-pass traversal to try to rewrite non-compliant portions of the tree. Prefers to replace
        // larger subtrees over smaller subtrees
        // TODO: this rewrite can probably be made more sophisticated
        Expression rewritten = replaceExpression(expression, expressionRemap);
        if (!isScoped(rewritten, symbolScope)) {
            // If the rewritten is still not compliant with the symbol scope, just give up
            return null;
        }
        return rewritten;
    }

    /**
     * Returns the most preferrable expression to be used as the canonical expression
     */
    private Expression getCanonical(Stream<Expression> expressions)
    {
        return expressions.min(canonicalComparator).orElse(null);
    }

    /**
     * Returns a canonical expression that is fully contained by the symbolScope and that is equivalent
     * to the specified expression. Returns null if unable to find a canonical.
     */
    @VisibleForTesting
    Expression getScopedCanonical(Expression expression, Predicate<Symbol> symbolScope)
    {
        Expression canonicalIndex = canonicalMap.get(expression);
        if (canonicalIndex == null) {
            return null;
        }

        Collection<Expression> equivalences = equalitySets.get(canonicalIndex);
        if (expression instanceof SymbolReference) {
            boolean inScope = equivalences.stream()
                    .filter(SymbolReference.class::isInstance)
                    .map(Symbol::from)
                    .anyMatch(symbolScope);

            if (!inScope) {
                return null;
            }
        }

        return getCanonical(
                equivalences.stream()
                        .filter(e -> isScoped(e, symbolScope)));
    }

    private boolean isScoped(Expression expression, Predicate<Symbol> symbolScope)
    {
        return extractUniqueSymbols(expression).stream().allMatch(symbolScope);
    }

    private static Multimap<Expression, Expression> makeEqualitySets(DisjointSet<Expression> equalities, Comparator<Expression> canonicalComparator)
    {
        ImmutableSetMultimap.Builder<Expression, Expression> builder = ImmutableSetMultimap.builder();
        for (Set<Expression> equalityGroup : equalities.getEquivalentClasses()) {
            if (!equalityGroup.isEmpty()) {
                builder.putAll(equalityGroup.stream().min(canonicalComparator).get(), equalityGroup);
            }
        }
        return builder.build();
    }

    private List<Expression> extractSubExpressions(Expression expression)
    {
        return expressionCache.computeIfAbsent(expression, e -> SubExpressionExtractor.extract(e).collect(toImmutableList()));
    }

    private Set<Symbol> extractUniqueSymbols(Expression expression)
    {
        return uniqueSymbolsCache.computeIfAbsent(expression, e -> ImmutableSet.copyOf(extractAllSymbols(expression)));
    }

    private List<Symbol> extractAllSymbols(Expression expression)
    {
        return symbolsCache.computeIfAbsent(expression, SymbolsExtractor::extractAll);
    }

    public static class EqualityPartition
    {
        private final List<Expression> scopeEqualities;
        private final List<Expression> scopeComplementEqualities;
        private final List<Expression> scopeStraddlingEqualities;

        public EqualityPartition(Iterable<Expression> scopeEqualities, Iterable<Expression> scopeComplementEqualities, Iterable<Expression> scopeStraddlingEqualities)
        {
            this.scopeEqualities = ImmutableList.copyOf(requireNonNull(scopeEqualities, "scopeEqualities is null"));
            this.scopeComplementEqualities = ImmutableList.copyOf(requireNonNull(scopeComplementEqualities, "scopeComplementEqualities is null"));
            this.scopeStraddlingEqualities = ImmutableList.copyOf(requireNonNull(scopeStraddlingEqualities, "scopeStraddlingEqualities is null"));
        }

        public List<Expression> getScopeEqualities()
        {
            return scopeEqualities;
        }

        public List<Expression> getScopeComplementEqualities()
        {
            return scopeComplementEqualities;
        }

        public List<Expression> getScopeStraddlingEqualities()
        {
            return scopeStraddlingEqualities;
        }
    }
}
