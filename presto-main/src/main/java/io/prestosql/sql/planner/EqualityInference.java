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
package io.prestosql.sql.planner;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.util.DisjointSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.prestosql.sql.ExpressionUtils.extractConjuncts;
import static io.prestosql.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.prestosql.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.prestosql.sql.planner.NullabilityAnalyzer.mayReturnNullOnNonNullInput;
import static java.util.Objects.requireNonNull;

/**
 * Makes equality based inferences to rewrite Expressions and generate equality sets in terms of specified symbol scopes
 */
public class EqualityInference
{
    // Ordering used to determine Expression preference when determining canonicals
    private static final Ordering<Expression> CANONICAL_ORDERING = Ordering.from((expression1, expression2) -> {
        // Current cost heuristic:
        // 1) Prefer fewer input symbols
        // 2) Prefer smaller expression trees
        // 3) Sort the expressions alphabetically - creates a stable consistent ordering (extremely useful for unit testing)
        // TODO: be more precise in determining the cost of an expression
        return ComparisonChain.start()
                .compare(SymbolsExtractor.extractAll(expression1).size(), SymbolsExtractor.extractAll(expression2).size())
                .compare(SubExpressionExtractor.extract(expression1).size(), SubExpressionExtractor.extract(expression2).size())
                .compare(expression1.toString(), expression2.toString())
                .result();
    });

    private final Multimap<Expression, Expression> equalitySets; // Indexed by canonical expression
    private final Map<Expression, Expression> canonicalMap; // Map each known expression to canonical expression
    private final Set<Expression> derivedExpressions;

    private EqualityInference(Multimap<Expression, Expression> equalitySets, Map<Expression, Expression> canonicalMap, Set<Expression> derivedExpressions)
    {
        this.equalitySets = equalitySets;
        this.canonicalMap = canonicalMap;
        this.derivedExpressions = derivedExpressions;
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
            List<Expression> candidates = equalitySet.stream()
                    .filter(candidate -> !derivedExpressions.contains(candidate))
                    .collect(Collectors.toList());

            for (Expression candidate : candidates) {
                Expression scopeRewritten = rewrite(candidate, scope::contains, false);
                if (scopeRewritten != null) {
                    scopeExpressions.add(scopeRewritten);
                }
                Expression scopeComplementRewritten = rewrite(candidate, expression -> !scope.contains(expression), false);
                if (scopeComplementRewritten != null) {
                    scopeComplementExpressions.add(scopeComplementRewritten);
                }
                if (scopeRewritten == null && scopeComplementRewritten == null) {
                    scopeStraddlingExpressions.add(candidate);
                }
            }
            // Compile the equality expressions on each side of the scope
            Expression matchingCanonical = getCanonical(scopeExpressions);
            if (scopeExpressions.size() >= 2) {
                scopeExpressions.stream()
                        .filter(expression -> !expression.equals(matchingCanonical))
                        .map(expression -> new ComparisonExpression(ComparisonExpression.Operator.EQUAL, matchingCanonical, expression))
                        .forEach(scopeEqualities::add);
            }
            Expression complementCanonical = getCanonical(scopeComplementExpressions);
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
            Expression connectingCanonical = getCanonical(connectingExpressions);
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
        if (expression instanceof ComparisonExpression &&
                isDeterministic(expression, metadata) &&
                !mayReturnNullOnNonNullInput(expression)) {
            ComparisonExpression comparison = (ComparisonExpression) expression;
            if (comparison.getOperator() == ComparisonExpression.Operator.EQUAL) {
                // We should only consider equalities that have distinct left and right components
                return !comparison.getLeft().equals(comparison.getRight());
            }
        }
        return false;
    }

    public static EqualityInference newInstance(Metadata metadata, Expression... expressions)
    {
        return newInstance(metadata, Arrays.asList(expressions));
    }

    public static EqualityInference newInstance(Metadata metadata, Collection<Expression> expressions)
    {
        DisjointSet<Expression> equalities = new DisjointSet<>();
        List<Expression> candidates = expressions.stream()
                .flatMap(expression -> extractConjuncts(expression).stream())
                .filter(expression -> isInferenceCandidate(metadata, expression))
                .collect(Collectors.toList());

        for (Expression expression : candidates) {
            ComparisonExpression comparison = (ComparisonExpression) expression;
            Expression expression1 = comparison.getLeft();
            Expression expression2 = comparison.getRight();

            equalities.findAndUnion(expression1, expression2);
        }

        Collection<Set<Expression>> equivalentClasses = equalities.getEquivalentClasses();

        // Map every expression to the set of equivalent expressions
        Map<Expression, Set<Expression>> byExpression = new HashMap<>();
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

            List<Expression> subExpressions = SubExpressionExtractor.extract(expression).stream()
                    .filter(e -> !e.equals(expression))
                    .collect(Collectors.toList());

            for (Expression subExpression : subExpressions) {
                Set<Expression> equivalences = byExpression.getOrDefault(subExpression, ImmutableSet.of()).stream()
                        .filter(e -> !e.equals(subExpression))
                        .collect(Collectors.toSet());

                for (Expression equivalentSubExpression : equivalences) {
                    Expression rewritten = replaceExpression(expression, ImmutableMap.of(subExpression, equivalentSubExpression));
                    equalities.findAndUnion(expression, rewritten);
                    derivedExpressions.add(rewritten);
                }
            }
        }

        Multimap<Expression, Expression> equalitySets = makeEqualitySets(equalities);

        ImmutableMap.Builder<Expression, Expression> canonicalMappings = ImmutableMap.builder();
        for (Map.Entry<Expression, Expression> entry : equalitySets.entries()) {
            Expression canonical = entry.getKey();
            Expression expression = entry.getValue();
            canonicalMappings.put(expression, canonical);
        }

        return new EqualityInference(equalitySets, canonicalMappings.build(), derivedExpressions);
    }

    /**
     * Provides a convenience Iterable of Expression conjuncts which have not been added to the inference
     */
    public static List<Expression> nonInferrableConjuncts(Metadata metadata, Expression expression)
    {
        return extractConjuncts(expression).stream()
            .filter(e -> !isInferenceCandidate(metadata, e))
            .collect(Collectors.toList());
    }

    private Expression rewrite(Expression expression, Predicate<Symbol> symbolScope, boolean allowFullReplacement)
    {
        Set<Expression> subExpressions = SubExpressionExtractor.extract(expression);
        if (!allowFullReplacement) {
            subExpressions = subExpressions.stream()
                    .filter(e -> !e.equals(expression))
                    .collect(Collectors.toSet());
        }

        ImmutableMap.Builder<Expression, Expression> expressionRemap = ImmutableMap.builder();
        for (Expression subExpression : subExpressions) {
            Expression canonical = getScopedCanonical(subExpression, symbolScope);
            if (canonical != null) {
                expressionRemap.put(subExpression, canonical);
            }
        }

        // Perform a naive single-pass traversal to try to rewrite non-compliant portions of the tree. Prefers to replace
        // larger subtrees over smaller subtrees
        // TODO: this rewrite can probably be made more sophisticated
        Expression rewritten = replaceExpression(expression, expressionRemap.build());
        if (!isScoped(rewritten, symbolScope)) {
            // If the rewritten is still not compliant with the symbol scope, just give up
            return null;
        }
        return rewritten;
    }

    /**
     * Returns the most preferrable expression to be used as the canonical expression
     */
    private static Expression getCanonical(Collection<Expression> expressions)
    {
        if (expressions.isEmpty()) {
            return null;
        }
        return CANONICAL_ORDERING.min(expressions);
    }

    /**
     * Returns a canonical expression that is fully contained by the symbolScope and that is equivalent
     * to the specified expression. Returns null if unable to to find a canonical.
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

        Set<Expression> candidates = equivalences.stream()
                .filter(e -> isScoped(e, symbolScope))
                .collect(Collectors.toSet());

        return getCanonical(candidates);
    }

    private static boolean isScoped(Expression expression, Predicate<Symbol> symbolScope)
    {
        return SymbolsExtractor.extractUnique(expression).stream().allMatch(symbolScope);
    }

    private static Multimap<Expression, Expression> makeEqualitySets(DisjointSet<Expression> equalities)
    {
        ImmutableSetMultimap.Builder<Expression, Expression> builder = ImmutableSetMultimap.builder();
        for (Set<Expression> equalityGroup : equalities.getEquivalentClasses()) {
            if (!equalityGroup.isEmpty()) {
                builder.putAll(CANONICAL_ORDERING.min(equalityGroup), equalityGroup);
            }
        }
        return builder.build();
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
