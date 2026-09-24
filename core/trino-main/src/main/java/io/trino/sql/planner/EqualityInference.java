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
import io.trino.metadata.Metadata;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.ComparisonOperator;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IrExpressions.Comparison;
import io.trino.sql.ir.IrUtils;
import io.trino.sql.ir.Reference;
import io.trino.type.CharVarcharCoercion;
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
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.ir.IrExpressions.matchComparison;
import static io.trino.sql.ir.IrExpressions.mayReturnNullOnNonNullInput;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.planner.DeterminismEvaluator.isDeterministic;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static java.util.Objects.requireNonNull;

/**
 * Makes equality based inferences to rewrite Expressions and generate equality sets in terms of specified symbol scopes
 */
public class EqualityInference
{
    private final CharVarcharCoercion charVarcharCoercion;
    private final Metadata metadata;
    // Comparator used to determine Expression preference when determining canonicals
    private final Comparator<Expression> canonicalComparator;
    // Every equality class, and the class each known expression belongs to
    private final List<EqualityClass> equalityClasses;
    private final Map<Expression, EqualityClass> classByExpression;
    // Cached per-expression facts, computed once and shared by the comparator and the scope checks
    private final Map<Expression, ExpressionInfo> expressionInfo = new HashMap<>();

    /**
     * One set of mutually equal expressions. {@code members} holds them all, {@code primary} holds
     * only the ones the inference was given, keeping the expressions derived by substitution apart.
     */
    private record EqualityClass(Expression canonical, List<Expression> members, List<Expression> primary) {}

    private record ExpressionInfo(List<Expression> subExpressions, int symbolCount, Set<Symbol> uniqueSymbols) {}

    public EqualityInference(PlannerContext plannerContext, CharVarcharCoercion charVarcharCoercion, Expression... expressions)
    {
        this(plannerContext, charVarcharCoercion, Arrays.asList(expressions));
    }

    public EqualityInference(PlannerContext plannerContext, CharVarcharCoercion charVarcharCoercion, Collection<Expression> expressions)
    {
        requireNonNull(plannerContext, "plannerContext is null");

        this.charVarcharCoercion = requireNonNull(charVarcharCoercion, "charVarcharCoercion is null");
        this.metadata = plannerContext.getMetadata();

        DisjointSet<Expression> equalities = new DisjointSet<>();
        expressions.stream()
                .flatMap(expression -> extractConjuncts(expression).stream())
                .filter(expression -> isInferenceCandidate(plannerContext, charVarcharCoercion, expression))
                .forEach(expression -> {
                    Comparison comparison = requireNonNull(matchComparison(expression), "expression is not a comparison");
                    Expression expression1 = comparison.left();
                    Expression expression2 = comparison.right();

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

        // Current cost heuristic:
        // 1) Prefer fewer input symbols
        // 2) Prefer smaller expression trees
        // 3) Sort the expressions alphabetically - creates a stable consistent ordering (extremely useful for unit testing)
        // TODO: be more precise in determining the cost of an expression
        Comparator<Expression> canonicalComparator = (left, right) -> {
            ExpressionInfo leftInfo = expressionInfo(left);
            ExpressionInfo rightInfo = expressionInfo(right);
            int bySymbols = Integer.compare(leftInfo.symbolCount(), rightInfo.symbolCount());
            if (bySymbols != 0) {
                return bySymbols;
            }
            int bySize = Integer.compare(leftInfo.subExpressions().size(), rightInfo.subExpressions().size());
            if (bySize != 0) {
                return bySize;
            }
            return left.toString().compareTo(right.toString());
        };

        ImmutableList.Builder<EqualityClass> equalityClasses = ImmutableList.builder();
        ImmutableMap.Builder<Expression, EqualityClass> classByExpression = ImmutableMap.builder();
        for (Set<Expression> equalityGroup : equalities.getEquivalentClasses()) {
            if (equalityGroup.isEmpty()) {
                continue;
            }
            Expression canonical = equalityGroup.stream().min(canonicalComparator).get();
            List<Expression> members = ImmutableList.copyOf(equalityGroup);
            List<Expression> primary = members.stream()
                    .filter(expression -> !derivedExpressions.contains(expression))
                    .collect(toImmutableList());
            EqualityClass equalityClass = new EqualityClass(canonical, members, primary);
            equalityClasses.add(equalityClass);
            for (Expression member : members) {
                classByExpression.put(member, equalityClass);
            }
        }

        this.equalityClasses = equalityClasses.build();
        this.classByExpression = classByExpression.buildOrThrow();
        this.canonicalComparator = canonicalComparator;
    }

    /**
     * Returns the classes of expressions known to be equal to each other, including expressions
     * derived by substituting equal sub-expressions.
     */
    public Collection<Collection<Expression>> getEqualitySets()
    {
        return equalityClasses.stream()
                .map(equalityClass -> (Collection<Expression>) equalityClass.members())
                .collect(toImmutableList());
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

        for (EqualityClass equalityClass : equalityClasses) {
            Set<Expression> scopeExpressions = new LinkedHashSet<>();
            Set<Expression> scopeComplementExpressions = new LinkedHashSet<>();
            Set<Expression> scopeStraddlingExpressions = new LinkedHashSet<>();

            // Try to push each non-derived expression into one side of the scope
            for (Expression candidate : equalityClass.primary()) {
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
            }
            // Compile the equality expressions on each side of the scope
            Expression matchingCanonical = getCanonical(scopeExpressions);
            if (scopeExpressions.size() >= 2) {
                scopeExpressions.stream()
                        .filter(expression -> !expression.equals(matchingCanonical))
                        .map(expression -> comparison(metadata, charVarcharCoercion, ComparisonOperator.EQUAL, matchingCanonical, expression))
                        .forEach(scopeEqualities::add);
            }
            Expression complementCanonical = getCanonical(scopeComplementExpressions);
            if (scopeComplementExpressions.size() >= 2) {
                scopeComplementExpressions.stream()
                        .filter(expression -> !expression.equals(complementCanonical))
                        .map(expression -> comparison(metadata, charVarcharCoercion, ComparisonOperator.EQUAL, complementCanonical, expression))
                        .forEach(scopeComplementEqualities::add);
            }

            // Compile single equality between matching and complement scope.
            // Only consider expressions that don't have derived expression in other scope.
            // Otherwise, redundant equality would be generated.
            Expression matchingConnecting = getConnecting(scopeExpressions, symbol -> !scope.contains(symbol));
            Expression complementConnecting = getConnecting(scopeComplementExpressions, scope::contains);
            if (matchingConnecting != null && complementConnecting != null && !matchingConnecting.equals(complementConnecting)) {
                scopeStraddlingEqualities.add(comparison(metadata, charVarcharCoercion, ComparisonOperator.EQUAL, matchingConnecting, complementConnecting));
            }

            // Compile the scope straddling equality expressions.
            // scopeStraddlingExpressions couldn't be pushed to either side,
            // therefore there needs to be an equality generated with
            // one of the scopes (either matching or complement).
            List<Expression> straddlingExpressions = new ArrayList<>();
            if (matchingCanonical != null) {
                straddlingExpressions.add(matchingCanonical);
            }
            else if (complementCanonical != null) {
                straddlingExpressions.add(complementCanonical);
            }
            straddlingExpressions.addAll(scopeStraddlingExpressions);
            Expression connectingCanonical = getCanonical(straddlingExpressions);
            if (connectingCanonical != null) {
                straddlingExpressions.stream()
                        .filter(expression -> !expression.equals(connectingCanonical))
                        .map(expression -> comparison(metadata, charVarcharCoercion, ComparisonOperator.EQUAL, connectingCanonical, expression))
                        .forEach(scopeStraddlingEqualities::add);
            }
        }

        return new EqualityPartition(scopeEqualities.build().asList(), scopeComplementEqualities.build().asList(), scopeStraddlingEqualities.build().asList());
    }

    /**
     * The equalities that fit entirely within the symbol scope.
     */
    public List<Expression> generateScopeEqualities(Set<Symbol> scope)
    {
        ImmutableList.Builder<Expression> equalities = ImmutableList.builder();
        for (EqualityClass equalityClass : equalityClasses) {
            Set<Expression> scopeExpressions = new LinkedHashSet<>();
            for (Expression candidate : equalityClass.primary()) {
                Expression rewritten = rewrite(candidate, scope::contains, false);
                if (rewritten != null) {
                    scopeExpressions.add(rewritten);
                }
            }
            if (scopeExpressions.size() < 2) {
                continue;
            }
            Expression canonical = getCanonical(scopeExpressions);
            for (Expression expression : scopeExpressions) {
                if (!expression.equals(canonical)) {
                    equalities.add(comparison(metadata, charVarcharCoercion, ComparisonOperator.EQUAL, canonical, expression));
                }
            }
        }
        return equalities.build();
    }

    /**
     * Determines whether an Expression may be successfully applied to the equality inference
     */
    public static boolean isInferenceCandidate(PlannerContext plannerContext, CharVarcharCoercion charVarcharCoercion, Expression expression)
    {
        return matchComparison(expression) instanceof Comparison comparison
                && comparison.operator() == ComparisonOperator.EQUAL
                && isDeterministic(expression)
                && !mayReturnNullOnNonNullInput(plannerContext, charVarcharCoercion, expression)
                // We should only consider equalities that have distinct left and right components
                && !comparison.left().equals(comparison.right());
    }

    /**
     * Provides a convenience Stream of Expression conjuncts which have not been added to the inference
     */
    public static Stream<Expression> nonInferrableConjuncts(PlannerContext plannerContext, CharVarcharCoercion charVarcharCoercion, Expression expression)
    {
        return extractConjuncts(expression).stream()
                .filter(e -> !isInferenceCandidate(plannerContext, charVarcharCoercion, e));
    }

    private Expression rewrite(Expression expression, Predicate<Symbol> symbolScope, boolean allowFullReplacement)
    {
        Map<Expression, Expression> expressionRemap = null;
        for (Expression subExpression : extractSubExpressions(expression)) {
            if (!allowFullReplacement && subExpression.equals(expression)) {
                continue;
            }
            Expression canonical = getScopedCanonical(subExpression, symbolScope);
            if (canonical != null) {
                if (expressionRemap == null) {
                    expressionRemap = new HashMap<>();
                }
                expressionRemap.putIfAbsent(subExpression, canonical);
            }
        }

        // Perform a naive single-pass traversal to try to rewrite non-compliant portions of the tree. Prefers to replace
        // larger subtrees over smaller subtrees
        // TODO: this rewrite can probably be made more sophisticated
        // no substitution leaves the expression as it is
        Expression rewritten = expressionRemap == null ? expression : replaceExpression(expression, expressionRemap);
        if (!isScoped(rewritten, symbolScope)) {
            // If the rewritten is still not compliant with the symbol scope, just give up
            return null;
        }
        return rewritten;
    }

    /**
     * Returns the most preferrable expression to be used as the canonical expression
     */
    private Expression getCanonical(Collection<Expression> expressions)
    {
        Expression canonical = null;
        for (Expression expression : expressions) {
            if (canonical == null || canonicalComparator.compare(expression, canonical) < 0) {
                canonical = expression;
            }
        }
        return canonical;
    }

    /**
     * The canonical expression of one side of a scope that can connect to the other side, which is
     * one that is either constant or cannot itself be rewritten into {@code otherScope}.
     */
    private Expression getConnecting(Collection<Expression> expressions, Predicate<Symbol> otherScope)
    {
        Expression connecting = null;
        for (Expression expression : expressions) {
            if (connecting != null && canonicalComparator.compare(expression, connecting) >= 0) {
                continue;
            }
            if (expressionInfo(expression).symbolCount() == 0 || rewrite(expression, otherScope, false) == null) {
                connecting = expression;
            }
        }
        return connecting;
    }

    /**
     * Returns a canonical expression that is fully contained by the symbolScope and that is equivalent
     * to the specified expression. Returns null if unable to find a canonical.
     */
    @VisibleForTesting
    Expression getScopedCanonical(Expression expression, Predicate<Symbol> symbolScope)
    {
        EqualityClass equalityClass = classByExpression.get(expression);
        if (equalityClass == null) {
            return null;
        }

        Collection<Expression> equivalences = equalityClass.members();
        if (expression instanceof Reference) {
            boolean inScope = false;
            for (Expression equivalence : equivalences) {
                if (equivalence instanceof Reference && symbolScope.test(Symbol.from(equivalence))) {
                    inScope = true;
                    break;
                }
            }

            if (!inScope) {
                return null;
            }
        }

        Expression canonical = null;
        for (Expression equivalence : equivalences) {
            if (isScoped(equivalence, symbolScope) && (canonical == null || canonicalComparator.compare(equivalence, canonical) < 0)) {
                canonical = equivalence;
            }
        }
        return canonical;
    }

    private boolean isScoped(Expression expression, Predicate<Symbol> symbolScope)
    {
        for (Symbol symbol : extractUniqueSymbols(expression)) {
            if (!symbolScope.test(symbol)) {
                return false;
            }
        }
        return true;
    }

    private Set<Symbol> extractUniqueSymbols(Expression expression)
    {
        return expressionInfo(expression).uniqueSymbols();
    }

    private List<Expression> extractSubExpressions(Expression expression)
    {
        return expressionInfo(expression).subExpressions();
    }

    private ExpressionInfo expressionInfo(Expression expression)
    {
        return expressionInfo.computeIfAbsent(expression, e -> {
            List<Expression> subExpressions = IrUtils.preOrder(e).collect(toImmutableList());
            List<Symbol> symbols = SymbolsExtractor.extractAll(e);
            return new ExpressionInfo(subExpressions, symbols.size(), ImmutableSet.copyOf(symbols));
        });
    }

    public record EqualityPartition(List<Expression> scopeEqualities, List<Expression> scopeComplementEqualities, List<Expression> scopeStraddlingEqualities)
    {
        public EqualityPartition
        {
            scopeEqualities = ImmutableList.copyOf(requireNonNull(scopeEqualities, "scopeEqualities is null"));
            scopeComplementEqualities = ImmutableList.copyOf(requireNonNull(scopeComplementEqualities, "scopeComplementEqualities is null"));
            scopeStraddlingEqualities = ImmutableList.copyOf(requireNonNull(scopeStraddlingEqualities, "scopeStraddlingEqualities is null"));
        }
    }
}
