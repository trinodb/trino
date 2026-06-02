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
package io.trino.lib;

import org.weakref.solver.Expression;
import org.weakref.solver.FunctionResolver;
import org.weakref.solver.Specificity;
import org.weakref.solver.TypeScheme;
import org.weakref.solver.TypeSystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.weakref.solver.Specificity.Order.EQUIVALENT;
import static org.weakref.solver.Specificity.Order.INCOMPARABLE;
import static org.weakref.solver.Specificity.Order.LESS_SPECIFIC;
import static org.weakref.solver.Specificity.Order.MORE_SPECIFIC;

/**
 * Trino-faithful overload specificity.
 * <p>
 * Ports the algorithm from Trino's {@code FunctionSpecificityComparator}
 * (<a href="https://github.com/trinodb/trino/pull/28791">trino#28791</a>). Two-phase:
 * <ol>
 *   <li><b>Bidirectional bound-signature forwardability.</b> Try to bind each candidate's bound
 *       formals to the other candidate's declared scheme. The direction that forwards wins.
 *       If only one forwards, that's the decisive answer. If both forward (mutually specific),
 *       fall through to phase 2. If neither forwards, the candidates are {@link Order#INCOMPARABLE
 *       INCOMPARABLE}.</li>
 *   <li><b>Structural declared-signature comparison.</b> Walk the two declared {@link TypeScheme}s
 *       positionally. Concrete types beat type variables. Two concretes compare via coercion
 *       direction ({@link TypeSystem#coercionPlan}) — the side that coerces TO the other is
 *       narrower. Parametric types recurse. Equality patterns ({@code (T, T)} vs
 *       {@code (T, U)}) are compared by subset. Fixed arity beats variadic when all else ties.</li>
 * </ol>
 * Intended composition: {@code Specificity.BY_COERCION_COUNT.then(new TrinoSpecificity(typeSystem))}.
 * The framework default eliminates candidates that require more conversions; this comparator
 * breaks remaining ties.
 */
public final class TrinoSpecificity
        implements Specificity
{
    private final TypeSystem typeSystem;

    public TrinoSpecificity(TypeSystem typeSystem)
    {
        this.typeSystem = typeSystem;
    }

    @Override
    public Order compare(FunctionResolver.Resolution left, FunctionResolver.Resolution right)
    {
        boolean leftForwardsToRight = forwards(left, right.scheme());
        boolean rightForwardsToLeft = forwards(right, left.scheme());

        if (leftForwardsToRight && !rightForwardsToLeft) {
            return MORE_SPECIFIC;
        }
        if (rightForwardsToLeft && !leftForwardsToRight) {
            return LESS_SPECIFIC;
        }
        if (!leftForwardsToRight) {
            // Neither can forward — the candidates are genuinely unrelated.
            return INCOMPARABLE;
        }

        // Both forward (mutually specific). Compare declared signatures structurally.
        int arity = left.argumentCoercions().size();
        return compareDeclared(left.scheme(), right.scheme(), arity);
    }

    /**
     * Phase 1 primitive: can {@code from}'s bound-signature argument types be bound by
     * {@code targetScheme}'s declared form (allowing coercion)?
     * <p>
     * A bound formal can be {@link Expression.AnyRow} if the declared position was a row family
     * wildcard. AnyRow isn't a concrete type that another scheme can bind against, so we
     * conservatively report "cannot forward" in that case.
     */
    private boolean forwards(FunctionResolver.Resolution from, TypeScheme targetScheme)
    {
        List<Expression> boundFormals = from.argumentCoercions().stream()
                .map(FunctionResolver.ArgumentCoercion::formalType)
                .toList();
        if (boundFormals.stream().anyMatch(Expression.AnyRow.class::isInstance)) {
            return false;
        }
        return targetScheme.matchFunctionCallOutcome(boundFormals, typeSystem) instanceof TypeScheme.Satisfied;
    }

    /**
     * Phase 2: structural specificity on the declared schemes.
     */
    private Order compareDeclared(TypeScheme leftScheme, TypeScheme rightScheme, int arity)
    {
        if (!(leftScheme.type() instanceof Expression.FunctionType leftFn)
                || !(rightScheme.type() instanceof Expression.FunctionType rightFn)) {
            return INCOMPARABLE;
        }

        List<Expression> leftArgs = expandArguments(leftFn, arity);
        List<Expression> rightArgs = expandArguments(rightFn, arity);
        if (leftArgs.size() != arity || rightArgs.size() != arity) {
            return INCOMPARABLE;
        }

        Set<String> leftVars = quantifiedVariables(leftScheme);
        Set<String> rightVars = quantifiedVariables(rightScheme);

        Order result = EQUIVALENT;
        for (int i = 0; i < arity; i++) {
            result = combine(result, compareTypes(leftArgs.get(i), rightArgs.get(i), leftVars, rightVars));
            if (result == INCOMPARABLE) {
                return INCOMPARABLE;
            }
        }

        result = combine(result, compareEqualityPatterns(leftArgs, rightArgs, leftVars, rightVars));
        if (result == INCOMPARABLE) {
            return INCOMPARABLE;
        }

        if (result == EQUIVALENT && leftFn.isVariadic() != rightFn.isVariadic()) {
            return leftFn.isVariadic() ? LESS_SPECIFIC : MORE_SPECIFIC;
        }
        return result;
    }

    /**
     * Structural comparison of two type-level expressions at the same argument position.
     */
    private Order compareTypes(Expression left, Expression right, Set<String> leftVars, Set<String> rightVars)
    {
        boolean leftIsTypeVar = isTypeVariable(left, leftVars);
        boolean rightIsTypeVar = isTypeVariable(right, rightVars);
        if (leftIsTypeVar && rightIsTypeVar) {
            return EQUIVALENT;
        }
        if (leftIsTypeVar) {
            return LESS_SPECIFIC;
        }
        if (rightIsTypeVar) {
            return MORE_SPECIFIC;
        }

        if (left.equals(right)) {
            return EQUIVALENT;
        }

        if (Expression.isGround(left) && Expression.isGround(right)) {
            return compareGround(left, right);
        }

        return compareApplication(left, right, leftVars, rightVars);
    }

    /**
     * Order by coercion direction: the side that can coerce TO the other is narrower.
     * If neither coerces (or both do equivalently) → {@link Order#INCOMPARABLE}.
     */
    private Order compareGround(Expression left, Expression right)
    {
        boolean leftToRight = typeSystem.coercionPlan(left, right).isPresent();
        boolean rightToLeft = typeSystem.coercionPlan(right, left).isPresent();
        if (leftToRight && !rightToLeft) {
            return MORE_SPECIFIC;
        }
        if (rightToLeft && !leftToRight) {
            return LESS_SPECIFIC;
        }
        return INCOMPARABLE;
    }

    /**
     * Structural comparison of two parametric applications (same base, same arity).
     * Anything else returns {@link Order#INCOMPARABLE}.
     */
    private Order compareApplication(Expression left, Expression right, Set<String> leftVars, Set<String> rightVars)
    {
        if (!(left instanceof Expression.Application(Expression leftHead, List<Expression> leftArgs))
                || !(right instanceof Expression.Application(Expression rightHead, List<Expression> rightArgs))) {
            return INCOMPARABLE;
        }
        if (!leftHead.equals(rightHead) || leftArgs.size() != rightArgs.size()) {
            return INCOMPARABLE;
        }

        Order result = EQUIVALENT;
        for (int i = 0; i < leftArgs.size(); i++) {
            result = combine(result, compareParameter(leftArgs.get(i), rightArgs.get(i), leftVars, rightVars));
            if (result == INCOMPARABLE) {
                return INCOMPARABLE;
            }
        }
        return result;
    }

    /**
     * Compare one parameter of a parametric type. Parameters may be numeric literals, numeric
     * variables (calculated-literal params like {@code decimal(p, s)}), or types (concrete or
     * variable). Concrete numeric parameters beat variables; two identical literals are
     * equivalent; two numeric variables are equivalent to each other.
     */
    private Order compareParameter(Expression left, Expression right, Set<String> leftVars, Set<String> rightVars)
    {
        if (left instanceof Expression.Literal leftLiteral && right instanceof Expression.Literal rightLiteral) {
            return leftLiteral.value() == rightLiteral.value() ? EQUIVALENT : INCOMPARABLE;
        }
        if (left instanceof Expression.Literal && right instanceof Expression.Variable) {
            return MORE_SPECIFIC;
        }
        if (left instanceof Expression.Variable && right instanceof Expression.Literal) {
            return LESS_SPECIFIC;
        }
        if (left instanceof Expression.Variable && right instanceof Expression.Variable) {
            // Two calculated-literal variables at this position are indistinguishable. If one were
            // a type variable and the other a numeric variable at the same position, the scheme
            // would be ill-kinded; we don't try to untangle that here.
            return EQUIVALENT;
        }
        return compareTypes(left, right, leftVars, rightVars);
    }

    /**
     * Compare the sets of equality obligations each scheme imposes across argument positions.
     * <p>
     * When a quantified variable (type or numeric) appears at multiple locations in a scheme's
     * declared signature, it forces those locations to bind to the same thing. Collect those
     * forced-equality pairs per scheme and compare as sets:
     * <ul>
     *   <li>If the left set is a strict superset of the right's, left imposes strictly more
     *       equalities and is narrower ({@link Order#MORE_SPECIFIC}).</li>
     *   <li>Strict subset → {@link Order#LESS_SPECIFIC}.</li>
     *   <li>Equal sets → {@link Order#EQUIVALENT}.</li>
     *   <li>Otherwise → {@link Order#INCOMPARABLE}.</li>
     * </ul>
     * Example: {@code (T, T)} has one equality pair {(arg0, arg1)}; {@code (T, U)} has none.
     * So {@code (T, T)} is narrower.
     */
    private static Order compareEqualityPatterns(
            List<Expression> leftArgs,
            List<Expression> rightArgs,
            Set<String> leftVars,
            Set<String> rightVars)
    {
        Set<EqualityPair> leftPairs = collectEqualityPairs(leftArgs, leftVars);
        Set<EqualityPair> rightPairs = collectEqualityPairs(rightArgs, rightVars);

        boolean leftContainsAll = leftPairs.containsAll(rightPairs);
        boolean rightContainsAll = rightPairs.containsAll(leftPairs);
        if (leftContainsAll && rightContainsAll) {
            return EQUIVALENT;
        }
        if (leftContainsAll) {
            return MORE_SPECIFIC;
        }
        if (rightContainsAll) {
            return LESS_SPECIFIC;
        }
        return INCOMPARABLE;
    }

    private static Set<EqualityPair> collectEqualityPairs(List<Expression> arguments, Set<String> quantified)
    {
        Map<String, List<Occurrence>> occurrences = new HashMap<>();
        for (int i = 0; i < arguments.size(); i++) {
            collectOccurrences(arguments.get(i), i, List.of(), quantified, occurrences);
        }

        Set<EqualityPair> pairs = new HashSet<>();
        for (List<Occurrence> places : occurrences.values()) {
            for (int i = 0; i < places.size(); i++) {
                for (int j = i + 1; j < places.size(); j++) {
                    pairs.add(new EqualityPair(places.get(i), places.get(j)));
                }
            }
        }
        return pairs;
    }

    private static void collectOccurrences(
            Expression expression,
            int argumentIndex,
            List<Integer> path,
            Set<String> quantified,
            Map<String, List<Occurrence>> occurrences)
    {
        if (expression instanceof Expression.Variable(String name) && quantified.contains(name)) {
            occurrences.computeIfAbsent(name, _ -> new ArrayList<>())
                    .add(new Occurrence(argumentIndex, path));
            return;
        }
        if (expression instanceof Expression.Application(Expression _, List<Expression> args)) {
            for (int i = 0; i < args.size(); i++) {
                List<Integer> nested = new ArrayList<>(path);
                nested.add(i);
                collectOccurrences(args.get(i), argumentIndex, nested, quantified, occurrences);
            }
        }
        // Symbols, literals, rows, function types: no further quantified-variable occurrences
        // to collect for equality-pattern purposes.
    }

    private static List<Expression> expandArguments(Expression.FunctionType fn, int arity)
    {
        List<Expression> expanded = new ArrayList<>(fn.parameterTypes());
        if (fn.isVariadic()) {
            Expression variadicType = fn.variadicParameterType().orElseThrow();
            while (expanded.size() < arity) {
                expanded.add(variadicType);
            }
        }
        return expanded;
    }

    private static Set<String> quantifiedVariables(TypeScheme scheme)
    {
        Set<String> names = new HashSet<>();
        for (Expression.Variable variable : scheme.parameters()) {
            names.add(variable.name());
        }
        return names;
    }

    private static boolean isTypeVariable(Expression expression, Set<String> quantified)
    {
        return expression instanceof Expression.Variable(String name) && quantified.contains(name);
    }

    private static Order combine(Order current, Order next)
    {
        if (current == INCOMPARABLE || next == INCOMPARABLE) {
            return INCOMPARABLE;
        }
        if (current == EQUIVALENT) {
            return next;
        }
        if (next == EQUIVALENT) {
            return current;
        }
        if (current == next) {
            return current;
        }
        // One MORE_SPECIFIC, the other LESS_SPECIFIC — the candidates agree on some dimensions
        // and disagree on others, which means they're INCOMPARABLE overall under partial order.
        return INCOMPARABLE;
    }

    private record EqualityPair(Occurrence first, Occurrence second)
    {
        EqualityPair
        {
            // Normalize order so pair equality is symmetric.
            if (first.compareTo(second) > 0) {
                Occurrence swap = first;
                first = second;
                second = swap;
            }
        }
    }

    private record Occurrence(int argumentIndex, List<Integer> path)
            implements Comparable<Occurrence>
    {
        Occurrence
        {
            path = List.copyOf(path);
        }

        @Override
        public int compareTo(Occurrence other)
        {
            int byArg = Integer.compare(argumentIndex, other.argumentIndex);
            if (byArg != 0) {
                return byArg;
            }
            int minLength = Math.min(path.size(), other.path.size());
            for (int i = 0; i < minLength; i++) {
                int byStep = Integer.compare(path.get(i), other.path.get(i));
                if (byStep != 0) {
                    return byStep;
                }
            }
            return Integer.compare(path.size(), other.path.size());
        }
    }
}
