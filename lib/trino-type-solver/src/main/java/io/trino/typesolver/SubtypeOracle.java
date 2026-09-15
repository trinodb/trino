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
package io.trino.typesolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/// Answers "is A a subtype of B?" by delegating to a [Solver] and caching the result.
///
/// The solver is invoked with a single `Subtype(left, right)` constraint; whatever
/// outcome it reaches ([Solver.Satisfied], [Solver.Unsatisfied],
/// [Solver.Incomplete]) maps to a [Relation] value. Because that call can itself
/// ask subtype questions recursively (e.g. for structural decomposition), an
/// in-progress set guards against infinite recursion by returning [Relation#INCOMPLETE]
/// for cycles.
///
/// Used by
/// [FunctionResolver] for argument-specificity comparisons, and by the [Solver]
/// internals for refining alternatives.
public final class SubtypeOracle
{
    private final TypeSystem typeSystem;
    private final Map<Key, Relation> cache = new HashMap<>();
    private final Map<Expression, Optional<Boolean>> numericValidity = new HashMap<>();
    private final Set<Key> inProgress = new HashSet<>();

    public SubtypeOracle(TypeSystem typeSystem)
    {
        this.typeSystem = typeSystem;
    }

    /// @return [Relation#SATISFIED] iff the solver proves `left` is a subtype of
    ///         `right`, [Relation#UNSATISFIED] iff it proves the opposite, or
    ///         [Relation#INCOMPLETE] if the relation depends on unresolved variables.
    public synchronized Relation classify(Expression left, Expression right)
    {
        return ResolutionBudget.nested(() -> classifyInternal(left, right));
    }

    private Relation classifyInternal(Expression left, Expression right)
    {
        if (left.equals(right)) {
            return Relation.SATISFIED;
        }
        Key original = new Key(left, right);
        Relation cached = cache.get(original);
        if (cached != null) {
            return cached;
        }
        Key key = key(left, right);
        cached = cache.get(key);
        if (cached != null) {
            cache.put(original, cached);
            return cached;
        }
        if (!inProgress.add(key)) {
            return Relation.INCOMPLETE;
        }
        try {
            Relation result = classifyUncached(left, right);
            cache.put(key, result);
            cache.put(original, result);
            return result;
        }
        finally {
            inProgress.remove(key);
        }
    }

    public boolean isSubtype(Expression left, Expression right)
    {
        return classify(left, right) == Relation.SATISFIED;
    }

    private static Key key(Expression left, Expression right)
    {
        if (Expression.isGround(left) && Expression.isGround(right)) {
            return new Key(left, right);
        }
        // A subtype query owns its fresh variables. Cache alpha-equivalent questions
        // together so each field of a wide row does not solve the same scalar rules
        // again just because its coercion rules allocated different variable names.
        Map<String, Expression> variables = new LinkedHashMap<>();
        if (isFlat(left) && isFlat(right)) {
            return new Key(canonicalizeFlat(left, variables), canonicalizeFlat(right, variables));
        }
        for (String name : Solver.variables(left)) {
            variables.put(name, Expression.variable("$canonical" + variables.size()));
        }
        for (String name : Solver.variables(right)) {
            variables.computeIfAbsent(name, _ -> Expression.variable("$canonical" + variables.size()));
        }
        if (variables.isEmpty()) {
            return new Key(left, right);
        }
        return new Key(Expression.rewrite(left, variables), Expression.rewrite(right, variables));
    }

    private static boolean isFlat(Expression expression)
    {
        if (expression instanceof Expression.Application(Expression.Symbol _, List<Expression> arguments)) {
            for (Expression argument : arguments) {
                if (!(argument instanceof Expression.Symbol || argument instanceof Expression.Literal || argument instanceof Expression.Variable)) {
                    return false;
                }
            }
            return true;
        }
        return expression instanceof Expression.Symbol || expression instanceof Expression.Literal || expression instanceof Expression.Variable;
    }

    // Only variables change; retain ground arguments and applications by identity.
    @SuppressWarnings("ReferenceEquality")
    private static Expression canonicalizeFlat(Expression expression, Map<String, Expression> variables)
    {
        ResolutionBudget.consume();
        if (expression instanceof Expression.Variable(String name)) {
            return variables.computeIfAbsent(name, _ -> Expression.variable("$canonical" + variables.size()));
        }
        if (expression instanceof Expression.Application(Expression head, List<Expression> arguments)) {
            // The flat-shape check guarantees a symbol head and depth at most one.
            ResolutionBudget.consume();
            List<Expression> rewritten = null;
            for (int index = 0; index < arguments.size(); index++) {
                Expression argument = arguments.get(index);
                Expression canonical = canonicalizeFlat(argument, variables);
                if (canonical != argument) {
                    if (rewritten == null) {
                        rewritten = new ArrayList<>(arguments);
                    }
                    rewritten.set(index, canonical);
                }
            }
            if (rewritten != null) {
                return new Expression.Application(head, rewritten);
            }
        }
        return expression;
    }

    private Relation classifyUncached(Expression left, Expression right)
    {
        Optional<Relation> numeric = classifyGroundNumericTypes(left, right);
        if (numeric.isPresent()) {
            return numeric.orElseThrow();
        }
        // Structural comparisons do not need another solver or a coercion plan. Reuse
        // this oracle for the components so repeated fields and nested suffixes share
        // their answers. Open structures still need the solver's binding machinery.
        if (Expression.isGround(left) && Expression.isGround(right)) {
            if (left instanceof Expression.Row(List<Expression.RowField> leftFields) &&
                    right instanceof Expression.Row(List<Expression.RowField> rightFields)) {
                return classifyComponents(leftFields.stream().map(Expression.RowField::type).toList(), rightFields.stream().map(Expression.RowField::type).toList());
            }
            if (left instanceof Expression.Application(Expression.Symbol(String leftName), List<Expression> leftArguments) &&
                    right instanceof Expression.Application(Expression.Symbol(String rightName), List<Expression> rightArguments) &&
                    leftName.equals(rightName) && typeSystem.canDecomposeCoercion(left, right, leftName)) {
                return classifyComponents(leftArguments, rightArguments);
            }
        }
        if (!Expression.isGround(left) || !Expression.isGround(right)) {
            Optional<Relation> simpleNumeric = classifySimpleNumericTypes(left, right);
            if (simpleNumeric.isPresent()) {
                return simpleNumeric.orElseThrow();
            }
            boolean matched = false;
            boolean onlyConditionalRepresentationBridges = true;
            for (CoercionRule rule : typeSystem.candidateCoercions(left, right)) {
                Optional<Set<Constraint>> match = matchConstraints(rule, new VariableAllocator(), left, right);
                if (match.isEmpty()) {
                    continue;
                }
                matched = true;
                if (!rule.isRepresentationBridge() || match.orElseThrow().isEmpty()) {
                    onlyConditionalRepresentationBridges = false;
                    break;
                }
            }
            // A conditional match proves only that some assignment of the open parameters makes
            // the relation hold. It does not prove the symbolic subtype relation itself. This
            // distinction matters for witness ordering: an exact representation bridge such as
            // unbounded varchar -> varchar(n), guarded by n == MAX_VALUE, must not make unbounded
            // varchar look narrower than every bounded varchar(n).
            if (matched && onlyConditionalRepresentationBridges) {
                return Relation.INCOMPLETE;
            }
        }
        return switch (new Solver(typeSystem, this).solveOutcome(List.of(new Subtype(left, right)))) {
            case Solver.Satisfied _ -> Relation.SATISFIED;
            case Solver.Unsatisfied _ -> Relation.UNSATISFIED;
            case Solver.Incomplete _ -> Relation.INCOMPLETE;
        };
    }

    private Relation classifyComponents(List<Expression> left, List<Expression> right)
    {
        if (left.size() != right.size()) {
            return Relation.UNSATISFIED;
        }
        Relation result = Relation.SATISFIED;
        for (int index = 0; index < left.size(); index++) {
            Relation component = classify(left.get(index), right.get(index));
            if (component == Relation.UNSATISFIED) {
                return component;
            }
            if (component == Relation.INCOMPLETE) {
                result = component;
            }
        }
        return result;
    }

    /// Scalar and numeric-parameter types often reduce a rule to literal comparisons.
    /// Evaluate those obligations directly, including constructor validation. A rule
    /// with any other obligation still uses the full solver.
    private Optional<Relation> classifyGroundNumericTypes(Expression left, Expression right)
    {
        if (!isGroundNumericType(left) || !isGroundNumericType(right)) {
            return Optional.empty();
        }
        Optional<Boolean> leftValid = numericValidity.computeIfAbsent(left, type -> evaluateNumericGuards(typeSystem.instantiateValidationConstraints(type)));
        Optional<Boolean> rightValid = numericValidity.computeIfAbsent(right, type -> evaluateNumericGuards(typeSystem.instantiateValidationConstraints(type)));
        if (leftValid.equals(Optional.of(false)) || rightValid.equals(Optional.of(false))) {
            return Optional.of(Relation.UNSATISFIED);
        }
        if (leftValid.isEmpty() || rightValid.isEmpty()) {
            return Optional.empty();
        }
        boolean needsSolver = false;
        for (CoercionRule rule : typeSystem.candidateCoercions(left, right)) {
            ResolutionBudget.consume();
            Optional<Set<Constraint>> match = matchConstraints(rule, new VariableAllocator(), left, right);
            if (match.isEmpty()) {
                continue;
            }
            Optional<Boolean> holds = evaluateNumericGuards(match.orElseThrow());
            if (holds.equals(Optional.of(true))) {
                return Optional.of(Relation.SATISFIED);
            }
            needsSolver |= holds.isEmpty();
        }
        return needsSolver ? Optional.empty() : Optional.of(Relation.UNSATISFIED);
    }

    private static Optional<Set<Constraint>> matchConstraints(CoercionRule rule, VariableAllocator allocator, Expression from, Expression to)
    {
        if (rule instanceof ScalarPatternCoercion scalar) {
            return scalar.matchConstraints(allocator, from, to);
        }
        if (rule instanceof PatternCoercion pattern) {
            return pattern.matchConstraints(allocator, from, to);
        }
        return rule.matches(allocator, from, to).map(CoercionRule.Match::constraints);
    }

    private static boolean isGroundNumericType(Expression expression)
    {
        return expression instanceof Expression.Symbol ||
                (expression instanceof Expression.Application(Expression.Symbol _, List<Expression> arguments) && arguments.stream().allMatch(Expression.Literal.class::isInstance));
    }

    /// A single scalar rule can often be decided by intersecting independent numeric
    /// intervals. Keep disjunctions, aliases, and arithmetic on the full solver path.
    private Optional<Relation> classifySimpleNumericTypes(Expression left, Expression right)
    {
        if (!hasNumericParameters(left) || !hasNumericParameters(right)) {
            return Optional.empty();
        }
        VariableAllocator allocator = new VariableAllocator();
        Solver.variables(left).forEach(name -> allocator.reserveThrough(VariableAllocator.variableId(name)));
        Solver.variables(right).forEach(name -> allocator.reserveThrough(VariableAllocator.variableId(name)));
        Set<Constraint> matched = null;
        boolean conditionalRepresentationBridge = false;
        for (CoercionRule rule : typeSystem.candidateCoercions(left, right)) {
            ResolutionBudget.consume();
            Optional<Set<Constraint>> match = matchConstraints(rule, allocator, left, right);
            if (match.isPresent()) {
                if (matched != null) {
                    return Optional.empty();
                }
                matched = match.orElseThrow();
                conditionalRepresentationBridge = rule.isRepresentationBridge() && !matched.isEmpty();
            }
        }
        if (matched == null) {
            return Optional.empty();
        }
        if (conditionalRepresentationBridge) {
            return Optional.of(Relation.INCOMPLETE);
        }
        List<Constraint> constraints = new ArrayList<>(typeSystem.instantiateValidationConstraints(left));
        constraints.addAll(typeSystem.instantiateValidationConstraints(right));
        constraints.addAll(matched);
        return NumericIntervals.satisfiable(constraints)
                .map(satisfied -> satisfied ? Relation.SATISFIED : Relation.UNSATISFIED);
    }

    private boolean hasNumericParameters(Expression expression)
    {
        if (expression instanceof Expression.Symbol) {
            return true;
        }
        if (!(expression instanceof Expression.Application(Expression.Symbol(String name), List<Expression> arguments)) ||
                !arguments.stream().allMatch(argument -> argument instanceof Expression.Variable || argument instanceof Expression.Literal)) {
            return false;
        }
        Optional<TypeConstructor> constructor = typeSystem.findConstructor(name, arguments.size());
        if (constructor.isEmpty() || constructor.orElseThrow().variadic()) {
            return false;
        }
        for (int index = 0; index < arguments.size(); index++) {
            if (constructor.orElseThrow().parameterKind(index) != Kind.NUMBER) {
                return false;
            }
        }
        return true;
    }

    private static Optional<Boolean> evaluateNumericGuards(Collection<Constraint> constraints)
    {
        boolean unknown = false;
        for (Constraint constraint : constraints) {
            if (!(constraint instanceof NumericRelation(Expression.BinaryOperation operation))) {
                unknown = true;
                continue;
            }
            Expression value = Expression.evaluate(Expression.conditional(operation, Expression.literal(1), Expression.literal(0)));
            if (!(value instanceof Expression.Literal(int result))) {
                unknown = true;
            }
            else if (result == 0) {
                return Optional.of(false);
            }
        }
        return unknown ? Optional.empty() : Optional.of(true);
    }

    public enum Relation
    {
        SATISFIED,
        UNSATISFIED,
        INCOMPLETE,
    }

    private record Key(Expression left, Expression right) {}
}
