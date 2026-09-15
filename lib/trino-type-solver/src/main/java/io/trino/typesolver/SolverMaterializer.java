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

import io.trino.typesolver.Expression.BinaryOperation;
import io.trino.typesolver.Expression.BinaryOperator;
import io.trino.typesolver.Expression.Literal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/// Post-solve materialization — collapses the solver's variable state into concrete
/// type and numeric bindings.
///
/// The solver's work-list pass may leave unresolved variables that still have
/// non-singleton [Domain]s. This class picks one alternative per unresolved variable
/// by backtracking search: variables are ordered by smallest domain first (most-constrained),
/// and at each level the materializer tries alternatives in ranked order (preferring
/// more-specific witnesses when the variable has an upper bound, more-general when it
/// has a lower bound, or deterministic string ordering otherwise). Guards attached to
/// selected alternatives are accumulated and the numeric sub-system is consulted once
/// to assign concrete values to any remaining numeric variables.
///
/// An [UnsatisfiableException] is thrown if no consistent selection exists.
final class SolverMaterializer
{
    private final TypeSystem typeSystem;
    private final Map<String, VariableState> variableBounds;
    private final List<Constraint> nextBatch;
    private final SubtypeOracle subtypeOracle;

    SolverMaterializer(TypeSystem typeSystem, Map<String, VariableState> variableBounds, List<Constraint> nextBatch, SubtypeOracle subtypeOracle)
    {
        this.typeSystem = typeSystem;
        this.variableBounds = variableBounds;
        this.nextBatch = nextBatch;
        this.subtypeOracle = subtypeOracle;
    }

    Materialization materialize()
    {
        Map<String, TypeVariableState> typeStates = new HashMap<>();
        variableBounds.forEach((name, state) -> {
            if (state instanceof TypeVariableState typeState) {
                typeStates.put(name, typeState);
            }
        });

        // Variables without a binding but with a restricted domain are the ones we still need
        // to pick. Order smallest-domain-first so the backtracking search fails fast on
        // infeasible corners.
        List<String> unresolvedVariables = typeStates.entrySet().stream()
                .filter(entry -> entry.getValue().binding().isEmpty())
                .filter(entry -> entry.getValue().domain().isRestricted())
                .filter(entry -> !entry.getValue().domain().alternatives().isEmpty())
                .sorted(Map.Entry.comparingByValue(Comparator.comparingInt(state -> state.domain().alternatives().size())))
                .map(Map.Entry::getKey)
                .toList();

        Map<String, Alternative> selected = new HashMap<>();
        for (SearchComponent component : searchComponents(unresolvedVariables, typeStates)) {
            Map<String, Alternative> selection = chooseAlternatives(component, 0, new HashMap<>(), typeStates, new HashSet<>())
                    .orElseThrow(() -> new UnsatisfiableException("No satisfiable witness assignment exists"));
            selected.putAll(selection);
        }
        Optional<Map<String, Alternative>> selections = Optional.of(Map.copyOf(selected));
        Map<String, Integer> numericValues = materializeNumericValues(selectedConstraints(selections));
        Map<String, Expression> substitutions = new HashMap<>();
        numericValues.forEach((name, value) -> substitutions.put(name, Expression.literal(value)));
        variableBounds.forEach((name, state) -> {
            if (!(state instanceof TypeVariableState typeState)) {
                return;
            }
            if (typeState.binding().isPresent()) {
                substitutions.put(name, typeState.binding().orElseThrow());
                return;
            }
            Alternative alternative = selections.orElse(Map.of()).get(name);
            if (alternative != null) {
                substitutions.put(name, alternative.witness());
            }
        });

        // A variable the search left free — no binding, no selected alternative — but holding
        // ground lower bounds defaults to their least upper bound: the smallest solution. This is
        // how the engine resolves a type variable constrained only by null arguments — unknown is
        // the bottom of the binding lattice and survives only when nothing rebinds it. Defaults
        // chain (one variable's default can ground another's bounds), so iterate to a fixpoint —
        // first strictly, then tolerating bounds that are still free variables: such a bound
        // defaults the same way from its own ground bounds, so it cannot raise the LUB (a lambda's
        // fresh parameter variable mirroring T through `function(x) <: function(T)` is the
        // canonical case — without the tolerance a null-typed container leaves T unmaterialized).
        boolean tolerateFreeVariables = false;
        boolean changed;
        do {
            changed = false;
            for (Map.Entry<String, VariableState> entry : variableBounds.entrySet()) {
                ResolutionBudget.consume();
                if (!(entry.getValue() instanceof TypeVariableState typeState) || substitutions.containsKey(entry.getKey())) {
                    continue;
                }
                if (defaultFromLowerBounds(entry.getKey(), typeState, substitutions, tolerateFreeVariables)) {
                    changed = true;
                }
            }
            if (!changed && !tolerateFreeVariables) {
                tolerateFreeVariables = true;
                changed = true;
            }
        }
        while (changed);

        Map<String, Expression> result = new HashMap<>();
        variableBounds.forEach((name, state) -> {
            if (!(state instanceof TypeVariableState typeState)) {
                return;
            }
            Expression expression = substitutions.get(name);
            if (expression != null) {
                result.put(name, mergeRowFieldNamesWithBounds(Expression.substitute(expression, substitutions), typeState, substitutions));
            }
        });
        Map<String, Alternative> materializedSelections = selections.orElse(Map.of()).entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().apply(substitutions)));
        return new Materialization(Map.copyOf(numericValues), Map.copyOf(result), Map.copyOf(materializedSelections));
    }

    private boolean defaultFromLowerBounds(String name, TypeVariableState typeState, Map<String, Expression> substitutions, boolean tolerateFreeVariables)
    {
        return typeState.lowerBounds().map(bounds -> {
            List<Expression> substituted = bounds.stream()
                    .map(bound -> Expression.substitute(bound, substitutions))
                    .toList();
            List<Expression> ground = substituted.stream()
                    .filter(Expression::isGround)
                    .distinct()
                    .toList();
            if (ground.isEmpty()) {
                return false;
            }
            boolean decidable = substituted.stream()
                    .allMatch(bound -> Expression.isGround(bound) || (tolerateFreeVariables && bound instanceof Expression.Variable));
            if (!decidable) {
                return false;
            }
            return ground.stream()
                    .filter(candidate -> ground.stream().allMatch(other -> subtypeOracle.isSubtype(other, candidate)))
                    .findFirst()
                    .map(top -> {
                        substitutions.put(name, top);
                        return true;
                    })
                    .orElse(false);
        }).orElse(false);
    }

    /// Merge a materialized type's row field names against the variable's ground lower bounds: a
    /// name survives only where the binding and every input agree, the way the engine computes
    /// row supertypes. This is applied to the final binding regardless of how it was reached — a
    /// forced symbolic witness binds with its source row's names before a competing anonymous
    /// bound can intersect it, so the merge must happen here rather than only during domain
    /// intersection. Rows that differ only in field names are mutual subtypes, so this never
    /// changes which type is chosen, only its names.
    private static Expression mergeRowFieldNamesWithBounds(Expression resolved, TypeVariableState typeState, Map<String, Expression> substitutions)
    {
        return typeState.lowerBounds().map(bounds -> {
            Expression merged = resolved;
            for (Expression bound : bounds) {
                Expression ground = Expression.substitute(bound, substitutions);
                if (Expression.isGround(ground)) {
                    merged = Expression.mergeRowFieldNames(merged, ground);
                }
            }
            return merged;
        }).orElse(resolved);
    }

    private List<SearchComponent> searchComponents(List<String> variables, Map<String, TypeVariableState> typeStates)
    {
        Map<String, String> parents = new HashMap<>();
        Map<String, Expression> bindings = new HashMap<>();
        typeStates.forEach((name, state) -> state.binding().ifPresent(binding -> bindings.put(name, binding)));
        for (String variable : variables) {
            Set<String> dependencies = new HashSet<>();
            dependencies.add(variable);
            TypeVariableState state = typeStates.get(variable);
            for (Alternative alternative : state.domain().alternatives()) {
                dependencies.addAll(Solver.variables(Expression.substitute(alternative.witness(), bindings)));
                alternative.guards().forEach(guard -> dependencies.addAll(Solver.variables(guard.apply(bindings))));
            }
            connect(parents, dependencies);
        }
        Map<Constraint, Set<String>> constraintVariables = new LinkedHashMap<>();
        for (Constraint constraint : nextBatch) {
            // Search checks numeric feasibility. Structural guards must already be
            // ground to be selectable; traits and other pending obligations are checked
            // on the complete materialization. A bound row that merely assembles fields
            // therefore does not couple their independent numeric choices.
            if (!(constraint instanceof NumericRelation)) {
                continue;
            }
            Set<String> dependencies = Solver.variables(constraint);
            constraintVariables.put(constraint, dependencies);
            connect(parents, dependencies);
        }
        Map<String, List<String>> groups = new LinkedHashMap<>();
        for (String variable : variables) {
            groups.computeIfAbsent(root(parents, variable), _ -> new ArrayList<>()).add(variable);
        }
        Map<String, Set<String>> members = new HashMap<>();
        for (String variable : parents.keySet()) {
            members.computeIfAbsent(root(parents, variable), _ -> new HashSet<>()).add(variable);
        }
        Map<String, List<Constraint>> groupedConstraints = new HashMap<>();
        List<Constraint> groundConstraints = new ArrayList<>();
        constraintVariables.forEach((constraint, dependencies) -> {
            if (dependencies.isEmpty()) {
                groundConstraints.add(constraint);
            }
            else {
                String root = root(parents, dependencies.iterator().next());
                groupedConstraints.computeIfAbsent(root, _ -> new ArrayList<>()).add(constraint);
            }
        });
        List<SearchComponent> result = new ArrayList<>();
        groups.forEach((root, choices) -> {
            Set<String> scope = members.get(root);
            List<Constraint> constraints = new ArrayList<>(groundConstraints);
            constraints.addAll(groupedConstraints.getOrDefault(root, List.of()));
            result.add(new SearchComponent(choices, scope, constraints));
        });
        return result;
    }

    private static void connect(Map<String, String> parents, Set<String> variables)
    {
        String representative = null;
        for (String variable : variables) {
            String root = root(parents, variable);
            if (representative == null) {
                representative = root;
            }
            else {
                parents.put(root, representative);
            }
        }
    }

    private static String root(Map<String, String> parents, String variable)
    {
        parents.putIfAbsent(variable, variable);
        String representative = variable;
        while (!parents.get(representative).equals(representative)) {
            ResolutionBudget.consume();
            representative = parents.get(representative);
        }
        while (!variable.equals(representative)) {
            String parent = parents.put(variable, representative);
            variable = parent;
        }
        return representative;
    }

    private record SearchComponent(List<String> choices, Set<String> variables, List<Constraint> constraints) {}

    private Optional<Map<String, Alternative>> chooseAlternatives(SearchComponent component, int index, Map<String, Alternative> selections, Map<String, TypeVariableState> typeStates, Set<Set<Constraint>> failedGuards)
    {
        return ResolutionBudget.nested(() -> chooseAlternativesInternal(component, index, selections, typeStates, failedGuards));
    }

    private Optional<Map<String, Alternative>> chooseAlternativesInternal(SearchComponent component, int index, Map<String, Alternative> selections, Map<String, TypeVariableState> typeStates, Set<Set<Constraint>> failedGuards)
    {
        if (index >= component.choices().size()) {
            return Optional.of(Map.copyOf(selections));
        }

        String variable = component.choices().get(index);
        TypeVariableState typeState = typeStates.get(variable);
        for (Alternative alternative : orderedAlternatives(typeState)) {
            ResolutionBudget.consume();
            if (!subtypeGuardsFeasible(alternative)) {
                continue;
            }
            selections.put(variable, alternative);
            List<Constraint> constraints = selectedConstraints(Optional.of(selections), component.constraints());
            Set<Constraint> guards = Set.copyOf(constraints);
            try {
                if (failedGuards.contains(guards)) {
                    selections.remove(variable);
                    continue;
                }
                materializeNumericValues(constraints, Optional.of(component.variables()));
            }
            catch (UnsatisfiableException _) {
                failedGuards.add(guards);
                selections.remove(variable);
                continue;
            }
            try {
                Optional<Map<String, Alternative>> result = chooseAlternatives(component, index + 1, selections, typeStates, failedGuards);
                if (result.isPresent()) {
                    return result;
                }
            }
            catch (UnsatisfiableException ignored) {
            }
            selections.remove(variable);
        }
        return Optional.empty();
    }

    private List<Alternative> orderedAlternatives(TypeVariableState typeState)
    {
        List<Alternative> alternatives = new ArrayList<>(typeState.domain().alternatives());
        alternatives.sort((left, right) -> compareAlternatives(typeState, left, right));
        return alternatives;
    }

    /// Whether the alternative's component obligations allow committing to it. A symbolic
    /// covariant witness carries per-component [Subtype] guards: while they still mention
    /// fresh variables the alternative is only a shape, not a selectable value — those variables
    /// have no resolution path after solving — and once domain intersection grounds them, the
    /// subtype either holds or refutes the alternative outright.
    private boolean subtypeGuardsFeasible(Alternative alternative)
    {
        for (Constraint guard : alternative.guards()) {
            if (guard instanceof Subtype(Expression left, Expression right)) {
                if (!Expression.isGround(left) || !Expression.isGround(right)) {
                    return false;
                }
                if (subtypeOracle.classify(left, right) == SubtypeOracle.Relation.UNSATISFIED) {
                    return false;
                }
            }
        }
        return true;
    }

    private int compareAlternatives(TypeVariableState typeState, Alternative left, Alternative right)
    {
        // A committable alternative (all subtype obligations ground) always ranks ahead of a
        // symbolic shape whose components are still open
        boolean leftCommittable = subtypeGuardsFeasible(left);
        boolean rightCommittable = subtypeGuardsFeasible(right);
        if (leftCommittable != rightCommittable) {
            return leftCommittable ? -1 : 1;
        }

        Preference preference = preference(typeState);
        if (preference != Preference.NONE) {
            boolean leftSubtypeRight = subtypeOracle.isSubtype(left.witness(), right.witness());
            boolean rightSubtypeLeft = subtypeOracle.isSubtype(right.witness(), left.witness());
            if (leftSubtypeRight != rightSubtypeLeft) {
                if (preference == Preference.MINIMAL) {
                    return leftSubtypeRight ? -1 : 1;
                }
                return rightSubtypeLeft ? -1 : 1;
            }
        }

        int guardComparison = Integer.compare(left.guards().size(), right.guards().size());
        if (guardComparison != 0) {
            return guardComparison;
        }

        int witnessComparison = left.witness().toString().compareTo(right.witness().toString());
        if (witnessComparison != 0) {
            return witnessComparison;
        }
        return Integer.compare(left.guards().hashCode(), right.guards().hashCode());
    }

    private Preference preference(TypeVariableState typeState)
    {
        boolean hasLowerBounds = hasMeaningfulBounds(typeState.lowerBounds());
        boolean hasUpperBounds = hasMeaningfulBounds(typeState.upperBounds());
        if (hasLowerBounds && !hasUpperBounds) {
            return Preference.MINIMAL;
        }
        if (!hasLowerBounds && hasUpperBounds) {
            return Preference.MAXIMAL;
        }
        return Preference.NONE;
    }

    private static boolean hasMeaningfulBounds(Optional<Set<Expression>> bounds)
    {
        return bounds.stream()
                .flatMap(Set::stream)
                .anyMatch(SolverMaterializer::isMeaningfulBound);
    }

    private static boolean isMeaningfulBound(Expression expression)
    {
        return !(expression instanceof Expression.Variable) &&
                !(expression instanceof Expression.AnyRow);
    }

    private List<Constraint> selectedConstraints(Optional<Map<String, Alternative>> selections)
    {
        return selectedConstraints(selections, nextBatch);
    }

    private List<Constraint> selectedConstraints(Optional<Map<String, Alternative>> selections, List<Constraint> base)
    {
        Set<Constraint> constraints = new LinkedHashSet<>(base);
        selections.orElse(Map.of()).values().forEach(alternative -> {
            constraints.addAll(alternative.guards());
            typeSystem.instantiateValidationConstraints(alternative.witness()).stream()
                    .filter(NumericRelation.class::isInstance)
                    .forEach(constraints::add);
        });
        return List.copyOf(constraints);
    }

    Map<String, Integer> materializeNumericValues(List<Constraint> constraints)
    {
        return materializeNumericValues(constraints, Optional.empty());
    }

    private Map<String, Integer> materializeNumericValues(List<Constraint> constraints, Optional<Set<String>> scope)
    {
        Map<String, Integer> values = new HashMap<>();
        Map<String, OptionalInt> maximums = new HashMap<>();

        for (String name : scope.orElseGet(variableBounds::keySet)) {
            ResolutionBudget.consume();
            VariableState state = variableBounds.get(name);
            if (state instanceof NumericVariableState(OptionalInt min, OptionalInt max)) {
                int value = min.orElse(0);
                if (max.isPresent() && value > max.orElseThrow()) {
                    throw new UnsatisfiableException("Materialized value for " + name + " exceeds upper bound");
                }
                values.put(name, value);
                maximums.put(name, max);
            }
        }

        Map<BinaryOperation, Set<String>> dependencies = new LinkedHashMap<>();
        Map<String, Set<BinaryOperation>> dependents = new HashMap<>();
        for (Constraint constraint : constraints) {
            if (constraint instanceof NumericRelation(BinaryOperation operation)) {
                Set<String> variables = Solver.variables(operation);
                dependencies.put(operation, variables);
                for (String variable : variables) {
                    dependents.computeIfAbsent(variable, _ -> new LinkedHashSet<>()).add(operation);
                }
            }
        }
        ArrayDeque<BinaryOperation> queue = new ArrayDeque<>(dependencies.keySet());
        Set<BinaryOperation> queued = new HashSet<>(dependencies.keySet());
        while (!queue.isEmpty()) {
            ResolutionBudget.consume();
            BinaryOperation operation = queue.removeFirst();
            queued.remove(operation);
            if (!raiseLowerBounds(operation, values)) {
                continue;
            }
            for (String variable : dependencies.get(operation)) {
                Integer value = values.get(variable);
                OptionalInt max = maximums.getOrDefault(variable, OptionalInt.empty());
                if (value != null && max.isPresent() && value > max.orElseThrow()) {
                    throw new UnsatisfiableException("Materialized value for " + variable + " exceeds upper bound");
                }
                for (BinaryOperation dependent : dependents.getOrDefault(variable, Set.of())) {
                    if (queued.add(dependent)) {
                        queue.addLast(dependent);
                    }
                }
            }
        }

        for (Constraint constraint : constraints) {
            if (constraint instanceof NumericRelation(BinaryOperation operation)) {
                Optional<Boolean> satisfied = evaluateBoolean(operation, values);
                if (satisfied.isEmpty() || !satisfied.orElseThrow()) {
                    throw new UnsatisfiableException("Failed to materialize numeric constraint: " + operation);
                }
            }
        }
        return Map.copyOf(values);
    }

    private static boolean raiseLowerBounds(BinaryOperation operation, Map<String, Integer> values)
    {
        if (operation.left() instanceof BinaryOperation sum &&
                sum.operator() == BinaryOperator.ADD &&
                sum.left() instanceof Expression.Variable(String variable) &&
                sum.right() instanceof Literal(int offset) &&
                operation.right() instanceof Literal(int literal)) {
            return switch (operation.operator()) {
                case BinaryOperator.GREATER_THAN -> raise(values, variable, literal - offset + 1);
                case BinaryOperator.GREATER_THAN_OR_EQUAL, BinaryOperator.EQUAL -> raise(values, variable, literal - offset);
                default -> false;
            };
        }
        if (operation.left() instanceof Literal(int literal) &&
                operation.right() instanceof BinaryOperation sum &&
                sum.operator() == BinaryOperator.ADD &&
                sum.left() instanceof Expression.Variable(String variable) &&
                sum.right() instanceof Literal(int offset)) {
            return switch (operation.operator()) {
                case BinaryOperator.LESS_THAN -> raise(values, variable, literal - offset + 1);
                case BinaryOperator.LESS_THAN_OR_EQUAL, BinaryOperator.EQUAL -> raise(values, variable, literal - offset);
                default -> false;
            };
        }
        if (operation.left() instanceof BinaryOperation difference &&
                difference.operator() == BinaryOperator.SUBTRACT &&
                difference.left() instanceof Expression.Variable(String variable) &&
                difference.right() instanceof Literal(int offset) &&
                operation.right() instanceof Literal(int literal)) {
            return switch (operation.operator()) {
                case BinaryOperator.GREATER_THAN -> raise(values, variable, literal + offset + 1);
                case BinaryOperator.GREATER_THAN_OR_EQUAL, BinaryOperator.EQUAL -> raise(values, variable, literal + offset);
                default -> false;
            };
        }
        if (operation.left() instanceof Literal(int literal) &&
                operation.right() instanceof BinaryOperation difference &&
                difference.operator() == BinaryOperator.SUBTRACT &&
                difference.left() instanceof Expression.Variable(String variable) &&
                difference.right() instanceof Literal(int offset)) {
            return switch (operation.operator()) {
                case BinaryOperator.LESS_THAN -> raise(values, variable, literal + offset + 1);
                case BinaryOperator.LESS_THAN_OR_EQUAL, BinaryOperator.EQUAL -> raise(values, variable, literal + offset);
                default -> false;
            };
        }

        // General difference form: a variable minus an evaluable subtrahend bounded below by an
        // evaluable expression — (p - s) >= 8, or its mirrored (8 <= p - s) — raises the variable
        // to bound + subtrahend once the rest settles
        if ((operation.operator() == BinaryOperator.GREATER_THAN || operation.operator() == BinaryOperator.GREATER_THAN_OR_EQUAL || operation.operator() == BinaryOperator.EQUAL) &&
                operation.left() instanceof BinaryOperation(BinaryOperator differenceOperator, Expression minuend, Expression subtrahend) &&
                differenceOperator == BinaryOperator.SUBTRACT &&
                minuend instanceof Expression.Variable(String variable)) {
            OptionalInt bound = evaluateNumericExpression(operation.right(), values);
            OptionalInt subtracted = evaluateNumericExpression(subtrahend, values);
            if (bound.isPresent() && subtracted.isPresent()) {
                int floor = bound.orElseThrow() + subtracted.orElseThrow() + (operation.operator() == BinaryOperator.GREATER_THAN ? 1 : 0);
                return raise(values, variable, floor);
            }
        }
        if ((operation.operator() == BinaryOperator.LESS_THAN || operation.operator() == BinaryOperator.LESS_THAN_OR_EQUAL || operation.operator() == BinaryOperator.EQUAL) &&
                operation.right() instanceof BinaryOperation(BinaryOperator differenceOperator, Expression minuend, Expression subtrahend) &&
                differenceOperator == BinaryOperator.SUBTRACT &&
                minuend instanceof Expression.Variable(String variable)) {
            OptionalInt bound = evaluateNumericExpression(operation.left(), values);
            OptionalInt subtracted = evaluateNumericExpression(subtrahend, values);
            if (bound.isPresent() && subtracted.isPresent()) {
                int floor = bound.orElseThrow() + subtracted.orElseThrow() + (operation.operator() == BinaryOperator.LESS_THAN ? 1 : 0);
                return raise(values, variable, floor);
            }
        }

        // General form: a bare variable bounded below by any expression that evaluates under the
        // values established so far — min(38, (p1 - s1) + s2) once the other variables settle.
        // The enclosing loop re-runs to a fixpoint and raise() is monotonic, so chained bounds
        // resolve in dependency order.
        if (operation.left() instanceof Expression.Variable(String variable) && !(operation.right() instanceof Literal) && normalizeComparison(operation) == null) {
            OptionalInt bound = evaluateNumericExpression(operation.right(), values);
            if (bound.isPresent()) {
                return switch (operation.operator()) {
                    case BinaryOperator.GREATER_THAN -> raise(values, variable, bound.orElseThrow() + 1);
                    case BinaryOperator.GREATER_THAN_OR_EQUAL, BinaryOperator.EQUAL -> raise(values, variable, bound.orElseThrow());
                    default -> false;
                };
            }
        }

        if (operation.left() instanceof Expression.Variable(String variable) && operation.right() instanceof Literal(int literal)) {
            return switch (operation.operator()) {
                case BinaryOperator.GREATER_THAN -> raise(values, variable, literal + 1);
                case BinaryOperator.GREATER_THAN_OR_EQUAL, BinaryOperator.EQUAL -> raise(values, variable, literal);
                default -> false;
            };
        }
        if (operation.left() instanceof Literal(int literal) && operation.right() instanceof Expression.Variable(String variable)) {
            return switch (operation.operator()) {
                case BinaryOperator.LESS_THAN -> raise(values, variable, literal + 1);
                case BinaryOperator.LESS_THAN_OR_EQUAL, BinaryOperator.EQUAL -> raise(values, variable, literal);
                default -> false;
            };
        }

        DifferenceConstraint difference = normalizeComparison(operation);
        if (difference == null) {
            return false;
        }
        return switch (difference.operator()) {
            case GREATER_THAN -> raise(values, difference.leftVariable(), values.getOrDefault(difference.rightVariable(), 0) + difference.literal() + 1);
            case GREATER_THAN_OR_EQUAL -> raise(values, difference.leftVariable(), values.getOrDefault(difference.rightVariable(), 0) + difference.literal());
            case LESS_THAN -> raise(values, difference.rightVariable(), values.getOrDefault(difference.leftVariable(), 0) - difference.literal() + 1);
            case LESS_THAN_OR_EQUAL -> raise(values, difference.rightVariable(), values.getOrDefault(difference.leftVariable(), 0) - difference.literal());
            case EQUAL -> {
                boolean leftRaised = raise(values, difference.leftVariable(), values.getOrDefault(difference.rightVariable(), 0) + difference.literal());
                boolean rightRaised = raise(values, difference.rightVariable(), values.getOrDefault(difference.leftVariable(), 0) - difference.literal());
                yield leftRaised || rightRaised;
            }
            default -> false;
        };
    }

    private static boolean raise(Map<String, Integer> values, String variable, int candidate)
    {
        Integer current = values.get(variable);
        if (current == null) {
            values.put(variable, candidate);
            return true;
        }
        if (candidate > current) {
            values.put(variable, candidate);
            return true;
        }
        return false;
    }

    private static Optional<Boolean> evaluateBoolean(BinaryOperation operation)
    {
        return evaluateBoolean(operation, Map.of());
    }

    private static Optional<Boolean> evaluateBoolean(BinaryOperation operation, Map<String, Integer> values)
    {
        OptionalInt left = evaluateNumericExpression(operation.left(), values);
        OptionalInt right = evaluateNumericExpression(operation.right(), values);
        if (left.isEmpty() || right.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(switch (operation.operator()) {
            case LESS_THAN -> left.orElseThrow() < right.orElseThrow();
            case LESS_THAN_OR_EQUAL -> left.orElseThrow() <= right.orElseThrow();
            case GREATER_THAN -> left.orElseThrow() > right.orElseThrow();
            case GREATER_THAN_OR_EQUAL -> left.orElseThrow() >= right.orElseThrow();
            case EQUAL -> left.orElseThrow() == right.orElseThrow();
            case NOT_EQUAL -> left.orElseThrow() != right.orElseThrow();
            default -> throw new UnsupportedOperationException("Expected comparison operator");
        });
    }

    private static OptionalInt evaluateNumericExpression(Expression expression)
    {
        return evaluateNumericExpression(expression, Map.of());
    }

    private static OptionalInt evaluateNumericExpression(Expression expression, Map<String, Integer> values)
    {
        ResolutionBudget.consume();
        return switch (expression) {
            case Literal(int value) -> OptionalInt.of(value);
            case Expression.Variable(String name) -> values.containsKey(name) ? OptionalInt.of(values.get(name)) : OptionalInt.empty();
            case BinaryOperation(BinaryOperator operator, Expression left, Expression right) -> {
                OptionalInt leftValue = evaluateNumericExpression(left, values);
                OptionalInt rightValue = evaluateNumericExpression(right, values);
                if (leftValue.isEmpty() || rightValue.isEmpty()) {
                    yield OptionalInt.empty();
                }
                // Saturating like Expression.evaluate: calculated varchar lengths overflow 32 bits
                // by design (growth formulas clamp with min(2147483647, ...)) and must not wrap
                yield OptionalInt.of(switch (operator) {
                    case ADD -> Expression.saturateToInt((long) leftValue.orElseThrow() + rightValue.orElseThrow());
                    case SUBTRACT -> Expression.saturateToInt((long) leftValue.orElseThrow() - rightValue.orElseThrow());
                    case MULTIPLY -> Expression.saturateToInt((long) leftValue.orElseThrow() * rightValue.orElseThrow());
                    case DIVIDE -> leftValue.orElseThrow() / rightValue.orElseThrow();
                    case MIN -> Math.min(leftValue.orElseThrow(), rightValue.orElseThrow());
                    case MAX -> Math.max(leftValue.orElseThrow(), rightValue.orElseThrow());
                    default -> throw new UnsupportedOperationException("Expected arithmetic operator");
                });
            }
            case Expression.Conditional(BinaryOperation condition, Expression ifTrue, Expression ifFalse) -> {
                Optional<Boolean> holds = evaluateBoolean(condition, values);
                if (holds.isEmpty()) {
                    yield OptionalInt.empty();
                }
                yield evaluateNumericExpression(holds.orElseThrow() ? ifTrue : ifFalse, values);
            }
            default -> OptionalInt.empty();
        };
    }

    private static DifferenceConstraint normalizeComparison(BinaryOperation operation)
    {
        Optional<LinearTerm> left = linearTerm(operation.left());
        Optional<LinearTerm> right = linearTerm(operation.right());
        if (left.isPresent() && right.isPresent()) {
            LinearTerm leftTerm = left.orElseThrow();
            LinearTerm rightTerm = right.orElseThrow();
            return new DifferenceConstraint(
                    operation.operator(),
                    leftTerm.variable(),
                    rightTerm.variable(),
                    rightTerm.offset() - leftTerm.offset());
        }
        Optional<VariableDifference> leftDifference = variableDifference(operation.left());
        if (leftDifference.isPresent()) {
            OptionalInt rightConstant = evaluateNumericExpression(operation.right());
            if (rightConstant.isPresent()) {
                VariableDifference difference = leftDifference.orElseThrow();
                return new DifferenceConstraint(
                        operation.operator(),
                        difference.leftVariable(),
                        difference.rightVariable(),
                        rightConstant.orElseThrow());
            }
        }
        Optional<VariableDifference> rightDifference = variableDifference(operation.right());
        if (rightDifference.isPresent()) {
            OptionalInt leftConstant = evaluateNumericExpression(operation.left());
            if (leftConstant.isPresent()) {
                VariableDifference difference = rightDifference.orElseThrow();
                return new DifferenceConstraint(
                        flipComparison(operation.operator()),
                        difference.leftVariable(),
                        difference.rightVariable(),
                        leftConstant.orElseThrow());
            }
        }
        return null;
    }

    private static Optional<LinearTerm> linearTerm(Expression expression)
    {
        return switch (expression) {
            case Expression.Variable(String variable) -> Optional.of(new LinearTerm(variable, 0));
            case BinaryOperation binary when binary.operator() == BinaryOperator.ADD &&
                    binary.left() instanceof Expression.Variable(String variable) &&
                    binary.right() instanceof Literal(int literal) -> Optional.of(new LinearTerm(variable, literal));
            case BinaryOperation binary when binary.operator() == BinaryOperator.ADD &&
                    binary.left() instanceof Literal(int literal) &&
                    binary.right() instanceof Expression.Variable(String variable) -> Optional.of(new LinearTerm(variable, literal));
            case BinaryOperation binary when binary.operator() == BinaryOperator.SUBTRACT &&
                    binary.left() instanceof Expression.Variable(String variable) &&
                    binary.right() instanceof Literal(int literal) -> Optional.of(new LinearTerm(variable, -literal));
            default -> Optional.empty();
        };
    }

    private static Optional<VariableDifference> variableDifference(Expression expression)
    {
        return switch (expression) {
            case BinaryOperation binary when binary.operator() == BinaryOperator.SUBTRACT &&
                    binary.left() instanceof Expression.Variable(String leftVariable) &&
                    binary.right() instanceof Expression.Variable(String rightVariable) -> Optional.of(new VariableDifference(leftVariable, rightVariable));
            default -> Optional.empty();
        };
    }

    private static BinaryOperator flipComparison(BinaryOperator operator)
    {
        return switch (operator) {
            case LESS_THAN -> BinaryOperator.GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> BinaryOperator.GREATER_THAN_OR_EQUAL;
            case GREATER_THAN -> BinaryOperator.LESS_THAN;
            case GREATER_THAN_OR_EQUAL -> BinaryOperator.LESS_THAN_OR_EQUAL;
            case EQUAL -> BinaryOperator.EQUAL;
            case NOT_EQUAL -> BinaryOperator.NOT_EQUAL;
            default -> throw new UnsupportedOperationException("Expected comparison operator");
        };
    }

    record Materialization(Map<String, Integer> numericValues, Map<String, Expression> typeValues, Map<String, Alternative> selectedAlternatives) {}

    private enum Preference
    {
        MINIMAL,
        MAXIMAL,
        NONE,
    }

    private record DifferenceConstraint(BinaryOperator operator, String leftVariable, String rightVariable, int literal) {}

    private record LinearTerm(String variable, int offset) {}

    private record VariableDifference(String leftVariable, String rightVariable) {}
}
