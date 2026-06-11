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
package org.weakref.solver;

import org.weakref.solver.Expression.BinaryOperation;
import org.weakref.solver.Expression.BinaryOperator;
import org.weakref.solver.Expression.Literal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Post-solve materialization — collapses the solver's variable state into concrete
 * type and numeric bindings.
 * <p>
 * The solver's work-list pass may leave unresolved variables that still have
 * non-singleton {@link Domain}s. This class picks one alternative per unresolved variable
 * by backtracking search: variables are ordered by smallest domain first (most-constrained),
 * and at each level the materializer tries alternatives in ranked order (preferring
 * more-specific witnesses when the variable has an upper bound, more-general when it
 * has a lower bound, or deterministic string ordering otherwise). Guards attached to
 * selected alternatives are accumulated and the numeric sub-system is consulted once
 * to assign concrete values to any remaining numeric variables.
 * <p>
 * An {@link UnsatisfiableException} is thrown if no consistent selection exists.
 */
final class SolverMaterializer
{
    private final TypeSystem typeSystem;
    private final Map<String, VariableState> variableBounds;
    private final List<Constraint> nextBatch;
    private final SubtypeOracle subtypeOracle;

    SolverMaterializer(TypeSystem typeSystem, Map<String, VariableState> variableBounds, List<Constraint> nextBatch)
    {
        this.typeSystem = typeSystem;
        this.variableBounds = variableBounds;
        this.nextBatch = nextBatch;
        this.subtypeOracle = new SubtypeOracle(typeSystem);
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
                .sorted(Map.Entry.comparingByValue(java.util.Comparator.comparingInt(state -> state.domain().alternatives().size())))
                .map(Map.Entry::getKey)
                .toList();

        Optional<Map<String, Alternative>> selections = chooseAlternatives(unresolvedVariables, 0, new HashMap<>(), typeStates);
        if (!unresolvedVariables.isEmpty() && selections.isEmpty()) {
            throw new UnsatisfiableException("No satisfiable witness assignment exists");
        }
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
            Alternative selected = selections.orElse(Map.of()).get(name);
            if (selected != null) {
                substitutions.put(name, selected.witness());
            }
        });

        // A variable the search left free — no binding, no selected alternative — but holding
        // ground lower bounds defaults to their least upper bound: the smallest solution. This is
        // how the engine resolves a type variable constrained only by null arguments — unknown is
        // the bottom of the binding lattice and survives only when nothing rebinds it. Defaults
        // chain (one variable's default can ground another's bounds), so iterate to a fixpoint —
        // first strictly, then tolerating bounds that are still free variables: such a bound
        // defaults the same way from its own ground bounds, so it cannot raise the LUB (a lambda's
        // fresh parameter variable mirroring T through `function(@x) <: function(T)` is the
        // canonical case — without the tolerance a null-typed container leaves T unmaterialized).
        boolean tolerateFreeVariables = false;
        boolean changed;
        do {
            changed = false;
            for (Map.Entry<String, VariableState> entry : variableBounds.entrySet()) {
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

    private Optional<Map<String, Alternative>> chooseAlternatives(List<String> variables, int index, Map<String, Alternative> selections, Map<String, TypeVariableState> typeStates)
    {
        if (index >= variables.size()) {
            return Optional.of(Map.copyOf(selections));
        }

        String variable = variables.get(index);
        TypeVariableState typeState = typeStates.get(variable);
        for (Alternative alternative : orderedAlternatives(typeState)) {
            selections.put(variable, alternative);
            try {
                materializeNumericValues(selectedConstraints(Optional.of(selections)));
                Optional<Map<String, Alternative>> result = chooseAlternatives(variables, index + 1, selections, typeStates);
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

    private int compareAlternatives(TypeVariableState typeState, Alternative left, Alternative right)
    {
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
        Set<Constraint> constraints = new LinkedHashSet<>(nextBatch);
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
        Map<String, Integer> values = new HashMap<>();
        Map<String, OptionalInt> maximums = new HashMap<>();

        variableBounds.forEach((name, state) -> {
            if (state instanceof NumericVariableState(OptionalInt min, OptionalInt max)) {
                values.put(name, min.orElse(0));
                maximums.put(name, max);
            }
        });

        boolean changed;
        int iteration = 0;
        do {
            changed = false;
            iteration++;
            if (iteration > Math.max(1, values.size() * Math.max(1, constraints.size() + 1)) * 4) {
                throw new IllegalStateException("Failed to materialize numeric values");
            }

            for (Constraint constraint : constraints) {
                if (!(constraint instanceof NumericRelation(BinaryOperation operation))) {
                    continue;
                }
                changed |= raiseLowerBounds(operation, values);
            }

            for (Map.Entry<String, Integer> entry : values.entrySet()) {
                OptionalInt max = maximums.getOrDefault(entry.getKey(), OptionalInt.empty());
                if (max.isPresent() && entry.getValue() > max.orElseThrow()) {
                    throw new UnsatisfiableException("Materialized value for " + entry.getKey() + " exceeds upper bound");
                }
            }
        }
        while (changed);

        for (Constraint constraint : constraints) {
            if (constraint instanceof NumericRelation(BinaryOperation operation)) {
                Optional<Boolean> satisfied = evaluateBoolean(substituteNumeric(operation, values));
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
            OptionalInt bound = evaluateNumericExpression(substituteNumericValues(operation.right(), values));
            OptionalInt subtracted = evaluateNumericExpression(substituteNumericValues(subtrahend, values));
            if (bound.isPresent() && subtracted.isPresent()) {
                int floor = bound.orElseThrow() + subtracted.orElseThrow() + (operation.operator() == BinaryOperator.GREATER_THAN ? 1 : 0);
                return raise(values, variable, floor);
            }
        }
        if ((operation.operator() == BinaryOperator.LESS_THAN || operation.operator() == BinaryOperator.LESS_THAN_OR_EQUAL || operation.operator() == BinaryOperator.EQUAL) &&
                operation.right() instanceof BinaryOperation(BinaryOperator differenceOperator, Expression minuend, Expression subtrahend) &&
                differenceOperator == BinaryOperator.SUBTRACT &&
                minuend instanceof Expression.Variable(String variable)) {
            OptionalInt bound = evaluateNumericExpression(substituteNumericValues(operation.left(), values));
            OptionalInt subtracted = evaluateNumericExpression(substituteNumericValues(subtrahend, values));
            if (bound.isPresent() && subtracted.isPresent()) {
                int floor = bound.orElseThrow() + subtracted.orElseThrow() + (operation.operator() == BinaryOperator.LESS_THAN ? 1 : 0);
                return raise(values, variable, floor);
            }
        }

        // General form: a bare variable bounded below by any expression that evaluates under the
        // values established so far — min(38, (p1 - s1) + s2) once the other variables settle.
        // The enclosing loop re-runs to a fixpoint and raise() is monotonic, so chained bounds
        // resolve in dependency order.
        if (operation.left() instanceof Expression.Variable(String variable) && !(operation.right() instanceof Literal)) {
            OptionalInt bound = evaluateNumericExpression(substituteNumericValues(operation.right(), values));
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

    private static Expression substituteNumericValues(Expression expression, Map<String, Integer> values)
    {
        return Expression.substitute(expression, values.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Expression.literal(entry.getValue()))));
    }

    private static BinaryOperation substituteNumeric(BinaryOperation operation, Map<String, Integer> values)
    {
        return (BinaryOperation) Expression.substitute(operation, values.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Expression.literal(entry.getValue()))));
    }

    private static Optional<Boolean> evaluateBoolean(BinaryOperation operation)
    {
        OptionalInt left = evaluateNumericExpression(operation.left());
        OptionalInt right = evaluateNumericExpression(operation.right());
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
        return switch (expression) {
            case Literal(int value) -> OptionalInt.of(value);
            case BinaryOperation(BinaryOperator operator, Expression left, Expression right) -> {
                OptionalInt leftValue = evaluateNumericExpression(left);
                OptionalInt rightValue = evaluateNumericExpression(right);
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
                Optional<Boolean> holds = evaluateBoolean(condition);
                if (holds.isEmpty()) {
                    yield OptionalInt.empty();
                }
                yield evaluateNumericExpression(holds.orElseThrow() ? ifTrue : ifFalse);
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
