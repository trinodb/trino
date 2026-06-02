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
import org.weakref.solver.Expression.Variable;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * Numeric reasoning for the solver.
 * <p>
 * Handles {@link NumericRelation} constraints: evaluates relations against current
 * bounds, propagates bounds across difference constraints, and tightens variable
 * ranges when a relation has the shape {@code variable &lt;cmp&gt; literal}.
 */
final class NumericPropagator
{
    private NumericPropagator() {}

    /**
     * Apply a numeric relation. If the relation is already determined true, return.
     * If determined false, throw. Otherwise propagate bounds and park as pending.
     */
    static void process(BinaryOperation operation, Solver.SolverState state)
    {
        if (evaluated(operation, state)) {
            return;
        }

        boolean propagated = propagate(operation, state);
        if (evaluated(operation, state)) {
            return;
        }
        if (propagated) {
            state.pendingConstraints().add(new NumericRelation(operation));
            return;
        }

        if (operation.left() instanceof Variable(String variable) && operation.right() instanceof Literal(int literal)) {
            updateNumericBounds(variable, operation.operator(), literal, true, state);
            return;
        }
        if (operation.left() instanceof Literal(int literal) && operation.right() instanceof Variable(String variable)) {
            updateNumericBounds(variable, operation.operator(), literal, false, state);
            return;
        }

        state.pendingConstraints().add(new NumericRelation(operation));
    }

    /**
     * Try to evaluate a relation against current bounds.
     *
     * @return {@link Optional#empty()} if undetermined, otherwise the truth value
     */
    static Optional<Boolean> evaluate(BinaryOperation operation, Solver.SolverState state)
    {
        Optional<Range> leftRange = evaluateRange(operation.left(), state);
        Optional<Range> rightRange = evaluateRange(operation.right(), state);
        if (leftRange.isPresent() && rightRange.isPresent()) {
            return compareRanges(operation.operator(), leftRange.orElseThrow(), rightRange.orElseThrow());
        }

        OptionalInt left = evaluateValue(operation.left(), state);
        OptionalInt right = evaluateValue(operation.right(), state);
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
            default -> throw new UnsupportedOperationException("Expected a comparison operator: " + operation.operator());
        });
    }

    /**
     * Propagate bounds across a comparison. Returns true if any bound changed.
     */
    static boolean propagate(BinaryOperation operation, Solver.SolverState state)
    {
        DifferenceConstraint constraint = normalizeComparison(operation);
        if (constraint == null) {
            return false;
        }
        return switch (constraint.operator()) {
            case GREATER_THAN -> propagateDifferenceAtLeast(constraint.leftVariable(), constraint.rightVariable(), constraint.literal() + 1, state);
            case GREATER_THAN_OR_EQUAL -> propagateDifferenceAtLeast(constraint.leftVariable(), constraint.rightVariable(), constraint.literal(), state);
            case LESS_THAN -> propagateDifferenceAtMost(constraint.leftVariable(), constraint.rightVariable(), constraint.literal() - 1, state);
            case LESS_THAN_OR_EQUAL -> propagateDifferenceAtMost(constraint.leftVariable(), constraint.rightVariable(), constraint.literal(), state);
            case EQUAL -> propagateDifferenceAtLeast(constraint.leftVariable(), constraint.rightVariable(), constraint.literal(), state)
                    | propagateDifferenceAtMost(constraint.leftVariable(), constraint.rightVariable(), constraint.literal(), state);
            default -> false;
        };
    }

    /**
     * Look up (or fail loudly) a numeric-kind variable state.
     */
    static NumericVariableState requireNumericVariable(String variable, Solver.SolverState state)
    {
        VariableState variableState = state.variableStates().get(variable);
        if (variableState == null) {
            variableState = new NumericVariableState();
            state.variableStates().put(variable, variableState);
            return (NumericVariableState) variableState;
        }
        if (!(variableState instanceof NumericVariableState numericVariableState)) {
            throw new IllegalStateException("Expected " + variable + " to be of NUMBER kind");
        }
        return numericVariableState;
    }

    private static boolean evaluated(BinaryOperation operation, Solver.SolverState state)
    {
        Optional<Boolean> satisfied = evaluate(operation, state);
        if (satisfied.isEmpty()) {
            return false;
        }
        if (!satisfied.orElseThrow()) {
            throw new UnsatisfiableException("Unsatisfiable: " + operation);
        }
        return true;
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
            OptionalInt rightConstant = evaluateConstant(operation.right());
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
            OptionalInt leftConstant = evaluateConstant(operation.left());
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
            case Variable(String variable) -> Optional.of(new LinearTerm(variable, 0));
            case BinaryOperation binary when binary.operator() == BinaryOperator.ADD &&
                    binary.left() instanceof Variable(String variable) &&
                    binary.right() instanceof Literal(int literal) -> Optional.of(new LinearTerm(variable, literal));
            case BinaryOperation binary when binary.operator() == BinaryOperator.ADD &&
                    binary.left() instanceof Literal(int literal) &&
                    binary.right() instanceof Variable(String variable) -> Optional.of(new LinearTerm(variable, literal));
            case BinaryOperation binary when binary.operator() == BinaryOperator.SUBTRACT &&
                    binary.left() instanceof Variable(String variable) &&
                    binary.right() instanceof Literal(int literal) -> Optional.of(new LinearTerm(variable, -literal));
            default -> Optional.empty();
        };
    }

    private static Optional<VariableDifference> variableDifference(Expression expression)
    {
        return switch (expression) {
            case BinaryOperation binary when binary.operator() == BinaryOperator.SUBTRACT &&
                    binary.left() instanceof Variable(String leftVariable) &&
                    binary.right() instanceof Variable(String rightVariable) -> Optional.of(new VariableDifference(leftVariable, rightVariable));
            default -> Optional.empty();
        };
    }

    private static OptionalInt evaluateConstant(Expression expression)
    {
        return switch (expression) {
            case Literal(int value) -> OptionalInt.of(value);
            case BinaryOperation(BinaryOperator operator, Expression left, Expression right) -> {
                OptionalInt leftValue = evaluateConstant(left);
                OptionalInt rightValue = evaluateConstant(right);
                if (leftValue.isEmpty() || rightValue.isEmpty()) {
                    yield OptionalInt.empty();
                }
                yield switch (operator) {
                    case ADD -> OptionalInt.of(leftValue.orElseThrow() + rightValue.orElseThrow());
                    case SUBTRACT -> OptionalInt.of(leftValue.orElseThrow() - rightValue.orElseThrow());
                    case MULTIPLY -> OptionalInt.of(leftValue.orElseThrow() * rightValue.orElseThrow());
                    case DIVIDE -> rightValue.orElseThrow() == 0 ? OptionalInt.empty() : OptionalInt.of(leftValue.orElseThrow() / rightValue.orElseThrow());
                    case MIN -> OptionalInt.of(Math.min(leftValue.orElseThrow(), rightValue.orElseThrow()));
                    case MAX -> OptionalInt.of(Math.max(leftValue.orElseThrow(), rightValue.orElseThrow()));
                    default -> OptionalInt.empty();
                };
            }
            default -> OptionalInt.empty();
        };
    }

    private static BinaryOperator flipComparison(BinaryOperator operator)
    {
        return switch (operator) {
            case LESS_THAN -> BinaryOperator.GREATER_THAN;
            case LESS_THAN_OR_EQUAL -> BinaryOperator.GREATER_THAN_OR_EQUAL;
            case GREATER_THAN -> BinaryOperator.LESS_THAN;
            case GREATER_THAN_OR_EQUAL -> BinaryOperator.LESS_THAN_OR_EQUAL;
            default -> operator;
        };
    }

    private static boolean propagateDifferenceAtLeast(String leftVariable, String rightVariable, int minimumDifference, Solver.SolverState state)
    {
        NumericVariableState leftState = requireNumericVariable(leftVariable, state);
        NumericVariableState rightState = requireNumericVariable(rightVariable, state);

        OptionalInt leftMin = leftState.min();
        OptionalInt leftMax = leftState.max();
        OptionalInt rightMin = rightState.min();
        OptionalInt rightMax = rightState.max();

        boolean changed = false;
        if (rightMin.isPresent()) {
            OptionalInt newLeftMin = tightenLower(leftMin, rightMin.orElseThrow() + minimumDifference);
            changed |= !newLeftMin.equals(leftMin);
            leftMin = newLeftMin;
        }
        if (leftMax.isPresent()) {
            OptionalInt newRightMax = tightenUpper(rightMax, leftMax.orElseThrow() - minimumDifference);
            changed |= !newRightMax.equals(rightMax);
            rightMax = newRightMax;
        }

        applyNumericState(leftVariable, leftMin, leftMax, state);
        applyNumericState(rightVariable, rightMin, rightMax, state);
        return changed;
    }

    private static boolean propagateDifferenceAtMost(String leftVariable, String rightVariable, int maximumDifference, Solver.SolverState state)
    {
        NumericVariableState leftState = requireNumericVariable(leftVariable, state);
        NumericVariableState rightState = requireNumericVariable(rightVariable, state);

        OptionalInt leftMin = leftState.min();
        OptionalInt leftMax = leftState.max();
        OptionalInt rightMin = rightState.min();
        OptionalInt rightMax = rightState.max();

        boolean changed = false;
        if (rightMax.isPresent()) {
            OptionalInt newLeftMax = tightenUpper(leftMax, rightMax.orElseThrow() + maximumDifference);
            changed |= !newLeftMax.equals(leftMax);
            leftMax = newLeftMax;
        }
        if (leftMin.isPresent()) {
            OptionalInt newRightMin = tightenLower(rightMin, leftMin.orElseThrow() - maximumDifference);
            changed |= !newRightMin.equals(rightMin);
            rightMin = newRightMin;
        }

        applyNumericState(leftVariable, leftMin, leftMax, state);
        applyNumericState(rightVariable, rightMin, rightMax, state);
        return changed;
    }

    private static void applyNumericState(String variable, OptionalInt min, OptionalInt max, Solver.SolverState state)
    {
        if (min.isPresent() && max.isPresent() && min.orElseThrow() > max.orElseThrow()) {
            throw new UnsatisfiableException("Unsatisfiable: " + variable + " has bounds [" + min.orElseThrow() + ", " + max.orElseThrow() + "]");
        }
        state.variableStates().put(variable, new NumericVariableState(min, max));
    }

    private static Optional<Range> evaluateRange(Expression expression, Solver.SolverState state)
    {
        return switch (expression) {
            case Literal(int value) -> Optional.of(new Range(value, value));
            case Variable(String name) -> {
                VariableState variableState = state.variableStates().get(name);
                if (variableState instanceof NumericVariableState(OptionalInt min, OptionalInt max) &&
                        min.isPresent() &&
                        max.isPresent()) {
                    yield Optional.of(new Range(min.orElseThrow(), max.orElseThrow()));
                }
                yield Optional.empty();
            }
            case BinaryOperation(BinaryOperator operator, Expression left, Expression right) -> {
                Optional<Range> leftRange = evaluateRange(left, state);
                Optional<Range> rightRange = evaluateRange(right, state);
                if (leftRange.isEmpty() || rightRange.isEmpty()) {
                    yield Optional.empty();
                }
                Range leftValue = leftRange.orElseThrow();
                Range rightValue = rightRange.orElseThrow();
                yield switch (operator) {
                    case ADD -> Optional.of(new Range(leftValue.min() + rightValue.min(), leftValue.max() + rightValue.max()));
                    case SUBTRACT -> Optional.of(new Range(leftValue.min() - rightValue.max(), leftValue.max() - rightValue.min()));
                    case MIN -> Optional.of(new Range(Math.min(leftValue.min(), rightValue.min()), Math.min(leftValue.max(), rightValue.max())));
                    case MAX -> Optional.of(new Range(Math.max(leftValue.min(), rightValue.min()), Math.max(leftValue.max(), rightValue.max())));
                    default -> Optional.empty();
                };
            }
            default -> Optional.empty();
        };
    }

    private static OptionalInt evaluateValue(Expression expression, Solver.SolverState state)
    {
        return switch (expression) {
            case Literal(int value) -> OptionalInt.of(value);
            case Variable(String name) -> {
                VariableState variableState = state.variableStates().get(name);
                if (variableState instanceof NumericVariableState(OptionalInt min, OptionalInt max) &&
                        min.isPresent() &&
                        max.isPresent() &&
                        min.orElseThrow() == max.orElseThrow()) {
                    yield OptionalInt.of(min.orElseThrow());
                }
                yield OptionalInt.empty();
            }
            case BinaryOperation(BinaryOperator operator, Expression left, Expression right) -> {
                OptionalInt leftValue = evaluateValue(left, state);
                OptionalInt rightValue = evaluateValue(right, state);
                if (leftValue.isEmpty() || rightValue.isEmpty()) {
                    yield OptionalInt.empty();
                }
                yield OptionalInt.of(switch (operator) {
                    case ADD -> leftValue.orElseThrow() + rightValue.orElseThrow();
                    case SUBTRACT -> leftValue.orElseThrow() - rightValue.orElseThrow();
                    case MULTIPLY -> leftValue.orElseThrow() * rightValue.orElseThrow();
                    case DIVIDE -> leftValue.orElseThrow() / rightValue.orElseThrow();
                    case MIN -> Math.min(leftValue.orElseThrow(), rightValue.orElseThrow());
                    case MAX -> Math.max(leftValue.orElseThrow(), rightValue.orElseThrow());
                    default -> throw new UnsupportedOperationException("Expected an arithmetic operator: " + operator);
                });
            }
            default -> OptionalInt.empty();
        };
    }

    private static void updateNumericBounds(String variable, BinaryOperator operator, int literal, boolean variableOnLeft, Solver.SolverState state)
    {
        NumericVariableState bounds = requireNumericVariable(variable, state);
        OptionalInt min = bounds.min();
        OptionalInt max = bounds.max();

        switch (operator) {
            case LESS_THAN -> {
                if (variableOnLeft) {
                    max = tightenUpper(max, literal - 1);
                }
                else {
                    min = tightenLower(min, literal + 1);
                }
            }
            case LESS_THAN_OR_EQUAL -> {
                if (variableOnLeft) {
                    max = tightenUpper(max, literal);
                }
                else {
                    min = tightenLower(min, literal);
                }
            }
            case GREATER_THAN -> {
                if (variableOnLeft) {
                    min = tightenLower(min, literal + 1);
                }
                else {
                    max = tightenUpper(max, literal - 1);
                }
            }
            case GREATER_THAN_OR_EQUAL -> {
                if (variableOnLeft) {
                    min = tightenLower(min, literal);
                }
                else {
                    max = tightenUpper(max, literal);
                }
            }
            case EQUAL -> {
                min = tightenLower(min, literal);
                max = tightenUpper(max, literal);
            }
            case NOT_EQUAL, ADD, SUBTRACT, MULTIPLY, DIVIDE -> {
                state.pendingConstraints().add(new NumericRelation(new BinaryOperation(
                        operator,
                        variableOnLeft ? new Variable(variable) : new Literal(literal),
                        variableOnLeft ? new Literal(literal) : new Variable(variable))));
                return;
            }
        }

        if (min.isPresent() && max.isPresent() && min.orElseThrow() > max.orElseThrow()) {
            throw new UnsatisfiableException("Unsatisfiable: " + variable + " has bounds [" + min.orElseThrow() + ", " + max.orElseThrow() + "]");
        }

        state.variableStates().put(variable, new NumericVariableState(min, max));
    }

    private static OptionalInt tightenLower(OptionalInt current, int value)
    {
        return OptionalInt.of(current.isPresent() ? Math.max(current.orElseThrow(), value) : value);
    }

    private static OptionalInt tightenUpper(OptionalInt current, int value)
    {
        return OptionalInt.of(current.isPresent() ? Math.min(current.orElseThrow(), value) : value);
    }

    private static Optional<Boolean> compareRanges(BinaryOperator operator, Range left, Range right)
    {
        return switch (operator) {
            case LESS_THAN -> {
                if (left.max() < right.min()) {
                    yield Optional.of(true);
                }
                if (left.min() >= right.max()) {
                    yield Optional.of(false);
                }
                yield Optional.empty();
            }
            case LESS_THAN_OR_EQUAL -> {
                if (left.max() <= right.min()) {
                    yield Optional.of(true);
                }
                if (left.min() > right.max()) {
                    yield Optional.of(false);
                }
                yield Optional.empty();
            }
            case GREATER_THAN -> compareRanges(BinaryOperator.LESS_THAN, right, left);
            case GREATER_THAN_OR_EQUAL -> compareRanges(BinaryOperator.LESS_THAN_OR_EQUAL, right, left);
            case EQUAL -> {
                if (left.min() == left.max() && right.min() == right.max() && left.min() == right.min()) {
                    yield Optional.of(true);
                }
                if (left.max() < right.min() || right.max() < left.min()) {
                    yield Optional.of(false);
                }
                yield Optional.empty();
            }
            case NOT_EQUAL -> {
                if (left.max() < right.min() || right.max() < left.min()) {
                    yield Optional.of(true);
                }
                if (left.min() == left.max() && right.min() == right.max() && left.min() == right.min()) {
                    yield Optional.of(false);
                }
                yield Optional.empty();
            }
            default -> Optional.empty();
        };
    }

    private record Range(int min, int max) {}

    private record DifferenceConstraint(BinaryOperator operator, String leftVariable, String rightVariable, int literal) {}

    private record LinearTerm(String variable, int offset) {}

    private record VariableDifference(String leftVariable, String rightVariable) {}
}
