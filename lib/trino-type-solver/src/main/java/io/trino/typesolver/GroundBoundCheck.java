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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/// A necessary-condition check for common ground row bounds. Constructor sets
/// overapproximate the registered rules: unknown targets and custom rule classes
/// disable rejection. Components are checked only when covariance is the sole
/// structural conversion. A successful check leaves all inference to the solver.
final class GroundBoundCheck
{
    private final TypeSystem typeSystem;
    private final Map<String, Optional<Set<String>>> targets = new HashMap<>();
    private final Map<String, Boolean> covariant = new HashMap<>();

    private GroundBoundCheck(TypeSystem typeSystem)
    {
        this.typeSystem = typeSystem;
    }

    static void checkRows(TypeSystem typeSystem, List<Constraint> constraints)
    {
        Map<String, Expression.Row> rows = null;
        GroundBoundCheck check = null;
        for (Constraint constraint : constraints) {
            ResolutionBudget.consume();
            if (!(constraint instanceof Subtype(Expression.Row row, Expression.Variable(String variable))) || !Expression.isGround(row)) {
                continue;
            }
            if (rows == null) {
                rows = new HashMap<>();
            }
            Expression.Row previous = rows.put(variable, row);
            if (previous != null) {
                if (check == null) {
                    check = new GroundBoundCheck(typeSystem);
                }
                if (!check.mayHaveCommonType(previous, row, 0)) {
                    throw new UnsatisfiableException("No common type for row bounds of " + variable);
                }
            }
        }
    }

    private boolean mayHaveCommonType(Expression left, Expression right, int depth)
    {
        ResolutionBudget.checkDepth(depth);
        if (left.equals(right)) {
            return true;
        }
        if (left instanceof Expression.Row(List<Expression.RowField> leftFields) &&
                right instanceof Expression.Row(List<Expression.RowField> rightFields) &&
                onlyCovariance(left, "row")) {
            if (leftFields.size() != rightFields.size()) {
                return false;
            }
            for (int index = 0; index < leftFields.size(); index++) {
                if (!mayHaveCommonType(leftFields.get(index).type(), rightFields.get(index).type(), depth + 1)) {
                    return false;
                }
            }
            return true;
        }
        if (left instanceof Expression.Application(Expression.Symbol(String leftName), List<Expression> leftArguments) &&
                right instanceof Expression.Application(Expression.Symbol(String rightName), List<Expression> rightArguments) &&
                leftName.equals(rightName) && onlyCovariance(left, leftName)) {
            if (leftArguments.size() != rightArguments.size()) {
                return false;
            }
            for (int index = 0; index < leftArguments.size(); index++) {
                if (!mayHaveCommonType(leftArguments.get(index), rightArguments.get(index), depth + 1)) {
                    return false;
                }
            }
            return true;
        }
        Optional<String> leftBase = base(left);
        Optional<String> rightBase = base(right);
        if (leftBase.isEmpty() || rightBase.isEmpty() || leftBase.equals(rightBase)) {
            return true;
        }
        Optional<Set<String>> leftTargets = targets.computeIfAbsent(leftBase.orElseThrow(), _ -> targetBases(left));
        Optional<Set<String>> rightTargets = targets.computeIfAbsent(rightBase.orElseThrow(), _ -> targetBases(right));
        return leftTargets.isEmpty() || rightTargets.isEmpty() || leftTargets.orElseThrow().stream().anyMatch(rightTargets.orElseThrow()::contains);
    }

    private boolean onlyCovariance(Expression type, String name)
    {
        return covariant.computeIfAbsent(name, _ -> {
            boolean structural = type instanceof Expression.Row;
            for (CoercionRule rule : typeSystem.candidateCoercions(type, Expression.variable("target"))) {
                ResolutionBudget.consume();
                if (rule instanceof ParametricTypeCovariantCoercion) {
                    structural = true;
                }
                else if (rule.getClass() != SelfCoercion.class) {
                    return false;
                }
            }
            return structural;
        });
    }

    private Optional<Set<String>> targetBases(Expression type)
    {
        Set<String> result = new HashSet<>();
        base(type).ifPresent(result::add);
        for (CoercionRule rule : typeSystem.candidateCoercions(type, Expression.variable("target"))) {
            ResolutionBudget.consume();
            Optional<String> target = switch (rule) {
                case SelfCoercion self when self.getClass() == SelfCoercion.class -> base(type);
                case PrimitiveTypeCoercion primitive -> Optional.of(primitive.toType());
                case ParametricTypeCovariantCoercion structural -> Optional.of(structural.type());
                case PatternCoercion pattern -> base(pattern.toPattern());
                case ScalarPatternCoercion scalar -> base(scalar.original().toPattern());
                default -> Optional.empty();
            };
            if (target.isEmpty()) {
                return Optional.empty();
            }
            result.add(target.orElseThrow());
        }
        return Optional.of(Set.copyOf(result));
    }

    private static Optional<String> base(Expression expression)
    {
        return switch (expression) {
            case Expression.Symbol(String name) -> Optional.of(name);
            case Expression.Application(Expression.Symbol(String name), _) -> Optional.of(name);
            case Expression.Row _ -> Optional.of("row");
            default -> Optional.empty();
        };
    }
}
