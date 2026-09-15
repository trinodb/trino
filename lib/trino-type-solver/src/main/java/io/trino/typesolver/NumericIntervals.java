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
import io.trino.typesolver.Expression.Variable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/// Decides conjunctions of independent inclusive integer bounds. Unsupported
/// relations defer to the full solver, including strict comparisons whose
/// boundary arithmetic requires the general numeric propagation rules.
final class NumericIntervals
{
    private NumericIntervals() {}

    static Optional<Boolean> satisfiable(Collection<Constraint> constraints)
    {
        Map<String, Interval> intervals = new HashMap<>();
        boolean satisfied = true;
        for (Constraint constraint : constraints) {
            ResolutionBudget.consume();
            if (constraint instanceof RequireKind(_, Kind kind) && kind == Kind.NUMBER) {
                continue;
            }
            if (!(constraint instanceof NumericRelation(BinaryOperation(BinaryOperator operator, Expression left, Expression right))) ||
                    (operator != BinaryOperator.LESS_THAN_OR_EQUAL && operator != BinaryOperator.GREATER_THAN_OR_EQUAL && operator != BinaryOperator.EQUAL)) {
                return Optional.empty();
            }
            if (left instanceof Literal(int leftValue) && right instanceof Literal(int rightValue)) {
                satisfied &= switch (operator) {
                    case LESS_THAN_OR_EQUAL -> leftValue <= rightValue;
                    case GREATER_THAN_OR_EQUAL -> leftValue >= rightValue;
                    case EQUAL -> leftValue == rightValue;
                    default -> throw new IllegalStateException("Unexpected operator: " + operator);
                };
                continue;
            }
            if (left instanceof Literal && right instanceof Variable) {
                Expression temporary = left;
                left = right;
                right = temporary;
                operator = switch (operator) {
                    case LESS_THAN_OR_EQUAL -> BinaryOperator.GREATER_THAN_OR_EQUAL;
                    case GREATER_THAN_OR_EQUAL -> BinaryOperator.LESS_THAN_OR_EQUAL;
                    default -> operator;
                };
            }
            if (!(left instanceof Variable(String name)) || !(right instanceof Literal(int value))) {
                return Optional.empty();
            }
            Interval interval = intervals.computeIfAbsent(name, _ -> new Interval());
            if (operator != BinaryOperator.LESS_THAN_OR_EQUAL) {
                interval.minimum = Math.max(interval.minimum, value);
            }
            if (operator != BinaryOperator.GREATER_THAN_OR_EQUAL) {
                interval.maximum = Math.min(interval.maximum, value);
            }
            satisfied &= interval.minimum <= interval.maximum;
        }
        return Optional.of(satisfied);
    }

    private static final class Interval
    {
        private int minimum = Integer.MIN_VALUE;
        private int maximum = Integer.MAX_VALUE;
    }
}
