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
package io.trino.spi.connector;

import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toUnmodifiableMap;

@Deprecated
public final class JoinCondition
{
    @Deprecated
    public enum Operator
    {
        EQUAL("=", StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME),
        NOT_EQUAL("<>", StandardFunctions.NOT_EQUAL_OPERATOR_FUNCTION_NAME),
        LESS_THAN("<", StandardFunctions.LESS_THAN_OPERATOR_FUNCTION_NAME),
        LESS_THAN_OR_EQUAL("<=", StandardFunctions.LESS_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME),
        GREATER_THAN(">", StandardFunctions.GREATER_THAN_OPERATOR_FUNCTION_NAME),
        GREATER_THAN_OR_EQUAL(">=", StandardFunctions.GREATER_THAN_OR_EQUAL_OPERATOR_FUNCTION_NAME),
        IS_DISTINCT_FROM("IS DISTINCT FROM", StandardFunctions.IS_DISTINCT_FROM_OPERATOR_FUNCTION_NAME),
        /**/;

        private static final Map<FunctionName, Operator> byFunctionName = Stream.of(values())
                .collect(toUnmodifiableMap(operator -> operator.callFunctionName, identity()));

        private final String value;
        private final FunctionName callFunctionName;

        Operator(String value, FunctionName callFunctionName)
        {
            this.value = value;
            this.callFunctionName = callFunctionName;
        }

        public String getValue()
        {
            return value;
        }

        public Operator flip()
        {
            return switch (this) {
                case EQUAL, NOT_EQUAL, IS_DISTINCT_FROM -> this;
                case LESS_THAN -> GREATER_THAN;
                case LESS_THAN_OR_EQUAL -> GREATER_THAN_OR_EQUAL;
                case GREATER_THAN -> LESS_THAN;
                case GREATER_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL;
            };
        }
    }

    public static Optional<JoinCondition> from(ConnectorExpression expression, Set<String> leftSymbols, Set<String> rightSymbols)
    {
        if (expression instanceof Call call && call.getArguments().size() == 2) {
            return Optional.ofNullable(Operator.byFunctionName.get(call.getFunctionName()))
                    .flatMap(operator -> {
                        rightSymbols.stream().filter(leftSymbols::contains).findAny().ifPresent(symbol -> {
                            throw new IllegalArgumentException(
                                    "Left and right symbol sets overlap, are both include %s: %s, %s".formatted(symbol, leftSymbols, rightSymbols));
                        });
                        ConnectorExpression left = call.getArguments().get(0);
                        ConnectorExpression right = call.getArguments().get(1);
                        Set<String> leftExpressionSymbols = findVariableNames(left);
                        Set<String> rightExpressionSymbols = findVariableNames(right);
                        if (leftSymbols.containsAll(leftExpressionSymbols) && rightSymbols.containsAll(rightExpressionSymbols)) {
                            return Optional.of(new JoinCondition(operator, left, right));
                        }
                        if (rightSymbols.containsAll(leftExpressionSymbols) && leftSymbols.containsAll(rightExpressionSymbols)) {
                            // normalize
                            return Optional.of(new JoinCondition(operator.flip(), right, left));
                        }
                        return Optional.empty();
                    });
        }
        return Optional.empty();
    }

    private static Set<String> findVariableNames(ConnectorExpression expression)
    {
        Set<String> variableNames = new HashSet<>();
        Set<ConnectorExpression> visited = new HashSet<>();
        Queue<ConnectorExpression> pending = new ArrayDeque<>(List.of(expression));
        while (!pending.isEmpty()) {
            ConnectorExpression next = pending.remove();
            if (!visited.add(next)) {
                continue;
            }
            pending.addAll(next.getChildren());
            if (next instanceof Variable variable) {
                variableNames.add(variable.getName());
            }
        }
        return variableNames;
    }

    private final Operator operator;
    private final ConnectorExpression leftExpression;
    private final ConnectorExpression rightExpression;

    public JoinCondition(Operator operator, ConnectorExpression leftExpression, ConnectorExpression rightExpression)
    {
        this.operator = requireNonNull(operator, "operator is null");
        this.leftExpression = requireNonNull(leftExpression, "leftExpression is null");
        this.rightExpression = requireNonNull(rightExpression, "rightExpression is null");
    }

    public Operator getOperator()
    {
        return operator;
    }

    public ConnectorExpression getLeftExpression()
    {
        return leftExpression;
    }

    public ConnectorExpression getRightExpression()
    {
        return rightExpression;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinCondition that = (JoinCondition) o;
        return operator == that.operator &&
                Objects.equals(leftExpression, that.leftExpression) &&
                Objects.equals(rightExpression, that.rightExpression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, leftExpression, rightExpression);
    }

    @Override
    public String toString()
    {
        return format("%s %s %s", leftExpression, operator.getValue(), rightExpression);
    }
}
