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

import io.trino.spi.expression.ConnectorExpression;

import java.util.Objects;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class JoinCondition
{
    public enum Operator
    {
        EQUAL("="),
        NOT_EQUAL("<>"),
        LESS_THAN("<"),
        LESS_THAN_OR_EQUAL("<="),
        GREATER_THAN(">"),
        GREATER_THAN_OR_EQUAL(">="),
        IS_DISTINCT_FROM("IS DISTINCT FROM");

        private final String value;

        Operator(String value)
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }
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
