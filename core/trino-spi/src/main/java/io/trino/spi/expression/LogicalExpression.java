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
package io.trino.spi.expression;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class LogicalExpression
        extends ConnectorExpression
{
    public enum Operator
    {
        AND, OR;
    }

    private final Operator operator;
    private final List<ConnectorExpression> terms;

    public LogicalExpression(Operator operator, List<ConnectorExpression> terms)
    {
        super(BOOLEAN);

        requireNonNull(operator, "operator is null");
        requireNonNull(terms, "terms is null");
        if (terms.size() < 2) {
            throw new IllegalArgumentException("Expected at least 2 terms");
        }

        this.operator = operator;
        this.terms = List.copyOf(terms);
    }

    public Operator getOperator()
    {
        return operator;
    }

    public List<ConnectorExpression> getTerms()
    {
        return terms;
    }

    @Override
    public List<ConnectorExpression> getChildren()
    {
        return terms;
    }

    public static LogicalExpression and(ConnectorExpression left, ConnectorExpression right)
    {
        return new LogicalExpression(Operator.AND, List.of(left, right));
    }

    public static LogicalExpression or(ConnectorExpression left, ConnectorExpression right)
    {
        return new LogicalExpression(Operator.OR, List.of(left, right));
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
        LogicalExpression that = (LogicalExpression) o;
        return operator == that.operator && Objects.equals(terms, that.terms);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, terms);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", LogicalExpression.class.getSimpleName() + "[", "]")
                .add(operator.toString())
                .add(terms.toString())
                .toString();
    }
}
