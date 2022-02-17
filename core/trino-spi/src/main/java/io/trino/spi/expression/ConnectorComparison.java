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

import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class ConnectorComparison
        extends ConnectorExpression
{
    private final String operatorSymbol;
    private final ConnectorExpression left;
    private final Optional<ConnectorExpression> right;

    public ConnectorComparison(Type type, String operatorType, ConnectorExpression left, Optional<ConnectorExpression> right)
    {
        super(type);
        this.operatorSymbol = requireNonNull(operatorType, "operatorSymbol is null");
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return right.map(connectorExpression -> List.of(left, connectorExpression))
                .orElseGet(() -> List.of(left));
    }

    public String getOperatorSymbol()
    {
        return operatorSymbol;
    }

    public ConnectorExpression getLeft()
    {
        return left;
    }

    public Optional<ConnectorExpression> getRight()
    {
        return right;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operatorSymbol, left, right);
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
        ConnectorComparison comparison = (ConnectorComparison) o;
        return Objects.equals(getType(), comparison.getType()) &&
                Objects.equals(operatorSymbol, comparison.getOperatorSymbol()) &&
                Objects.equals(left, comparison.getLeft()) &&
                Objects.equals(right, comparison.getRight());
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", ConnectorComparison.class.getSimpleName() + "[", "]");
        return stringJoiner
                .add("symbol=" + getOperatorSymbol())
                .add("left=" + left)
                .add("right=" + right)
                .toString();
    }
}
