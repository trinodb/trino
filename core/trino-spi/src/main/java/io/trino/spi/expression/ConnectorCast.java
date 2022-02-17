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
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class ConnectorCast
        extends ConnectorExpression
{
    private final ConnectorExpression expression;

    public ConnectorCast(Type type, ConnectorExpression expression)
    {
        super(type);
        this.expression = requireNonNull(expression, "expressions is null");
    }

    public ConnectorExpression getExpression()
    {
        return expression;
    }

    @Override
    public List<? extends ConnectorExpression> getChildren()
    {
        return List.of(expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(this.getType(), expression);
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
        ConnectorCast cast = (ConnectorCast) o;
        return Objects.equals(getType(), cast.getType()) &&
                Objects.equals(expression, cast.expression);
    }

    @Override
    public String toString()
    {
        StringJoiner stringJoiner = new StringJoiner(", ", ConnectorCast.class.getSimpleName() + "[", "]");
        return stringJoiner
                .add("type=" + getType())
                .add("expression=" + expression)
                .toString();
    }
}
