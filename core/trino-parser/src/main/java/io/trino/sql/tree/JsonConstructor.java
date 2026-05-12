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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.JsonPathParameter.JsonFormat;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class JsonConstructor
        extends Expression
{
    private final Expression expression;
    private final JsonFormat format;

    public JsonConstructor(NodeLocation location, Expression expression, JsonFormat format)
    {
        super(location);
        this.expression = requireNonNull(expression, "expression is null");
        this.format = requireNonNull(format, "format is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    public JsonFormat getFormat()
    {
        return format;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonConstructor(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(expression);
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
        JsonConstructor that = (JsonConstructor) o;
        return Objects.equals(expression, that.expression) && format == that.format;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, format);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }
        return format == ((JsonConstructor) other).format;
    }
}
