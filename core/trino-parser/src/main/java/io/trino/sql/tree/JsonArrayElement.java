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
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class JsonArrayElement
        extends Node
{
    private final Expression value;
    private final Optional<JsonFormat> format;

    public JsonArrayElement(NodeLocation location, Expression value, Optional<JsonFormat> format)
    {
        this(Optional.of(location), value, format);
    }

    public JsonArrayElement(Expression value, Optional<JsonFormat> format)
    {
        this(Optional.empty(), value, format);
    }

    public JsonArrayElement(Optional<NodeLocation> location, Expression value, Optional<JsonFormat> format)
    {
        super(location);

        requireNonNull(value, "value is null");
        requireNonNull(format, "format is null");

        this.value = value;
        this.format = format;
    }

    public Expression getValue()
    {
        return value;
    }

    public Optional<JsonFormat> getFormat()
    {
        return format;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitJsonArrayElement(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(value);
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

        JsonArrayElement that = (JsonArrayElement) o;
        return Objects.equals(value, that.value) &&
                Objects.equals(format, that.format);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, format);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("value", value)
                .add("format", format)
                .omitNullValues()
                .toString();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return format.equals(((JsonArrayElement) other).format);
    }
}
