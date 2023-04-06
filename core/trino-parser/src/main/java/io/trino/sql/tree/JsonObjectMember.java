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

public class JsonObjectMember
        extends Node
{
    private final Expression key;
    private final Expression value;
    private final Optional<JsonFormat> format;

    public JsonObjectMember(NodeLocation location, Expression key, Expression value, Optional<JsonFormat> format)
    {
        this(Optional.of(location), key, value, format);
    }

    public JsonObjectMember(Expression key, Expression value, Optional<JsonFormat> format)
    {
        this(Optional.empty(), key, value, format);
    }

    private JsonObjectMember(Optional<NodeLocation> location, Expression key, Expression value, Optional<JsonFormat> format)
    {
        super(location);

        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");
        requireNonNull(format, "format is null");

        this.key = key;
        this.value = value;
        this.format = format;
    }

    public Expression getKey()
    {
        return key;
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
        return visitor.visitJsonObjectMember(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of(key, value);
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

        JsonObjectMember that = (JsonObjectMember) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                Objects.equals(format, that.format);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(key, value, format);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("key", key)
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

        return format.equals(((JsonObjectMember) other).format);
    }
}
