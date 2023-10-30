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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class VariableDeclaration
        extends ControlStatement
{
    private final List<Identifier> names;
    private final DataType type;
    private final Optional<Expression> defaultValue;

    public VariableDeclaration(NodeLocation location, List<Identifier> names, DataType type, Optional<Expression> defaultValue)
    {
        super(location);
        this.names = requireNonNull(names, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
    }

    public List<Identifier> getNames()
    {
        return names;
    }

    public DataType getType()
    {
        return type;
    }

    public Optional<Expression> getDefaultValue()
    {
        return defaultValue;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitVariableDeclaration(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return defaultValue.map(ImmutableList::of).orElse(ImmutableList.of());
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof VariableDeclaration other) &&
                Objects.equals(names, other.names) &&
                Objects.equals(type, other.type) &&
                Objects.equals(defaultValue, other.defaultValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(names, type, defaultValue);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("names", names)
                .add("type", type)
                .add("defaultValue", defaultValue)
                .toString();
    }
}
