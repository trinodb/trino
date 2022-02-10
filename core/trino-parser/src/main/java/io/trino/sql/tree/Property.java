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
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

// TODO: Model DEFAULT as a type of expression to make it easier to do analysis, planning and error reporting in a uniform way
public class Property
        extends Node
{
    private final Identifier name;
    private final Optional<Expression> value;   // empty iff the value is set to DEFAULT

    /**
     * Constructs an instance representing a property whose value is set to DEFAULT
     */
    public Property(Identifier name)
    {
        this(Optional.empty(), name, Optional.empty());
    }

    /**
     * Constructs an instance representing a property whose value is set to DEFAULT
     */
    public Property(NodeLocation location, Identifier name)
    {
        this(Optional.of(requireNonNull(location, "location is null")), name, Optional.empty());
    }

    public Property(Identifier name, Expression value)
    {
        this(Optional.empty(), name, Optional.of(requireNonNull(value, "value is null")));
    }

    public Property(NodeLocation location, Identifier name, Expression value)
    {
        this(
                Optional.of(requireNonNull(location, "location is null")),
                name,
                Optional.of(requireNonNull(value, "value is null")));
    }

    private Property(Optional<NodeLocation> location, Identifier name, Optional<Expression> value)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.value = requireNonNull(value, "value is null");
    }

    public Identifier getName()
    {
        return name;
    }

    public boolean isSetToDefault()
    {
        return !value.isPresent();
    }

    /**
     * Returns the non-default value of the property. This method should be called only if the property is not set to DEFAULT.
     */
    public Expression getNonDefaultValue()
    {
        checkState(!isSetToDefault(), "Cannot get non-default value of property %s since its value is set to DEFAULT", name);
        return value.get();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitProperty(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return isSetToDefault() ? ImmutableList.of(name) : ImmutableList.of(name, getNonDefaultValue());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Property other = (Property) obj;
        return Objects.equals(name, other.name) &&
                Objects.equals(value, other.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, value);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("value", isSetToDefault() ? "DEFAULT" : getNonDefaultValue())
                .toString();
    }
}
