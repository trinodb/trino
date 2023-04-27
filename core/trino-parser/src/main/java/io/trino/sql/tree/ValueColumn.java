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
import io.trino.sql.tree.JsonValue.EmptyOrErrorBehavior;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.tree.JsonValue.EmptyOrErrorBehavior.DEFAULT;
import static java.util.Objects.requireNonNull;

public class ValueColumn
        extends JsonTableColumnDefinition
{
    private final Identifier name;
    private final DataType type;
    private final Optional<StringLiteral> jsonPath;
    private final EmptyOrErrorBehavior emptyBehavior;
    private final Optional<Expression> emptyDefault;
    private final Optional<EmptyOrErrorBehavior> errorBehavior;
    private final Optional<Expression> errorDefault;

    public ValueColumn(
            NodeLocation location,
            Identifier name,
            DataType type,
            Optional<StringLiteral> jsonPath,
            EmptyOrErrorBehavior emptyBehavior,
            Optional<Expression> emptyDefault,
            Optional<EmptyOrErrorBehavior> errorBehavior,
            Optional<Expression> errorDefault)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.type = requireNonNull(type, "type is null");
        this.jsonPath = requireNonNull(jsonPath, "jsonPath is null");
        this.emptyBehavior = requireNonNull(emptyBehavior, "emptyBehavior is null");
        this.emptyDefault = requireNonNull(emptyDefault, "emptyDefault is null");
        checkArgument(emptyBehavior == DEFAULT || !emptyDefault.isPresent(), "default value can be specified only for DEFAULT ... ON EMPTY option");
        checkArgument(emptyBehavior != DEFAULT || emptyDefault.isPresent(), "DEFAULT ... ON EMPTY option requires default value");
        this.errorBehavior = requireNonNull(errorBehavior, "errorBehavior is null");
        this.errorDefault = requireNonNull(errorDefault, "errorDefault is null");
        checkArgument(errorBehavior.isPresent() && errorBehavior.get() == DEFAULT || !errorDefault.isPresent(), "default value can be specified only for DEFAULT ... ON ERROR option");
        checkArgument(!errorBehavior.isPresent() || errorBehavior.get() != DEFAULT || errorDefault.isPresent(), "DEFAULT ... ON ERROR option requires default value");
    }

    public Identifier getName()
    {
        return name;
    }

    public DataType getType()
    {
        return type;
    }

    public Optional<StringLiteral> getJsonPath()
    {
        return jsonPath;
    }

    public EmptyOrErrorBehavior getEmptyBehavior()
    {
        return emptyBehavior;
    }

    public Optional<Expression> getEmptyDefault()
    {
        return emptyDefault;
    }

    public Optional<EmptyOrErrorBehavior> getErrorBehavior()
    {
        return errorBehavior;
    }

    public Optional<Expression> getErrorDefault()
    {
        return errorDefault;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitValueColumn(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> children = ImmutableList.builder();
        jsonPath.ifPresent(children::add);
        emptyDefault.ifPresent(children::add);
        errorDefault.ifPresent(children::add);
        return children.build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("jsonPath", jsonPath.orElse(null))
                .add("emptyBehavior", emptyBehavior)
                .add("emptyDefault", emptyDefault.orElse(null))
                .add("errorBehavior", errorBehavior.orElse(null))
                .add("errorDefault", errorDefault.orElse(null))
                .omitNullValues()
                .toString();
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

        ValueColumn that = (ValueColumn) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(type, that.type) &&
                Objects.equals(jsonPath, that.jsonPath) &&
                emptyBehavior == that.emptyBehavior &&
                Objects.equals(emptyDefault, that.emptyDefault) &&
                Objects.equals(errorBehavior, that.errorBehavior) &&
                Objects.equals(errorDefault, that.errorDefault);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, jsonPath, emptyBehavior, emptyDefault, errorBehavior, errorDefault);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        ValueColumn otherValueColumn = (ValueColumn) other;

        return name.equals(otherValueColumn.name) &&
                type.equals(otherValueColumn.type) &&
                emptyBehavior == otherValueColumn.emptyBehavior &&
                Objects.equals(errorBehavior, otherValueColumn.errorBehavior);
    }
}
