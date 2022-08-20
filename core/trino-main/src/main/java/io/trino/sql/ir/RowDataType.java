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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class RowDataType
        extends DataType
{
    private final List<Field> fields;

    @JsonCreator
    public RowDataType(
            @JsonProperty("fields") List<Field> fields)
    {
        this.fields = ImmutableList.copyOf(fields);
    }

    @JsonProperty
    public List<Field> getFields()
    {
        return fields;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return fields;
    }

    @Override
    protected <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitRowDataType(this, context);
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
        RowDataType that = (RowDataType) o;
        return fields.equals(that.fields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fields);
    }

    public static class Field
            extends Node
    {
        private final Optional<Identifier> name;
        private final DataType type;

        @JsonCreator
        public Field(
                @JsonProperty("name") Optional<Identifier> name,
                @JsonProperty("type") DataType type)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public Optional<Identifier> getName()
        {
            return name;
        }

        @JsonProperty
        public DataType getType()
        {
            return type;
        }

        @Override
        public List<? extends Node> getChildren()
        {
            ImmutableList.Builder<Node> children = ImmutableList.builder();
            name.ifPresent(children::add);
            children.add(type);

            return children.build();
        }

        @Override
        protected <R, C> R accept(IrVisitor<R, C> visitor, C context)
        {
            return visitor.visitRowField(this, context);
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            if (name.isPresent()) {
                builder.append(name.get());
                builder.append(" ");
            }
            builder.append(type);

            return builder.toString();
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
            Field field = (Field) o;
            return name.equals(field.name) &&
                    type.equals(field.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, type);
        }

        @Override
        public boolean shallowEquals(Node other)
        {
            return sameClass(this, other);
        }
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
