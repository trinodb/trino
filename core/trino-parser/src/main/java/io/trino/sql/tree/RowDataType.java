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

import static java.util.Objects.requireNonNull;

public final class RowDataType
        extends DataType
{
    private final List<Field> fields;

    public RowDataType(NodeLocation location, List<Field> fields)
    {
        super(Optional.of(location));
        this.fields = ImmutableList.copyOf(fields);
    }

    public RowDataType(Optional<NodeLocation> location, List<Field> fields)
    {
        super(location);
        this.fields = ImmutableList.copyOf(fields);
    }

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

    public static final class Field
            extends Node
    {
        private final Optional<Identifier> name;
        private final DataType type;
        private final Optional<Expression> defaultValue;

        public Field(NodeLocation location, Optional<Identifier> name, DataType type)
        {
            this(location, name, type, Optional.empty());
        }

        public Field(NodeLocation location, Optional<Identifier> name, DataType type, Optional<Expression> defaultValue)
        {
            super(location);

            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
            this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
        }

        @Deprecated
        public Field(Optional<NodeLocation> location, Optional<Identifier> name, DataType type)
        {
            super(location);

            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
            this.defaultValue = Optional.empty();
        }

        public Field(Optional<NodeLocation> location, Optional<Identifier> name, DataType type, Optional<Expression> defaultValue)
        {
            super(location);

            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
            this.defaultValue = requireNonNull(defaultValue, "defaultValue is null");
        }

        public Optional<Identifier> getName()
        {
            return name;
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
        public List<? extends Node> getChildren()
        {
            ImmutableList.Builder<Node> children = ImmutableList.builder();
            name.ifPresent(children::add);
            children.add(type);
            defaultValue.ifPresent(children::add);

            return children.build();
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
            defaultValue.ifPresent(value -> builder.append(" DEFAULT ").append(value));

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
                    type.equals(field.type) &&
                    defaultValue.equals(field.defaultValue);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, type, defaultValue);
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
