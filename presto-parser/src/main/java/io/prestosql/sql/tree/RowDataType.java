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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RowDataType
        extends DataType
{
    private final List<Field> fields;

    public RowDataType(NodeLocation location, List<Field> fields)
    {
        super(Optional.of(location));
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
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
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

        public Field(NodeLocation location, Optional<Identifier> name, DataType type)
        {
            super(Optional.of(location));

            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        public Optional<Identifier> getName()
        {
            return name;
        }

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
        protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
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
    }
}
