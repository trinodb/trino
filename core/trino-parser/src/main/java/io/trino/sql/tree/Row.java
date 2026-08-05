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

public final class Row
        extends Expression
{
    private final List<Field> fields;

    @Deprecated
    public Row(List<Field> fields)
    {
        super(Optional.empty());
        this.fields = ImmutableList.copyOf(fields);
    }

    public Row(NodeLocation location, List<Field> fields)
    {
        super(location);
        this.fields = ImmutableList.copyOf(fields);
    }

    public List<Field> getFields()
    {
        return fields;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRow(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return fields;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fields);
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
        Row other = (Row) obj;
        return Objects.equals(this.fields, other.fields);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }

    public static class Field
            extends Node
    {
        private final Optional<Identifier> name;
        private final Expression expression;

        public Field(NodeLocation location, Optional<Identifier> name, Expression expression)
        {
            super(location);

            this.name = requireNonNull(name, "name is null");
            this.expression = requireNonNull(expression, "expression is null");
        }

        @Deprecated
        public Field(Expression expression)
        {
            super(Optional.empty());

            this.name = Optional.empty();
            this.expression = requireNonNull(expression, "expression is null");
        }

        public Optional<Identifier> getName()
        {
            return name;
        }

        public Expression getExpression()
        {
            return expression;
        }

        @Override
        public List<? extends Node> getChildren()
        {
            ImmutableList.Builder<Node> children = ImmutableList.builder();
            name.ifPresent(children::add);
            children.add(expression);

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
            name.ifPresent(value -> builder.append(value).append(" "));
            builder.append(expression);

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
            Row.Field field = (Row.Field) o;
            return name.equals(field.name) &&
                    expression.equals(field.expression);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, expression);
        }

        @Override
        public boolean shallowEquals(Node other)
        {
            return sameClass(this, other);
        }
    }
}
