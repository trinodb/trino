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

public class SessionSpecification
        extends Node
{
    private final QualifiedName name;
    private final Expression expression;

    public SessionSpecification(QualifiedName name, Expression expression)
    {
        this(Optional.empty(), name, expression);
    }

    public SessionSpecification(NodeLocation location, QualifiedName name, Expression expression)
    {
        this(Optional.of(location), name, expression);
    }

    public SessionSpecification(Optional<NodeLocation> location, QualifiedName name, Expression expression)
    {
        super(location);
        this.name = requireNonNull(name, "name is null");
        this.expression = requireNonNull(expression, "expression is null");
    }

    public QualifiedName getName()
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
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSessionSpecification(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, expression);
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof SessionSpecification other) &&
                Objects.equals(name, other.name) &&
                Objects.equals(expression, other.expression);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("expression", expression)
                .toString();
    }
}
