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

public class PivotAggregation
        extends Node
{
    private final Expression expression;
    private final Optional<Identifier> alias;

    public PivotAggregation(NodeLocation location, Expression expression, Optional<Identifier> alias)
    {
        super(location);
        this.expression = requireNonNull(expression, "expression is null");
        this.alias = requireNonNull(alias, "alias is null");
    }

    public Expression getExpression()
    {
        return expression;
    }

    public Optional<Identifier> getAlias()
    {
        return alias;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPivotAggregation(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        // alias is a scalar attribute, not a child: it is compared in shallowEquals instead.
        return ImmutableList.of(expression);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("expression", expression)
                .add("alias", alias.orElse(null))
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
        PivotAggregation that = (PivotAggregation) o;
        return Objects.equals(expression, that.expression) &&
                Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, alias);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }
        return Objects.equals(alias, ((PivotAggregation) other).alias);
    }
}
