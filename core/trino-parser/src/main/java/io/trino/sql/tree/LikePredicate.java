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

/// SQL spec `<character like predicate part 2> ::= [NOT] LIKE <character pattern> [ESCAPE <escape character>]`.
/// The in-place `NOT LIKE` is recorded via [#isNegated()]; outer `NOT (x LIKE y)`
/// stays as a [NotExpression] wrapping a non-negated `LikePredicate`.
public final class LikePredicate
        extends Predicate
{
    private final boolean negated;
    private final Expression pattern;
    private final Optional<Expression> escape;

    public LikePredicate(NodeLocation location, boolean negated, Expression pattern, Optional<Expression> escape)
    {
        super(location);
        this.negated = negated;
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.escape = requireNonNull(escape, "escape is null");
    }

    public boolean isNegated()
    {
        return negated;
    }

    public Expression getPattern()
    {
        return pattern;
    }

    public Optional<Expression> getEscape()
    {
        return escape;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        ImmutableList.Builder<Node> builder = ImmutableList.<Node>builder().add(pattern);
        escape.ifPresent(builder::add);
        return builder.build();
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitLikePredicate(this, context);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other) && negated == ((LikePredicate) other).negated;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof LikePredicate that
                && negated == that.negated
                && pattern.equals(that.pattern)
                && escape.equals(that.escape);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(negated, pattern, escape);
    }

    @Override
    public String toString()
    {
        return (negated ? "NOT LIKE " : "LIKE ") + pattern + escape.map(e -> " ESCAPE " + e).orElse("");
    }
}
