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

import static java.util.Objects.requireNonNull;

public class WhenClause
        extends Expression
{
    /// What this WHEN clause matches on. [Operand] carries a value expression — a boolean for
    /// searched CASE, an equality target for simple CASE. [Partial] carries a SQL:2023 F262
    /// predicate fragment evaluated against the surrounding simple-CASE operand.
    public sealed interface Match
    {
        /// The wrapped [Node]; used by tree-walking visitors.
        Node node();
    }

    public record Operand(Expression expression)
            implements Match
    {
        public Operand
        {
            requireNonNull(expression, "expression is null");
        }

        @Override
        public Node node()
        {
            return expression;
        }
    }

    public record Partial(Predicate predicate)
            implements Match
    {
        public Partial
        {
            requireNonNull(predicate, "predicate is null");
        }

        @Override
        public Node node()
        {
            return predicate;
        }
    }

    private final Match match;
    private final Expression result;

    public WhenClause(NodeLocation location, Match match, Expression result)
    {
        super(location);
        this.match = requireNonNull(match, "match is null");
        this.result = requireNonNull(result, "result is null");
    }

    public WhenClause(NodeLocation location, Expression operand, Expression result)
    {
        this(location, new Operand(operand), result);
    }

    /// SQL:2023 F262 extended-CASE WHEN clause: the match side is a predicate fragment (the
    /// `<when operand>` production) over the surrounding simple-CASE operand. The LHS is
    /// supplied implicitly by the enclosing [SimpleCaseExpression].
    public WhenClause(NodeLocation location, Predicate predicate, Expression result)
    {
        this(location, new Partial(predicate), result);
    }

    public Match getMatch()
    {
        return match;
    }

    /// Returns the value-expression match side — a boolean condition for searched CASE, or an
    /// equality target for simple CASE. Throws when this clause carries an F262 predicate fragment
    /// instead.
    ///
    /// @deprecated Prefer [#getMatch()] and pattern-match on the [Match] variant.
    @Deprecated
    public Expression getOperand()
    {
        if (match instanceof Operand operand) {
            return operand.expression();
        }
        throw new IllegalStateException("WhenClause is a predicate-fragment match, not an operand: " + match);
    }

    public Expression getResult()
    {
        return result;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitWhenClause(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(match.node(), result);
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

        WhenClause that = (WhenClause) o;
        return match.equals(that.match) && result.equals(that.result);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(match, result);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
