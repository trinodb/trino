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

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/// SQL spec `<match predicate part 2> ::= MATCH [UNIQUE] [SIMPLE | PARTIAL | FULL] <table subquery>`.
/// The LHS row value lives on the surrounding [Predicated] (regular use) or
/// [SimpleCaseExpression] (extended `CASE` WHEN use, per SQL:2023 F262).
public final class MatchPredicate
        extends Predicate
{
    public enum Type
    {
        SIMPLE,
        PARTIAL,
        FULL,
    }

    private final boolean unique;
    private final Type type;
    private final Expression subquery;

    public MatchPredicate(NodeLocation location, boolean unique, Type type, Expression subquery)
    {
        super(location);
        this.unique = unique;
        this.type = requireNonNull(type, "type is null");
        this.subquery = requireNonNull(subquery, "subquery is null");
    }

    public boolean isUnique()
    {
        return unique;
    }

    public Type getType()
    {
        return type;
    }

    public Expression getSubquery()
    {
        return subquery;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitMatchPredicate(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return List.of(subquery);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other)
                && unique == ((MatchPredicate) other).unique
                && type == ((MatchPredicate) other).type;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof MatchPredicate that
                && unique == that.unique
                && type == that.type
                && subquery.equals(that.subquery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(unique, type, subquery);
    }

    @Override
    public String toString()
    {
        return "MATCH" + (unique ? " UNIQUE " : " ") + type + " " + subquery;
    }
}
