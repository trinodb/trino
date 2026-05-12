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

/// Represents the SQL spec's `<predicate> ::= <row value predicand> <predicate part 2>` shape:
/// a value (the LHS) paired with a predicate (the part-2 fragment).
///
/// The same [Predicate] types appear in two positions in the spec — as the right-hand side of
/// a full predicate (this class), and as a SQL:2023 F262 extended `CASE` WHEN operand, where
/// the case operand is implicitly bound to the LHS.
public final class Predicated
        extends Expression
{
    private final Expression value;
    private final Predicate predicate;

    public Predicated(NodeLocation location, Expression value, Predicate predicate)
    {
        super(location);
        this.value = requireNonNull(value, "value is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    public Expression getValue()
    {
        return value;
    }

    public Predicate getPredicate()
    {
        return predicate;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPredicated(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return List.of(value, predicate);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Predicated that)) {
            return false;
        }
        return value.equals(that.value) && predicate.equals(that.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, predicate);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        // Predicated has no shape of its own — the operator/quantifier/negation it pairs with a
        // value lives on the predicate child, walked separately by the visitor.
        return sameClass(this, other);
    }
}
