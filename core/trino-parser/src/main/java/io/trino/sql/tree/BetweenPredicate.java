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
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// SQL spec `<between predicate part 2> ::= [NOT] BETWEEN [ASYMMETRIC | SYMMETRIC] <row value predicand> AND <row value predicand>`.
/// The `negated` flag captures the in-place `NOT BETWEEN` form; outer
/// `NOT (x BETWEEN ...)` stays as a [NotExpression] wrapping a non-negated
/// `BetweenPredicate`. The `symmetry` is the optional `ASYMMETRIC` / `SYMMETRIC`
/// keyword exactly as written: empty when neither was given (the default,
/// semantically `ASYMMETRIC`), so the predicate round-trips through the formatter.
/// `SYMMETRIC` treats the bounds as an unordered pair — the predicate holds when the
/// value lies between them in either order; the `ASYMMETRIC` form is `x >= min AND x <= max`.
public final class BetweenPredicate
        extends Predicate
{
    public enum Symmetry
    {
        ASYMMETRIC,
        SYMMETRIC,
    }

    private final boolean negated;
    private final Optional<Symmetry> symmetry;
    private final Expression min;
    private final Expression max;

    public BetweenPredicate(NodeLocation location, boolean negated, Optional<Symmetry> symmetry, Expression min, Expression max)
    {
        super(location);
        this.negated = negated;
        this.symmetry = requireNonNull(symmetry, "symmetry is null");
        this.min = requireNonNull(min, "min is null");
        this.max = requireNonNull(max, "max is null");
    }

    public boolean isNegated()
    {
        return negated;
    }

    public Optional<Symmetry> getSymmetry()
    {
        return symmetry;
    }

    public boolean isSymmetric()
    {
        return symmetry.map(value -> value == Symmetry.SYMMETRIC).orElse(false);
    }

    public Expression getMin()
    {
        return min;
    }

    public Expression getMax()
    {
        return max;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return List.of(min, max);
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBetweenPredicate(this, context);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other)
                && negated == ((BetweenPredicate) other).negated
                && symmetry.equals(((BetweenPredicate) other).symmetry);
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof BetweenPredicate that
                && negated == that.negated
                && symmetry.equals(that.symmetry)
                && min.equals(that.min)
                && max.equals(that.max);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(negated, symmetry, min, max);
    }

    @Override
    public String toString()
    {
        return (negated ? "NOT BETWEEN " : "BETWEEN ") + symmetry.map(value -> value + " ").orElse("") + min + " AND " + max;
    }
}
