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

/// SQL spec `<boolean test> ::= <boolean primary> IS [NOT] <truth value>`, where
/// `<truth value> ::= TRUE | FALSE | UNKNOWN`. Tests the three-valued result of a boolean
/// operand against a truth value and always yields a non-NULL boolean. The in-place `NOT`
/// (i.e. `IS NOT TRUE`) is recorded via [#isNegated()]; an outer `NOT (x IS TRUE)` is a
/// separate [NotExpression] wrapping a non-negated `BooleanTestPredicate`.
///
/// Modeled as a [Predicate] subtype because, like the null predicate, it has a left-hand-side
/// operand and reduces to a non-NULL boolean. The operand is therefore the [Predicated] value.
public final class BooleanTestPredicate
        extends Predicate
{
    public enum TruthValue
    {
        TRUE,
        FALSE,
        UNKNOWN,
    }

    private final boolean negated;
    private final TruthValue truthValue;

    public BooleanTestPredicate(NodeLocation location, boolean negated, TruthValue truthValue)
    {
        super(location);
        this.negated = negated;
        this.truthValue = requireNonNull(truthValue, "truthValue is null");
    }

    public boolean isNegated()
    {
        return negated;
    }

    public TruthValue getTruthValue()
    {
        return truthValue;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return List.of();
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitBooleanTestPredicate(this, context);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }
        BooleanTestPredicate that = (BooleanTestPredicate) other;
        return negated == that.negated && truthValue == that.truthValue;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof BooleanTestPredicate that
                && negated == that.negated
                && truthValue == that.truthValue;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(negated, truthValue);
    }

    @Override
    public String toString()
    {
        return (negated ? "IS NOT " : "IS ") + truthValue;
    }
}
