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

/// SQL spec `<quantified comparison predicate part 2> ::= <comp op> <quantifier> ( <table subquery> )`.
/// The quantifier (`ALL`, `ANY`, `SOME`) lives alongside the comparison operator
/// (without the [ComparisonPredicate] class's flip/negate machinery, which doesn't lift to
/// quantified form).
public final class QuantifiedComparisonPredicate
        extends Predicate
{
    public enum Quantifier
    {
        ALL,
        ANY,
        SOME,
    }

    private final ComparisonPredicate.Operator operator;
    private final Quantifier quantifier;
    private final Expression subquery;

    public QuantifiedComparisonPredicate(NodeLocation location, ComparisonPredicate.Operator operator, Quantifier quantifier, Expression subquery)
    {
        super(location);
        this.operator = requireNonNull(operator, "operator is null");
        this.quantifier = requireNonNull(quantifier, "quantifier is null");
        this.subquery = requireNonNull(subquery, "subquery is null");
    }

    public ComparisonPredicate.Operator getOperator()
    {
        return operator;
    }

    public Quantifier getQuantifier()
    {
        return quantifier;
    }

    public Expression getSubquery()
    {
        return subquery;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return List.of(subquery);
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitQuantifiedComparisonPredicate(this, context);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other)
                && operator == ((QuantifiedComparisonPredicate) other).operator
                && quantifier == ((QuantifiedComparisonPredicate) other).quantifier;
    }

    @Override
    public boolean equals(Object o)
    {
        return o instanceof QuantifiedComparisonPredicate that
                && operator == that.operator
                && quantifier == that.quantifier
                && subquery.equals(that.subquery);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, quantifier, subquery);
    }

    @Override
    public String toString()
    {
        return operator.getValue() + " " + quantifier + " " + subquery;
    }
}
