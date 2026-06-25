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

/// The `<predicate part 2>` fragment of the SQL spec: the operator and right-hand side(s)
/// of a predicate, without the LHS value. Concrete subtypes parallel the spec's per-predicate
/// part-2 productions (comparison, between, in, like, null, boolean-test, distinct,
/// quantified-comparison, match).
///
/// Used in two positions: as the `clause` of a [Predicated], and as a SQL:2023 F262
/// extended `CASE` WHEN operand where the LHS is the case operand.
public abstract sealed class Predicate
        extends Node
        permits BetweenPredicate,
                BooleanTestPredicate,
                ComparisonPredicate,
                DistinctFromPredicate,
                InPredicate,
                IsNullPredicate,
                LikePredicate,
                MatchPredicate,
                QuantifiedComparisonPredicate
{
    protected Predicate(NodeLocation location)
    {
        super(location);
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitPredicate(this, context);
    }
}
