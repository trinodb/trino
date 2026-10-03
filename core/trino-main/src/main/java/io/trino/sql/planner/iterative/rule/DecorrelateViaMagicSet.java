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

package io.trino.sql.planner.iterative.rule;

import io.trino.sql.PlannerContext;
import io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;

import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isScalarGlobalAggregation;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isScalarSubqueryShape;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;

/// Decorrelates a LEFT correlated join via the magic-set decomposition
/// ([DependentJoinAlgebra#magicSetJoin]): the subquery is evaluated once per distinct
/// correlation value of the input (a residual INNER dependent join over the distinct set) and the
/// input is LEFT-joined back NULL-safely on the correlation columns, with the correlated-join
/// filter folded into the join-back condition. The general LEFT fallback — registered last, after
/// the single-join forms have had their chance.
public class DecorrelateViaMagicSet
        extends DependentJoinFrameworkRule
{
    public DecorrelateViaMagicSet(PlannerContext plannerContext)
    {
        super(plannerContext);
    }

    /// The general LEFT decorrelation: the magic-set decomposition (see [DependentJoinAlgebra#magicSetJoin]),
    /// with a non-trivial correlated-join filter folded into the NULL-safe join-back condition.
    /// Ordered after the plain forms, so it only fires when no single-join form matched.
    @Override
    protected Optional<PlanNode> apply(CorrelatedJoinNode correlatedJoin, DependentJoinAlgebra algebra, Context context)
    {
        if (correlatedJoin.getType() != JoinType.LEFT) {
            return Optional.empty();
        }
        PlanNode subquery = correlatedJoin.getSubquery();
        if (isScalarSubqueryShape(subquery, context.getLookup()) || isScalarGlobalAggregation(subquery, context.getLookup())) {
            return Optional.empty();
        }
        return algebra.magicSetJoin(
                        JoinType.LEFT,
                        correlatedJoin.getInput(),
                        subquery,
                        correlatedJoin.getCorrelation(),
                        correlatedJoin.getFilter(),
                        subquery.getOutputSymbols())
                .map(plan -> algebra.restrictOutputs(plan, correlatedJoin.getOutputSymbols()));
    }
}
