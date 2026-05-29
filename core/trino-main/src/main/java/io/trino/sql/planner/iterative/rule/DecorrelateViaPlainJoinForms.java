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

import com.google.common.collect.ImmutableSet;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra;
import io.trino.sql.planner.optimizations.decorrelation.PlainJoinForms;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.Set;

import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isScalarGlobalAggregation;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isScalarSubqueryShape;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;

/// Decorrelates via the single-join legacy-parity forms (see [PlainJoinForms]): a
/// filter-only LIMIT/TopN re-emitted below a plain join when its grouping is
/// equality-correlated, and — for LEFT correlated joins — the dedup-below/dedup-above,
/// grouped-aggregation, and plain-LEFT-join forms. These beat both the generic pushdown and the
/// magic-set on plan quality (no unique-id window, no input clone), so the rule is registered
/// before them.
public class DecorrelateViaPlainJoinForms
        extends DependentJoinFrameworkRule
{
    public DecorrelateViaPlainJoinForms(PlannerContext plannerContext)
    {
        super(plannerContext);
    }

    /// The single-join legacy-parity forms that beat both the generic per-operator pushdown and the
    /// magic-set on plan quality: a filter-only LIMIT/TopN re-emitted below a plain join when its
    /// grouping is equality-correlated (INNER and LEFT), and — for a LEFT correlated join — the
    /// dedup-below/dedup-above, grouped-aggregation, and plain-join forms. All are decided by a
    /// walk of the subquery chain; no pushdown is involved, so a non-match cleanly yields to the
    /// next rule.
    @Override
    protected Optional<PlanNode> apply(CorrelatedJoinNode correlatedJoin, DependentJoinAlgebra algebra, Context context)
    {
        if (!correlatedJoin.getFilter().equals(TRUE)) {
            return Optional.empty();
        }
        PlanNode subquery = correlatedJoin.getSubquery();
        if (isScalarSubqueryShape(subquery, context.getLookup()) || isScalarGlobalAggregation(subquery, context.getLookup())) {
            return Optional.empty();
        }
        PlainJoinForms forms = new PlainJoinForms(context.getIdAllocator(), context.getSymbolAllocator(), context.getLookup(), plannerContext());
        PlanNode input = correlatedJoin.getInput();
        Set<Symbol> correlation = ImmutableSet.copyOf(correlatedJoin.getCorrelation());

        Optional<PlanNode> result = forms.tryPlainJoinLimit(correlatedJoin.getType(), input, subquery, correlation);
        if (correlatedJoin.getType() == JoinType.LEFT) {
            if (result.isEmpty()) {
                result = forms.tryPlainLeftJoinDistinct(input, subquery, correlation);
            }
            if (result.isEmpty()) {
                result = forms.tryGroupedAggregationLeftJoin(input, subquery, correlation);
            }
            if (result.isEmpty()) {
                result = forms.tryPlainLeftJoin(input, subquery, correlation);
            }
        }
        return result.map(plan -> algebra.restrictOutputs(plan, correlatedJoin.getOutputSymbols()));
    }
}
