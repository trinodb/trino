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
import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.decorrelation.DependentJoinAlgebra;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;
import java.util.Set;

import static io.trino.SystemSessionProperties.isUseLegacyDecorrelator;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.correlationCarriedAsJoinOutput;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.isDecorrelateUnnestShape;
import static io.trino.sql.planner.optimizations.decorrelation.CorrelatedSubqueryShapes.topNOrdersByCorrelation;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;
import static java.util.Objects.requireNonNull;

/// Base of the dependent-join decorrelation rule family (Neumann & Kemper, BTW 2015), active
/// only when `use_legacy_decorrelator = false`. Each subclass owns one decorrelation form;
/// the family is registered in priority order, and a rule that declines simply leaves the
/// [CorrelatedJoinNode] for the next one. A correlated join no rule accepts fails as an
/// unsupported correlated subquery. Shared admission checks run here: a shape failing them is
/// left untouched by the whole family and fails with the clean "correlated subquery
/// is not supported" error (matching legacy, which rejects these shapes too), except for the
/// `DecorrelateUnnest` deferral, which a surviving shape-local rule picks up.
///
/// Shapes no rule in the family handles (an outer join with both sides depending on
/// correlation, FULL outer joins, a non-aggregate scalar with a non-trivial LEFT correlated-join
/// filter, grouping sets carrying a global set) fail as unsupported — the legacy decorrelator
/// rejects them too. Correlated `IN` predicates do not arrive as a
/// [CorrelatedJoinNode]; the companion rule `RewriteCorrelatedInPredicateToCorrelatedJoin`
/// (also disabled by `use_legacy_decorrelator`) lowers `x IN (subquery)` into one over a global count
/// aggregation, which this family then decorrelates via the scalar-aggregation rule.
public abstract class DependentJoinFrameworkRule
        implements Rule<CorrelatedJoinNode>
{
    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(nonEmpty(correlation()));

    private final PlannerContext plannerContext;

    protected DependentJoinFrameworkRule(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    protected PlannerContext plannerContext()
    {
        return plannerContext;
    }

    @Override
    public Pattern<CorrelatedJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return !isUseLegacyDecorrelator(session);
    }

    @Override
    public final Result apply(CorrelatedJoinNode node, Captures captures, Context context)
    {
        if (!eligible(node, context.getLookup())) {
            return Result.empty();
        }
        DependentJoinAlgebra algebra = new DependentJoinAlgebra(
                context.getIdAllocator(),
                context.getSymbolAllocator(),
                context.getLookup(),
                node.getOriginSubquery());
        return apply(node, algebra, context).map(Result::ofPlanNode).orElseGet(Result::empty);
    }

    /// Shared admission checks for every framework rule. A shape failing them is left untouched by
    /// the whole rule family, and the query fails with the clean "correlated
    /// subquery is not supported" error (matching legacy, which rejects these shapes too), except
    /// for the `DecorrelateUnnest` deferral, which a surviving shape-local rule picks up.
    private static boolean eligible(CorrelatedJoinNode correlatedJoin, Lookup lookup)
    {
        JoinType type = correlatedJoin.getType();
        if (type != JoinType.INNER && type != JoinType.LEFT) {
            return false;
        }
        Set<Symbol> correlation = ImmutableSet.copyOf(correlatedJoin.getCorrelation());
        PlanNode subquery = correlatedJoin.getSubquery();

        // The framework rebinds free correlation references in expressions to the cloned correlation.
        // It cannot handle a correlation symbol that a join inside the subquery carries as a
        // structural output (a column that directly selects the outer value).
        if (correlationCarriedAsJoinOutput(subquery, correlation, lookup)) {
            return false;
        }
        // A TopN ordered by correlation has no meaningful per-outer-row order (the sort key is
        // constant within each outer row), so the bounded result is non-deterministic.
        if (topNOrdersByCorrelation(subquery, correlation, lookup)) {
            return false;
        }
        // Bounds/EnforceSingleRow/projections over an UNNEST of the correlation are the class
        // DecorrelateUnnest rewrites to a single join-free unnest with ordinality-based
        // bookkeeping — better than the magic-set's clone + array-keyed join-back, and
        // order-preserving. That rule is shape-local (it does not depend on the legacy
        // decorrelator) and is intended to outlive it; defer. A correlated filter inside the
        // chain (which that rule cannot handle) stays with the framework.
        if (correlatedJoin.getFilter().equals(TRUE) && isDecorrelateUnnestShape(subquery, correlation, lookup)) {
            return false;
        }
        return true;
    }

    protected abstract Optional<PlanNode> apply(CorrelatedJoinNode node, DependentJoinAlgebra algebra, Context context);
}
