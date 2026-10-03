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

import io.trino.Session;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinType;

import static io.trino.SystemSessionProperties.isUseLegacyDecorrelator;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.correlation;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;

/// Normalizes an INNER correlated join with a non-trivial filter to a plain filter above the
/// filter-free correlated join: `CJ_INNER(input, subquery, f) ≡ σ_f(CJ_INNER(input, subquery, true))` — INNER correlated-join semantics are "lateral product, then filter". This
/// lets the rest of the dependent-join rule family (and the surviving shape-local unnest rules)
/// assume a TRUE filter on INNER joins. LEFT correlated joins keep their filter: it decides
/// null-extension and is folded into the magic-set's join-back condition instead.
public class ExtractCorrelatedJoinFilter
        implements Rule<CorrelatedJoinNode>
{
    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(nonEmpty(correlation()));

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
    public Result apply(CorrelatedJoinNode node, Captures captures, Context context)
    {
        if (node.getType() != JoinType.INNER || node.getFilter().equals(TRUE)) {
            return Result.empty();
        }
        return Result.ofPlanNode(new FilterNode(
                context.getIdAllocator().getNextId(),
                new CorrelatedJoinNode(
                        context.getIdAllocator().getNextId(),
                        node.getInput(),
                        node.getSubquery(),
                        node.getCorrelation(),
                        JoinType.INNER,
                        TRUE,
                        node.getOriginSubquery()),
                node.getFilter()));
    }
}
