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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.PlanNode;

import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtLeastScalar;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.sql.planner.plan.Patterns.CorrelatedJoin.filter;
import static io.trino.sql.planner.plan.Patterns.correlatedJoin;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

public class RemoveUnreferencedScalarSubqueries
        implements Rule<CorrelatedJoinNode>
{
    private static final Pattern<CorrelatedJoinNode> PATTERN = correlatedJoin()
            .with(filter().equalTo(TRUE_LITERAL));

    @Override
    public Pattern<CorrelatedJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(CorrelatedJoinNode correlatedJoinNode, Captures captures, Context context)
    {
        PlanNode input = correlatedJoinNode.getInput();
        PlanNode subquery = correlatedJoinNode.getSubquery();

        if (isUnreferencedScalar(input, context.getLookup()) && correlatedJoinNode.getCorrelation().isEmpty()) {
            if (correlatedJoinNode.getType() == INNER
                    || correlatedJoinNode.getType() == RIGHT
                    || isAtLeastScalar(subquery, context.getLookup())) {
                return Result.ofPlanNode(subquery);
            }
        }

        if (isUnreferencedScalar(subquery, context.getLookup())) {
            return Result.ofPlanNode(input);
        }

        return Result.empty();
    }

    private boolean isUnreferencedScalar(PlanNode planNode, Lookup lookup)
    {
        return planNode.getOutputSymbols().isEmpty() && isScalar(planNode, lookup);
    }
}
