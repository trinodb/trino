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
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;

import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

public final class PushJoinPredicates
        implements Rule<JoinNode>
{
    private final PlannerContext plannerContext;
    private final boolean useTableProperties;
    private final boolean dynamicFiltering;

    public PushJoinPredicates(PlannerContext plannerContext, boolean useTableProperties, boolean dynamicFiltering)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.useTableProperties = useTableProperties;
        this.dynamicFiltering = dynamicFiltering;
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return join();
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        PlanNode result = new Pushdown(context, plannerContext, useTableProperties, dynamicFiltering).pushThroughJoin(node, TRUE);
        return result == node ? Result.empty() : Result.ofPlanNode(result);
    }
}
