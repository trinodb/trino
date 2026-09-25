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

import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

abstract class FilterPushdownRule<T extends PlanNode>
        implements Rule<FilterNode>
{
    private final PlannerContext plannerContext;
    private final boolean useTableProperties;
    private final boolean dynamicFiltering;

    private final Capture<T> sourceCapture = newCapture();
    private final Pattern<FilterNode> pattern;

    protected FilterPushdownRule(PlannerContext plannerContext, boolean useTableProperties, boolean dynamicFiltering, Pattern<T> sourcePattern)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.useTableProperties = useTableProperties;
        this.dynamicFiltering = dynamicFiltering;
        pattern = filter().with(source().matching(sourcePattern.capturedAs(sourceCapture)));
    }

    @Override
    public final Pattern<FilterNode> getPattern()
    {
        return pattern;
    }

    @Override
    public final Result apply(FilterNode node, Captures captures, Context context)
    {
        T source = captures.get(sourceCapture);
        Expression predicate = node.getPredicate();
        PlanNode result = pushDown(new Pushdown(context, plannerContext, useTableProperties, dynamicFiltering), source, predicate);
        if (result == node || result instanceof FilterNode filter &&
                filter.getPredicate().equals(predicate) && filter.getSource() == source) {
            return Result.empty();
        }
        return Result.ofPlanNode(result);
    }

    protected abstract PlanNode pushDown(Pushdown pushdown, T source, Expression predicate);
}
