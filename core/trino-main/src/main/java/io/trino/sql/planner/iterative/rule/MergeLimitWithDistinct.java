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
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.DistinctLimitNode;
import io.trino.sql.planner.plan.LimitNode;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.Limit.requiresPreSortedInputs;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.source;

public class MergeLimitWithDistinct
        implements Rule<LimitNode>
{
    private static final Capture<AggregationNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .matching(limit -> !limit.isWithTies())
            .with(requiresPreSortedInputs().equalTo(false))
            .with(source().matching(aggregation().capturedAs(CHILD).matching(AggregationNode::producesDistinctRows)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        AggregationNode child = captures.get(CHILD);

        return Result.ofPlanNode(
                new DistinctLimitNode(
                        parent.getId(),
                        child.getSource(),
                        parent.getCount(),
                        false,
                        child.getGroupingKeys(),
                        child.getHashSymbol()));
    }
}
