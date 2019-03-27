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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.plan.Patterns.SemiJoin.getFilteringSource;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.semiJoin;

/**
 * Remove the aggregation node that produces distinct rows from the Filtering source of a Semi join.
 */
public class RemoveAggregationInSemiJoin
        implements Rule<SemiJoinNode>
{
    private static final Capture<AggregationNode> CHILD = newCapture();

    private static final Pattern<SemiJoinNode> PATTERN = semiJoin()
            .with(getFilteringSource()
                    .matching(aggregation()
                            .capturedAs(CHILD).matching(AggregationNode::producesDistinctRows)));

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(SemiJoinNode semiJoinNode, Captures captures, Context context)
    {
        AggregationNode filteringSource = captures.get(CHILD);
        return Result.ofPlanNode(semiJoinNode
                .replaceChildren(ImmutableList.of(semiJoinNode.getSource(), getOnlyElement(filteringSource.getSources()))));
    }
}
