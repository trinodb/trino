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

import com.google.common.collect.ImmutableList;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.SemiJoinNode;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.semiJoin;
import static io.trino.sql.planner.plan.Patterns.source;

public class PushLimitThroughSemiJoin
        implements Rule<LimitNode>
{
    private static final Capture<SemiJoinNode> CHILD = newCapture();

    // Applies to both limit with ties and limit without ties.
    private static final Pattern<LimitNode> PATTERN = limit()
            .with(source().matching(semiJoin().capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        SemiJoinNode semiJoinNode = captures.get(CHILD);

        if (parent.isWithTies()) {
            // do not push down Limit if ties depend on symbol produced by SemiJoin
            if (parent.getTiesResolvingScheme().get().getOrderBy().contains(semiJoinNode.getSemiJoinOutput())) {
                return Result.empty();
            }
        }
        // Do not push down Limit with pre-sorted inputs if input ordering depends on symbol produced by SemiJoin
        if (parent.getPreSortedInputs().contains(semiJoinNode.getSemiJoinOutput())) {
            return Result.empty();
        }
        return Result.ofPlanNode(
                semiJoinNode.replaceChildren(ImmutableList.of(
                        parent.replaceChildren(ImmutableList.of(semiJoinNode.getSource())),
                        semiJoinNode.getFilteringSource())));
    }
}
