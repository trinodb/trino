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
import io.trino.sql.planner.plan.SortNode;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.Limit.requiresPreSortedInputs;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.sort;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * This rule removes SortNode being a source of LimitNode with ties.
 * LimitNode with ties performs sorting, so the SortNode is redundant.
 * <p>
 * Transforms:
 * <pre>
 * - LimitNode
 *      count: n
 *      tiesResolvingScheme: (x)
 *      - SortNode: order by (y)
 *           - source
 * </pre>
 * into:
 * <pre>
 * - LimitNode
 *  *      count: n
 *  *      tiesResolvingScheme: (x)
 *  *      - source
 * </pre>
 */
public class RemoveRedundantSortBelowLimitWithTies
        implements Rule<LimitNode>
{
    private static final Capture<SortNode> SORT = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .matching(LimitNode::isWithTies)
            .with(requiresPreSortedInputs().equalTo(false))
            .with(source().matching(sort().capturedAs(SORT)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode node, Captures captures, Context context)
    {
        SortNode sortNode = captures.get(SORT);
        return Result.ofPlanNode(node.replaceChildren(ImmutableList.of(sortNode.getSource())));
    }
}
