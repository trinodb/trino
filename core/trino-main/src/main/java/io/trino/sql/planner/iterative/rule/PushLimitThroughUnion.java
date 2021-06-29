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
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.UnionNode;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static io.trino.sql.planner.plan.Patterns.Limit.requiresPreSortedInputs;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.union;

/**
 * Transforms:
 * <pre>
 * - Limit
 *    - Union
 *       - relation1
 *       - relation2
 *       ..
 * </pre>
 * Into:
 * <pre>
 * - Limit
 *    - Union
 *       - Limit
 *          - relation1
 *       - Limit
 *          - relation2
 *       ..
 * </pre>
 * Applies to LimitNode without ties only to avoid optimizer loop.
 */
public class PushLimitThroughUnion
        implements Rule<LimitNode>
{
    private static final Capture<UnionNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .matching(limit -> !limit.isWithTies())
            .with(requiresPreSortedInputs().equalTo(false))
            .with(source().matching(union().capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        UnionNode unionNode = captures.get(CHILD);
        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        boolean shouldApply = false;
        for (PlanNode source : unionNode.getSources()) {
            // This check is to ensure that we don't fire the optimizer if it was previously applied.
            if (isAtMost(source, context.getLookup(), parent.getCount())) {
                builder.add(source);
            }
            else {
                shouldApply = true;
                builder.add(new LimitNode(context.getIdAllocator().getNextId(), source, parent.getCount(), true));
            }
        }

        if (!shouldApply) {
            return Result.empty();
        }

        return Result.ofPlanNode(
                parent.replaceChildren(ImmutableList.of(
                        unionNode.replaceChildren(builder.build()))));
    }
}
