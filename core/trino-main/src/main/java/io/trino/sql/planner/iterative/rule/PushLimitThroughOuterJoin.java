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
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Optional;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static io.trino.sql.planner.plan.JoinNode.Type.LEFT;
import static io.trino.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.trino.sql.planner.plan.Patterns.Join.type;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.limit;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * Transforms:
 * <pre>
 * - Limit
 *    - Join
 *       - left source
 *       - right source
 * </pre>
 * Into:
 * <pre>
 * - Limit
 *    - Join
 *       - Limit (present if Join is left or outer)
 *          - left source
 *       - Limit (present if Join is right or outer)
 *          - right source
 * </pre>
 * Applies to LimitNode without ties only to avoid optimizer loop.
 */
public class PushLimitThroughOuterJoin
        implements Rule<LimitNode>
{
    private static final Capture<JoinNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .matching(limit -> !limit.isWithTies())
            .with(source().matching(
                    join()
                            .with(type().matching(type -> type == LEFT || type == RIGHT))
                            .capturedAs(CHILD)));

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode parent, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(CHILD);
        PlanNode left = joinNode.getLeft();
        PlanNode right = joinNode.getRight();

        if (joinNode.getType() == LEFT && !isAtMost(left, context.getLookup(), parent.getCount())) {
            if (!ImmutableSet.copyOf(left.getOutputSymbols()).containsAll(parent.getPreSortedInputs())) {
                return Result.empty();
            }
            return Result.ofPlanNode(
                    parent.replaceChildren(ImmutableList.of(
                            joinNode.replaceChildren(ImmutableList.of(
                                    new LimitNode(context.getIdAllocator().getNextId(), left, parent.getCount(), Optional.empty(), true, parent.getPreSortedInputs()),
                                    right)))));
        }

        if (joinNode.getType() == RIGHT && !isAtMost(right, context.getLookup(), parent.getCount())) {
            if (!ImmutableSet.copyOf(right.getOutputSymbols()).containsAll(parent.getPreSortedInputs())) {
                return Result.empty();
            }
            return Result.ofPlanNode(
                    parent.replaceChildren(ImmutableList.of(
                            joinNode.replaceChildren(ImmutableList.of(
                                    left,
                                    new LimitNode(context.getIdAllocator().getNextId(), right, parent.getCount(), Optional.empty(), true, parent.getPreSortedInputs()))))));
        }

        return Result.empty();
    }
}
