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
import com.google.common.collect.Range;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.LimitNode;
import io.prestosql.sql.planner.plan.PlanNode;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.prestosql.sql.planner.plan.Patterns.Join.type;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.limit;
import static io.prestosql.sql.planner.plan.Patterns.source;

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
 */
public class PushLimitThroughOuterJoin
        implements Rule<LimitNode>
{
    private static final Capture<JoinNode> CHILD = newCapture();

    private static final Pattern<LimitNode> PATTERN =
            limit()
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

        if (joinNode.getType() == LEFT && !isLimited(left, context.getLookup(), parent.getCount())) {
            return Result.ofPlanNode(
                    parent.replaceChildren(ImmutableList.of(
                            joinNode.replaceChildren(ImmutableList.of(
                                    new LimitNode(context.getIdAllocator().getNextId(), left, parent.getCount(), true),
                                    right)))));
        }

        if (joinNode.getType() == RIGHT && !isLimited(right, context.getLookup(), parent.getCount())) {
            return Result.ofPlanNode(
                    parent.replaceChildren(ImmutableList.of(
                            joinNode.replaceChildren(ImmutableList.of(
                                    left,
                                    new LimitNode(context.getIdAllocator().getNextId(), right, parent.getCount(), true))))));
        }

        return Result.empty();
    }

    private static boolean isLimited(PlanNode node, Lookup lookup, long limit)
    {
        Range<Long> cardinality = extractCardinality(node, lookup);
        return cardinality.hasUpperBound() && cardinality.upperEndpoint() <= limit;
    }
}
