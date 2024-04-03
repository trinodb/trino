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
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TopNNode;

import java.util.List;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.planner.plan.JoinType.RIGHT;
import static io.trino.sql.planner.plan.Patterns.Join.type;
import static io.trino.sql.planner.plan.Patterns.TopN.step;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.topN;
import static io.trino.sql.planner.plan.TopNNode.Step.PARTIAL;

/**
 * Transforms:
 * <pre>
 * - TopN (partial)
 *    - Join (left, right)
 *       - left source
 *       - right source
 * </pre>
 * Into:
 * <pre>
 * - Join
 *    - TopN (present if Join is left, not already limited, and orderBy symbols come from left source)
 *       - left source
 *    - TopN (present if Join is right, not already limited, and orderBy symbols come from right source)
 *       - right source
 * </pre>
 * <p>
 * TODO: this Rule violates the expectation that Rule transformations must preserve the semantics of the
 * expression subtree. It works only because it's a PARTIAL TopN, so there will be a FINAL TopN that "fixes" it.
 */
public class PushTopNThroughOuterJoin
        implements Rule<TopNNode>
{
    private static final Capture<JoinNode> JOIN_CHILD = newCapture();

    private static final Pattern<TopNNode> PATTERN =
            topN().with(step().equalTo(PARTIAL))
                    .with(source().matching(
                            join().capturedAs(JOIN_CHILD).with(type().matching(type -> type == LEFT || type == RIGHT))));

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode parent, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN_CHILD);

        List<Symbol> orderBySymbols = parent.getOrderingScheme().orderBy();

        PlanNode left = joinNode.getLeft();
        PlanNode right = joinNode.getRight();
        JoinType type = joinNode.getType();

        if ((type == LEFT)
                && ImmutableSet.copyOf(left.getOutputSymbols()).containsAll(orderBySymbols)
                && !isAtMost(left, context.getLookup(), parent.getCount())) {
            return Result.ofPlanNode(
                    joinNode.replaceChildren(ImmutableList.of(
                            parent.replaceChildren(ImmutableList.of(left)),
                            right)));
        }

        if ((type == RIGHT)
                && ImmutableSet.copyOf(right.getOutputSymbols()).containsAll(orderBySymbols)
                && !isAtMost(right, context.getLookup(), parent.getCount())) {
            return Result.ofPlanNode(
                    joinNode.replaceChildren(ImmutableList.of(
                            left,
                            parent.replaceChildren(ImmutableList.of(right)))));
        }

        return Result.empty();
    }
}
