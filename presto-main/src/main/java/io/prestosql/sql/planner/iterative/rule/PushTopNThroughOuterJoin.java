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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TopNNode;

import java.util.List;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.optimizations.QueryCardinalityUtil.extractCardinality;
import static io.prestosql.sql.planner.plan.JoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.prestosql.sql.planner.plan.Patterns.Join.type;
import static io.prestosql.sql.planner.plan.Patterns.TopN.step;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.topN;
import static io.prestosql.sql.planner.plan.TopNNode.Step.PARTIAL;

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

        List<Symbol> orderBySymbols = parent.getOrderingScheme().getOrderBy();

        PlanNode left = joinNode.getLeft();
        PlanNode right = joinNode.getRight();
        JoinNode.Type type = joinNode.getType();

        if ((type == LEFT)
                && ImmutableSet.copyOf(left.getOutputSymbols()).containsAll(orderBySymbols)
                && !isLimited(left, context.getLookup(), parent.getCount())) {
            return Result.ofPlanNode(
                    joinNode.replaceChildren(ImmutableList.of(
                            parent.replaceChildren(ImmutableList.of(left)),
                            right)));
        }

        if ((type == RIGHT)
                && ImmutableSet.copyOf(right.getOutputSymbols()).containsAll(orderBySymbols)
                && !isLimited(right, context.getLookup(), parent.getCount())) {
            return Result.ofPlanNode(
                    joinNode.replaceChildren(ImmutableList.of(
                            left,
                            parent.replaceChildren(ImmutableList.of(right)))));
        }

        return Result.empty();
    }

    private static boolean isLimited(PlanNode node, Lookup lookup, long limit)
    {
        Range<Long> cardinality = extractCardinality(node, lookup);
        return cardinality.hasUpperBound() && cardinality.upperEndpoint() <= limit;
    }
}
