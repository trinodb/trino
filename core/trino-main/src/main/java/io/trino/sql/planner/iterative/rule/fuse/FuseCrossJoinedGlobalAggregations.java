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
package io.trino.sql.planner.iterative.rule.fuse;

import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.JoinNode;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.SystemSessionProperties.isCombineSimilarSubPlans;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.plan.Patterns.Join.left;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.tree.BooleanLiteral.TRUE_LITERAL;

/**
 * Simplified "Computation Reuse via Fusion in Amazon Athena" paper JoinOnKeys rule
 * that works only on a single cross join over global aggregations over the same table.
 * It transforms
 * <pre>
 *   cross join
 *     aggregation global
 *       agg_left <- aggLeft()
 *         filter(filter_left)
 *           table scan(table)
 *     aggregation global
 *       agg_right <- aggRight()
 *         filter(filter_right)
 *           table scan(table)
 * </pre>
 * into:
 * <pre>
 *   aggregation global
 *       agg_left <- aggLeft(mask = agg_mask_left)
 *       agg_right <- aggRight(mask = agg_mask_right)
 *     project(agg_mask_left = filter_left, agg_mask_left = filter_right)
 *       filter(filter_left or filter_right)
 *         table scan
 * </pre>
 * <p>
 * This makes the plan to read the source table only once,
 * resulting in the potentially significant performance improvement.
 */
public class FuseCrossJoinedGlobalAggregations
        implements Rule<JoinNode>
{
    private static final Capture<AggregationNode> LEFT_AGGREGATION_NODE = newCapture();
    private static final Capture<AggregationNode> RIGHT_AGGREGATION_NODE = newCapture();
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(JoinNode::isCrossJoin)
            .with(left()
                    .matching(aggregation()
                            .matching(AggregationNode::hasSingleGlobalAggregation)
                            .capturedAs(LEFT_AGGREGATION_NODE)))
            .with(right()
                    .matching(aggregation()
                            .matching(AggregationNode::hasSingleGlobalAggregation)
                            .capturedAs(RIGHT_AGGREGATION_NODE)));

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isCombineSimilarSubPlans(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        AggregationNode left = captures.get(LEFT_AGGREGATION_NODE);
        AggregationNode right = captures.get(RIGHT_AGGREGATION_NODE);

        return PlanNodeFuser.fuse(context, left, right)
                .map(fused -> {
                    checkArgument(
                            fused.leftFilter().equals(TRUE_LITERAL) && fused.rightFilter().equals(TRUE_LITERAL),
                            "Expected both fused filter to be TRUE but got left %s, right %s",
                            fused.leftFilter(),
                            fused.rightFilter());
                    return Result.ofPlanNode(fused.plan());
                })
                .orElse(Result.empty());
    }
}
