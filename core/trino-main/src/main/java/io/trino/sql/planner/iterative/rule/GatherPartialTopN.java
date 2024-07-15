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
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TopNNode;

import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.gatheringExchange;
import static io.trino.sql.planner.plan.ExchangeNode.partitionedExchange;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.Patterns.topN;
import static io.trino.sql.planner.plan.TopNNode.Step.PARTIAL;

/**
 * Adds local round-robin and gathering exchange on top of partial TopN to limit the task output size.
 * Replaces plans like:
 * <pre>
 * exchange(remote)
 *   - topn(partial)
 * </pre>
 * with
 * <pre>
 *  exchange(remote)
 *      - topn(partial)
 *          - exchange(local, gather)
 *              - topn(partial)
 *                  - local_exchange(round_robin)
 *                      - topn(partial)
 * </pre>
 */
public class GatherPartialTopN
        implements Rule<ExchangeNode>
{
    private static final Capture<TopNNode> TOPN = newCapture();
    // the pattern filters for parent and source exchanges are added to avoid infinite recursion in iterative optimizer
    private static final Pattern<ExchangeNode> PATTERN = exchange()
            .matching(GatherPartialTopN::isGatherRemoteExchange)
            .with(source().matching(
                    topN().matching(topN -> topN.getStep().equals(PARTIAL))
                            .with(source().matching(source -> !isGatherLocalExchange(source)))
                            .capturedAs(TOPN)));

    private static boolean isGatherLocalExchange(PlanNode source)
    {
        return source instanceof ExchangeNode exchange
               && exchange.getScope().equals(LOCAL)
               && exchange.getType().equals(GATHER);
    }

    private static boolean isGatherRemoteExchange(ExchangeNode exchangeNode)
    {
        return exchangeNode.getScope().equals(REMOTE)
               && exchangeNode.getType().equals(GATHER)
               // non-empty orderingScheme means it's a merging exchange
               && exchangeNode.getOrderingScheme().isEmpty();
    }

    @Override
    public Pattern<ExchangeNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ExchangeNode node, Captures captures, Context context)
    {
        TopNNode originalPartialTopN = captures.get(TOPN);

        TopNNode roundRobinTopN = new TopNNode(
                context.getIdAllocator().getNextId(),
                partitionedExchange(
                        context.getIdAllocator().getNextId(),
                        LOCAL,
                        originalPartialTopN,
                        new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), originalPartialTopN.getOutputSymbols())),
                originalPartialTopN.getCount(),
                originalPartialTopN.getOrderingScheme(),
                PARTIAL);

        return Result.ofPlanNode(node.replaceChildren(
                ImmutableList.of(new TopNNode(
                        context.getIdAllocator().getNextId(),
                        gatheringExchange(context.getIdAllocator().getNextId(), LOCAL, roundRobinTopN),
                        originalPartialTopN.getCount(),
                        originalPartialTopN.getOrderingScheme(),
                        PARTIAL))));
    }
}
