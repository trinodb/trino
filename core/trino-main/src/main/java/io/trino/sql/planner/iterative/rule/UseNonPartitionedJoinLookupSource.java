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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.cost.StatsProvider;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.operator.join.LookupJoinOperator;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.planner.plan.ValuesNode;

import java.util.List;
import java.util.Optional;

import static io.trino.SystemSessionProperties.getJoinPartitionedBuildMinRowCount;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.join;

/**
 * Rule that transforms
 * <pre>
 *     join
 *       probe
 *       build
 *         localExchange(partitioned)
 * </pre>
 * into:
 * <pre>
 *     join
 *       probe
 *       build
 *         localExchange(gather)
 * </pre>
 * for small build sides.
 * Avoiding partitioned exchange on the probe side improves {@link LookupJoinOperator} performance.
 */
public class UseNonPartitionedJoinLookupSource
        implements Rule<JoinNode>
{
    private static final Capture<ExchangeNode> RIGHT_EXCHANGE_NODE = Capture.newCapture();
    private static final Pattern<JoinNode> JOIN_PATTERN = join()
            .with(right().matching(exchange()
                    .matching(UseNonPartitionedJoinLookupSource::canBeTranslatedToLocalGather)
                    .capturedAs(RIGHT_EXCHANGE_NODE)));

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return JOIN_PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getJoinPartitionedBuildMinRowCount(session) > 0;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        double buildSideRowCount = getSourceTablesRowCount(node.getRight(), context);
        if (Double.isNaN(buildSideRowCount)) {
            // buildSideRowCount = NaN means stats are not available or build side contains join
            return Result.empty();
        }
        if (buildSideRowCount >= getJoinPartitionedBuildMinRowCount(context.getSession())) {
            // build side has too many rows
            return Result.empty();
        }

        ExchangeNode rightSideExchange = captures.get(RIGHT_EXCHANGE_NODE);
        ExchangeNode singleThreadedExchange = toGatheringExchange(rightSideExchange);
        return Result.ofPlanNode(node.replaceChildren(ImmutableList.of(node.getLeft(), singleThreadedExchange)));
    }

    private static ExchangeNode toGatheringExchange(ExchangeNode exchangeNode)
    {
        return new ExchangeNode(
                exchangeNode.getId(),
                GATHER,
                LOCAL,
                new PartitioningScheme(
                        Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                        exchangeNode.getPartitioningScheme().getOutputLayout()),
                exchangeNode.getSources(),
                exchangeNode.getInputs(),
                Optional.empty());
    }

    private static boolean canBeTranslatedToLocalGather(ExchangeNode exchangeNode)
    {
        return exchangeNode.getScope() == LOCAL
                && !isSingleGather(exchangeNode)
                && exchangeNode.getOrderingScheme().isEmpty()
                && exchangeNode.getPartitioningScheme().getBucketToPartition().isEmpty()
                && !exchangeNode.getPartitioningScheme().isReplicateNullsAndAny();
    }

    private static boolean isSingleGather(ExchangeNode exchangeNode)
    {
        return exchangeNode.getType() == GATHER
                && exchangeNode.getPartitioningScheme().getPartitioning().getHandle() == SINGLE_DISTRIBUTION;
    }

    private static double getSourceTablesRowCount(PlanNode node, Context context)
    {
        return getSourceTablesRowCount(node, context.getLookup(), context.getStatsProvider());
    }

    @VisibleForTesting
    static double getSourceTablesRowCount(PlanNode node, Lookup lookup, StatsProvider statsProvider)
    {
        boolean hasExpandingNodes = PlanNodeSearcher.searchFrom(node, lookup)
                .whereIsInstanceOfAny(JoinNode.class, UnnestNode.class)
                .matches();
        if (hasExpandingNodes) {
            return Double.NaN;
        }

        List<PlanNode> sourceNodes = PlanNodeSearcher.searchFrom(node, lookup)
                .whereIsInstanceOfAny(TableScanNode.class, ValuesNode.class)
                .findAll();

        return sourceNodes.stream()
                .mapToDouble(sourceNode -> statsProvider.getStats(sourceNode).getOutputRowCount())
                .sum();
    }
}
