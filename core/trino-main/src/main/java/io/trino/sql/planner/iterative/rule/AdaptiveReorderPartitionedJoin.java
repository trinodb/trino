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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Lookup;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanNodeSearcher;
import io.trino.sql.planner.optimizations.StreamPreferredProperties;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionAdaptiveJoinReorderingMinSizeThreshold;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionAdaptiveJoinReorderingSizeDifferenceRatio;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isFaultTolerantExecutionAdaptiveJoinReorderingEnabled;
import static io.trino.cost.PlanNodeStatsEstimateMath.getFirstKnownOutputSizeInBytes;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.sql.planner.optimizations.StreamPreferredProperties.partitionedOn;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.deriveStreamPropertiesWithoutActualProperties;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.partitionedExchange;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Flip sides for partitioned join based on data size stats. This rule is only used during Adaptive Planning in FTE.
 * From:
 * <pre>
 *    Join (PARTITIONED)
 *      - left side (partitioned)
 *      - Partial Aggregation (optional)
 *          - Exchange (LOCAL)
 *              - right side (partitioned)
 * </pre>
 * TO:
 * <pre>
 *    Join (PARTITIONED)
 *      - Partial Aggregation (optional)
 *          - right side (partitioned)
 *      - Exchange (LOCAL) (if needed, derived using source stream properties)
 *          - left side (partitioned)
 * </pre>
 */
public class AdaptiveReorderPartitionedJoin
        implements Rule<JoinNode>
{
    private static final Capture<ExchangeNode> LOCAL_EXCHANGE_NODE = Capture.newCapture();
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(AdaptiveReorderPartitionedJoin::isPartitionedJoinWithNoHashSymbols)
            .or(
                    // In case partial aggregation is missing
                    prev -> prev.with(right().matching(
                            exchange().matching(exchangeNode -> exchangeNode.getScope().equals(LOCAL)
                                            // Skip when exchange is gather
                                            && !exchangeNode.getType().equals(GATHER))
                                    .capturedAs(LOCAL_EXCHANGE_NODE))),
                    // In case partial aggregation is present
                    prev -> prev.with(right().matching(
                            aggregation().matching(node -> node.getStep() == PARTIAL)
                                    .with(source().matching(
                                            exchange().matching(exchangeNode -> exchangeNode.getScope().equals(LOCAL)
                                                            // Skip when exchange is gather
                                                            && !exchangeNode.getType().equals(GATHER))
                                                    .capturedAs(LOCAL_EXCHANGE_NODE))))));

    private final Metadata metadata;

    public AdaptiveReorderPartitionedJoin(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    private static boolean isPartitionedJoinWithNoHashSymbols(JoinNode joinNode)
    {
        // Check if the join is partitioned and does not have hash symbols
        return joinNode.getDistributionType().equals(Optional.of(PARTITIONED))
                // TODO: Add support for hash symbols. For now, it's not important since hash optimization
                //  is disabled by default
                && joinNode.getRightHashSymbol().isEmpty()
                && joinNode.getLeftHashSymbol().isEmpty();
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        // This rule is only enabled in case of FTE
        return getRetryPolicy(session) == TASK && isFaultTolerantExecutionAdaptiveJoinReorderingEnabled(session);
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        ExchangeNode localExchangeNode = captures.get(LOCAL_EXCHANGE_NODE);
        // Check if the captured exchange node is the build side local exchange node, otherwise bail out
        List<Symbol> buildSymbols = Lists.transform(joinNode.getCriteria(), JoinNode.EquiJoinClause::getRight);
        if (!isBuildSideLocalExchangeNode(localExchangeNode, ImmutableSet.copyOf(buildSymbols))) {
            return Result.empty();
        }

        // Remove local exchange from build side
        JoinNode joinNodeWithoutExchanges = removeLocalExchangeFromBuildSide(joinNode, localExchangeNode, context);
        JoinNode flippedJoin = flipJoinBasedOnStats(joinNodeWithoutExchanges, context);
        if (flippedJoin != joinNodeWithoutExchanges) {
            // Add local exchange back to build side after flipping
            return Result.ofPlanNode(addLocalExchangeToBuildSideIfNeeded(flippedJoin, metadata, context));
        }
        return Result.empty();
    }

    private static boolean isBuildSideLocalExchangeNode(ExchangeNode exchangeNode, Set<Symbol> rightSymbols)
    {
        return exchangeNode.getScope() == LOCAL
                && exchangeNode.getPartitioningScheme().getPartitioning().getColumns().equals(rightSymbols)
                // TODO: Add support for local exchange with hash symbols. For now, it's not important since hash
                //  optimization is disabled by default
                && exchangeNode.getPartitioningScheme().getHashColumn().isEmpty();
    }

    private static JoinNode removeLocalExchangeFromBuildSide(JoinNode joinNode, ExchangeNode localExchangeNode, Context context)
    {
        PlanNode rightNodeWithoutLocalExchange = PlanNodeSearcher
                .searchFrom(joinNode.getRight(), context.getLookup())
                .where(node -> node.getId().equals(localExchangeNode.getId()))
                .removeFirst();

        return new JoinNode(
                joinNode.getId(),
                joinNode.getType(),
                joinNode.getLeft(),
                rightNodeWithoutLocalExchange,
                joinNode.getCriteria(),
                joinNode.getLeftOutputSymbols(),
                joinNode.getRightOutputSymbols(),
                joinNode.isMaySkipOutputDuplicates(),
                joinNode.getFilter(),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType(),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters(),
                joinNode.getReorderJoinStatsAndCost());
    }

    private static JoinNode addLocalExchangeToBuildSideIfNeeded(JoinNode joinNode, Metadata metadata, Context context)
    {
        StreamProperties rightProperties = deriveStreamPropertiesRecursively(
                joinNode.getRight(),
                metadata,
                context.getLookup(),
                context.getSession());
        List<Symbol> buildSymbols = Lists.transform(joinNode.getCriteria(), JoinNode.EquiJoinClause::getRight);
        StreamPreferredProperties expectedRightProperties = partitionedOn(buildSymbols);
        if (expectedRightProperties.isSatisfiedBy(rightProperties)) {
            return joinNode;
        }

        // Add local exchange to build side
        ExchangeNode exchangeNode = partitionedExchange(
                context.getIdAllocator().getNextId(),
                LOCAL,
                joinNode.getRight(),
                buildSymbols,
                joinNode.getRightHashSymbol());
        return new JoinNode(
                joinNode.getId(),
                joinNode.getType(),
                joinNode.getLeft(),
                exchangeNode,
                joinNode.getCriteria(),
                joinNode.getLeftOutputSymbols(),
                joinNode.getRightOutputSymbols(),
                joinNode.isMaySkipOutputDuplicates(),
                joinNode.getFilter(),
                joinNode.getLeftHashSymbol(),
                joinNode.getRightHashSymbol(),
                joinNode.getDistributionType(),
                joinNode.isSpillable(),
                joinNode.getDynamicFilters(),
                joinNode.getReorderJoinStatsAndCost());
    }

    private static JoinNode flipJoinBasedOnStats(JoinNode joinNode, Context context)
    {
        double leftOutputSizeInBytes = getFirstKnownOutputSizeInBytes(
                joinNode.getLeft(),
                context.getLookup(),
                context.getStatsProvider());
        double rightOutputSizeInBytes = getFirstKnownOutputSizeInBytes(
                joinNode.getRight(),
                context.getLookup(),
                context.getStatsProvider());
        DataSize minSizeThreshold = getFaultTolerantExecutionAdaptiveJoinReorderingMinSizeThreshold(context.getSession());
        double sizeDifferenceRatio = getFaultTolerantExecutionAdaptiveJoinReorderingSizeDifferenceRatio(context.getSession());
        if (rightOutputSizeInBytes > minSizeThreshold.toBytes()
                && rightOutputSizeInBytes > sizeDifferenceRatio * leftOutputSizeInBytes) {
            return joinNode.flipChildren();
        }
        return joinNode;
    }

    private static StreamProperties deriveStreamPropertiesRecursively(PlanNode node, Metadata metadata, Lookup lookup, Session session)
    {
        PlanNode resolvedNode = lookup.resolve(node);
        List<StreamProperties> inputProperties = resolvedNode.getSources().stream()
                .map(source -> deriveStreamPropertiesRecursively(source, metadata, lookup, session))
                .collect(toImmutableList());
        return deriveStreamPropertiesWithoutActualProperties(resolvedNode, inputProperties, metadata, session);
    }
}
