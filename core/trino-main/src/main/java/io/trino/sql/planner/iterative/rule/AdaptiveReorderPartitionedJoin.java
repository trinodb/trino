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
import io.trino.sql.planner.optimizations.StreamPreferredProperties;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.SimplePlanRewriter;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionAdaptiveJoinReorderingMinSizeThreshold;
import static io.trino.SystemSessionProperties.getFaultTolerantExecutionAdaptiveJoinReorderingSizeDifferenceRatio;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.SystemSessionProperties.isFaultTolerantExecutionAdaptiveJoinReorderingEnabled;
import static io.trino.cost.PlanNodeStatsEstimateMath.getFirstKnownOutputSizeInBytes;
import static io.trino.operator.RetryPolicy.TASK;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.StreamPreferredProperties.partitionedOn;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.deriveStreamPropertiesWithoutActualProperties;
import static io.trino.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.trino.sql.planner.plan.ChildReplacer.replaceChildren;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.sql.planner.plan.ExchangeNode.partitionedExchange;
import static io.trino.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static io.trino.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.aggregation;
import static io.trino.sql.planner.plan.Patterns.exchange;
import static io.trino.sql.planner.plan.Patterns.join;
import static io.trino.sql.planner.plan.Patterns.source;
import static io.trino.sql.planner.plan.SimplePlanRewriter.rewriteWith;
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

        boolean flipJoin = flipJoinBasedOnStats(joinNode, context);
        if (flipJoin) {
            return Result.ofPlanNode(flipJoinAndFixLocalExchanges(joinNode, localExchangeNode.getId(), metadata, context));
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

    private static JoinNode flipJoinAndFixLocalExchanges(
            JoinNode joinNode,
            PlanNodeId buildSideLocalExchangeId,
            Metadata metadata,
            Context context)
    {
        JoinNode flippedJoinNode = joinNode.flipChildren();

        // Fix local exchange on probe side
        BuildToProbeLocalExchangeRewriter buildToProbeLocalExchangeRewriter = new BuildToProbeLocalExchangeRewriter(
                buildSideLocalExchangeId,
                context);
        PlanNode probeSide = rewriteWith(
                buildToProbeLocalExchangeRewriter,
                context.getLookup().resolve(flippedJoinNode.getLeft()));

        // Fix local exchange on build side
        PlanNode buildSide = flippedJoinNode.getRight();
        StreamProperties rightProperties = deriveStreamPropertiesRecursively(
                buildSide,
                metadata,
                context.getLookup(),
                context.getSession());
        List<Symbol> buildSymbols = Lists.transform(flippedJoinNode.getCriteria(), JoinNode.EquiJoinClause::getRight);
        StreamPreferredProperties expectedRightProperties = partitionedOn(buildSymbols);
        // Do not add local exchange if the partitioning properties are already satisfied
        if (!expectedRightProperties.isSatisfiedBy(rightProperties)) {
            ProbeToBuildLocalExchangeRewriter probeToBuildLocalExchangeRewriter = new ProbeToBuildLocalExchangeRewriter(
                    buildSymbols,
                    context);
            // Rewrite build side with local exchange
            buildSide = rewriteWith(probeToBuildLocalExchangeRewriter, context.getLookup().resolve(buildSide));
        }

        return new JoinNode(
                flippedJoinNode.getId(),
                flippedJoinNode.getType(),
                probeSide,
                buildSide,
                flippedJoinNode.getCriteria(),
                flippedJoinNode.getLeftOutputSymbols(),
                flippedJoinNode.getRightOutputSymbols(),
                flippedJoinNode.isMaySkipOutputDuplicates(),
                flippedJoinNode.getFilter(),
                flippedJoinNode.getLeftHashSymbol(),
                flippedJoinNode.getRightHashSymbol(),
                flippedJoinNode.getDistributionType(),
                flippedJoinNode.isSpillable(),
                flippedJoinNode.getDynamicFilters(),
                flippedJoinNode.getReorderJoinStatsAndCost());
    }

    private static boolean flipJoinBasedOnStats(JoinNode joinNode, Context context)
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
        return rightOutputSizeInBytes > minSizeThreshold.toBytes()
                && rightOutputSizeInBytes > sizeDifferenceRatio * leftOutputSizeInBytes;
    }

    private static StreamProperties deriveStreamPropertiesRecursively(PlanNode node, Metadata metadata, Lookup lookup, Session session)
    {
        PlanNode resolvedNode = lookup.resolve(node);
        List<StreamProperties> inputProperties = resolvedNode.getSources().stream()
                .map(source -> deriveStreamPropertiesRecursively(source, metadata, lookup, session))
                .collect(toImmutableList());
        return deriveStreamPropertiesWithoutActualProperties(resolvedNode, inputProperties, metadata, session);
    }

    private static class BuildToProbeLocalExchangeRewriter
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeId localExchangeNodeId;
        private final Context context;

        private BuildToProbeLocalExchangeRewriter(PlanNodeId localExchangeNodeId, Context context)
        {
            this.localExchangeNodeId = requireNonNull(localExchangeNodeId, "localExchangeNodeId is null");
            this.context = requireNonNull(context, "context is null");
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            throw new UnsupportedOperationException("Unexpected plan node: " + node.getClass().getSimpleName());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> ctx)
        {
            // Other than partial aggregation is not possible in this rule since pattern matches either
            // (partial aggregation + local exchange) or (local exchange) on build side
            verify(node.getStep() == PARTIAL, "Unexpected aggregation step: %s", node.getStep());
            // Skip partial aggregation and rewrite sources which contains build side local exchange
            return rewriteSources(this, node, context);
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> ctx)
        {
            verify(
                    node.getScope().equals(LOCAL) && node.getId().equals(localExchangeNodeId),
                    "Unexpected exchange node: %s", node.getId());
            // Remove local exchange if there is only one source since we are converting build side
            // to probe side
            if (node.getSources().size() == 1) {
                return node.getSources().getFirst();
            }

            // Add RoundRobinExchange to replace the partitioned local exchange if there are multiple sources
            return roundRobinExchange(
                    context.getIdAllocator().getNextId(),
                    LOCAL,
                    node.getSources(),
                    node.getOutputSymbols());
        }
    }

    private static class ProbeToBuildLocalExchangeRewriter
            extends SimplePlanRewriter<Void>
    {
        private final Context context;
        private final List<Symbol> buildSymbols;

        private ProbeToBuildLocalExchangeRewriter(List<Symbol> buildSymbols, Context context)
        {
            this.buildSymbols = requireNonNull(buildSymbols, "buildSymbols is null");
            this.context = requireNonNull(context, "context is null");
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Void> ctx)
        {
            // Add partitioned local exchange to the probe side which is now the build side since we have
            // flipped the join.
            return partitionedExchange(
                    context.getIdAllocator().getNextId(),
                    LOCAL,
                    node,
                    buildSymbols,
                    Optional.empty());
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> ctx)
        {
            // if there are multiple sources with round-robin exchange, replace it with partitioned exchange
            // instead of adding one.
            if (node.getScope().equals(LOCAL)
                    && node.getSources().size() > 1
                    && node.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION)) {
                return partitionedExchange(
                        context.getIdAllocator().getNextId(),
                        LOCAL,
                        node.getSources(),
                        buildSymbols,
                        node.getOutputSymbols());
            }
            return visitPlan(node, ctx);
        }
    }

    private static PlanNode rewriteSources(SimplePlanRewriter<Void> rewriter, PlanNode node, Context context)
    {
        ImmutableList.Builder<PlanNode> children = ImmutableList.builderWithExpectedSize(node.getSources().size());
        node.getSources().forEach(source -> children.add(rewriteWith(rewriter, context.getLookup().resolve(source))));
        return replaceChildren(node, children.build());
    }
}
