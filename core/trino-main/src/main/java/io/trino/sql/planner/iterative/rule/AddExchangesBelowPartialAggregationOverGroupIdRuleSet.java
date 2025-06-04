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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.cost.SymbolStatsEstimate;
import io.trino.cost.TaskCountEstimator;
import io.trino.execution.TaskManagerConfig;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.StreamPreferredProperties;
import io.trino.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.GroupIdNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.getTaskConcurrency;
import static io.trino.SystemSessionProperties.isEnableForcedExchangeBelowGroupId;
import static io.trino.SystemSessionProperties.isEnableStatsCalculator;
import static io.trino.matching.Capture.newCapture;
import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.matching.Pattern.typeOf;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.optimizations.StreamPreferredProperties.fixedParallelism;
import static io.trino.sql.planner.optimizations.StreamPropertyDerivations.deriveProperties;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.partitionedExchange;
import static io.trino.sql.planner.plan.Patterns.Aggregation.groupingColumns;
import static io.trino.sql.planner.plan.Patterns.Aggregation.step;
import static io.trino.sql.planner.plan.Patterns.Exchange.scope;
import static io.trino.sql.planner.plan.Patterns.source;
import static java.lang.Double.isNaN;
import static java.lang.Math.min;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

/**
 * Transforms
 * <pre>
 *   - Exchange
 *     - [ Projection ]
 *       - Partial Aggregation
 *         - GroupId
 * </pre>
 * to
 * <pre>
 *   - Exchange
 *     - [ Projection ]
 *       - Partial Aggregation
 *         - GroupId
 *           - LocalExchange
 *             - RemoteExchange
 * </pre>
 * <p>
 * Rationale: GroupId increases number of rows (number of times equal to number of grouping sets) and then
 * partial aggregation reduces number of rows. However, under certain conditions, exchanging the rows before
 * GroupId (before multiplication) makes partial aggregation more effective, resulting in less data being
 * exchanged afterwards.
 */
public class AddExchangesBelowPartialAggregationOverGroupIdRuleSet
{
    private static final Capture<ProjectNode> PROJECTION = newCapture();
    private static final Capture<AggregationNode> AGGREGATION = newCapture();
    private static final Capture<GroupIdNode> GROUP_ID = newCapture();
    private static final Capture<ExchangeNode> REMOTE_EXCHANGE = newCapture();

    private static final Pattern<ExchangeNode> WITH_PROJECTION =
            // If there was no exchange here, adding new exchanges could break property derivations logic of AddExchanges, AddLocalExchanges
            typeOf(ExchangeNode.class)
                    .with(scope().equalTo(REMOTE)).capturedAs(REMOTE_EXCHANGE)
                    .with(source().matching(
                            // PushPartialAggregationThroughExchange adds a projection. However, it can be removed if RemoveRedundantIdentityProjections is run in the mean-time.
                            typeOf(ProjectNode.class).capturedAs(PROJECTION)
                                    .with(source().matching(
                                            typeOf(AggregationNode.class).capturedAs(AGGREGATION)
                                                    .with(step().equalTo(AggregationNode.Step.PARTIAL))
                                                    .with(nonEmpty(groupingColumns()))
                                                    .with(source().matching(
                                                            typeOf(GroupIdNode.class).capturedAs(GROUP_ID)))))));

    private static final Pattern<ExchangeNode> WITHOUT_PROJECTION =
            // If there was no exchange here, adding new exchanges could break property derivations logic of AddExchanges, AddLocalExchanges
            typeOf(ExchangeNode.class)
                    .with(scope().equalTo(REMOTE)).capturedAs(REMOTE_EXCHANGE)
                    .with(source().matching(
                            typeOf(AggregationNode.class).capturedAs(AGGREGATION)
                                    .with(step().equalTo(AggregationNode.Step.PARTIAL))
                                    .with(nonEmpty(groupingColumns()))
                                    .with(source().matching(
                                            typeOf(GroupIdNode.class).capturedAs(GROUP_ID)))));

    private static final double GROUPING_SETS_SYMBOL_REQUIRED_FREQUENCY = 0.5;
    private static final double ANTI_SKEWNESS_MARGIN = 3;

    private final PlannerContext plannerContext;
    private final TaskCountEstimator taskCountEstimator;
    private final DataSize maxPartialAggregationMemoryUsage;

    public AddExchangesBelowPartialAggregationOverGroupIdRuleSet(
            PlannerContext plannerContext,
            TaskCountEstimator taskCountEstimator,
            TaskManagerConfig taskManagerConfig)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
        this.maxPartialAggregationMemoryUsage = taskManagerConfig.getMaxPartialAggregationMemoryUsage();
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                belowProjectionRule(),
                belowExchangeRule());
    }

    @VisibleForTesting
    AddExchangesBelowExchangePartialAggregationGroupId belowExchangeRule()
    {
        return new AddExchangesBelowExchangePartialAggregationGroupId();
    }

    @VisibleForTesting
    AddExchangesBelowProjectionPartialAggregationGroupId belowProjectionRule()
    {
        return new AddExchangesBelowProjectionPartialAggregationGroupId();
    }

    private class AddExchangesBelowProjectionPartialAggregationGroupId
            extends BaseAddExchangesBelowExchangePartialAggregationGroupId
    {
        @Override
        public Pattern<ExchangeNode> getPattern()
        {
            return WITH_PROJECTION;
        }

        @Override
        public Result apply(ExchangeNode exchange, Captures captures, Context context)
        {
            ProjectNode project = captures.get(PROJECTION);
            AggregationNode aggregation = captures.get(AGGREGATION);
            GroupIdNode groupId = captures.get(GROUP_ID);
            ExchangeNode remoteExchange = captures.get(REMOTE_EXCHANGE);
            return transform(aggregation, groupId, remoteExchange.getPartitioningScheme().getPartitionCount(), context)
                    .map(newAggregation -> Result.ofPlanNode(
                            exchange.replaceChildren(ImmutableList.of(
                                    project.replaceChildren(ImmutableList.of(
                                            newAggregation))))))
                    .orElseGet(Result::empty);
        }
    }

    @VisibleForTesting
    class AddExchangesBelowExchangePartialAggregationGroupId
            extends BaseAddExchangesBelowExchangePartialAggregationGroupId
    {
        @Override
        public Pattern<ExchangeNode> getPattern()
        {
            return WITHOUT_PROJECTION;
        }

        @Override
        public Result apply(ExchangeNode exchange, Captures captures, Context context)
        {
            AggregationNode aggregation = captures.get(AGGREGATION);
            GroupIdNode groupId = captures.get(GROUP_ID);
            ExchangeNode remoteExchange = captures.get(REMOTE_EXCHANGE);
            return transform(aggregation, groupId, remoteExchange.getPartitioningScheme().getPartitionCount(), context)
                    .map(newAggregation -> {
                        PlanNode newExchange = exchange.replaceChildren(ImmutableList.of(newAggregation));
                        return Result.ofPlanNode(newExchange);
                    })
                    .orElseGet(Result::empty);
        }
    }

    private abstract class BaseAddExchangesBelowExchangePartialAggregationGroupId
            implements Rule<ExchangeNode>
    {
        @Override
        public boolean isEnabled(Session session)
        {
            if (!isEnableStatsCalculator(session)) {
                // Old stats calculator is not trust-worthy
                return false;
            }

            return isEnableForcedExchangeBelowGroupId(session);
        }

        protected Optional<PlanNode> transform(AggregationNode aggregation, GroupIdNode groupId, Optional<Integer> partitionCount, Context context)
        {
            if (groupId.getGroupingSets().size() < 2) {
                return Optional.empty();
            }

            Set<Symbol> groupingKeys = aggregation.getGroupingKeys().stream()
                    .filter(symbol -> !groupId.getGroupIdSymbol().equals(symbol))
                    .collect(toImmutableSet());

            Multiset<Symbol> groupingSetHistogram = groupId.getGroupingSets().stream()
                    .flatMap(Collection::stream)
                    .collect(toImmutableMultiset());

            if (!Objects.equals(groupingSetHistogram.elementSet(), groupingKeys)) {
                // TODO handle the case when some aggregation keys are pass-through in GroupId (e.g. common in all grouping sets). However, this is never the case for ROLLUP.
                // TODO handle the case when some grouping set symbols are not used in aggregation (possible?)
                return Optional.empty();
            }

            double aggregationMemoryRequirements = estimateAggregationMemoryRequirements(groupingKeys, groupId, groupingSetHistogram, context);
            if (isNaN(aggregationMemoryRequirements) || aggregationMemoryRequirements < maxPartialAggregationMemoryUsage.toBytes()) {
                // Aggregation will be effective even without exchanges (or we have insufficient information).
                return Optional.empty();
            }

            List<Symbol> desiredHashSymbols = groupingSetHistogram.entrySet().stream()
                    // Take only frequently used symbols
                    .filter(entry -> entry.getCount() >= groupId.getGroupingSets().size() * GROUPING_SETS_SYMBOL_REQUIRED_FREQUENCY)
                    .map(Multiset.Entry::getElement)
                    // And only the symbols used in the aggregation (these are usually all symbols)
                    .peek(symbol -> verify(groupingKeys.contains(symbol)))
                    // Transform to symbols before GroupId
                    .map(groupId.getGroupingColumns()::get)
                    .collect(toImmutableList());

            // Use only the symbol with the highest cardinality (if we have statistics). This makes partial aggregation more efficient in case of
            // low correlation between symbol that are in every grouping set vs additional symbols.
            PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(groupId.getSource());
            desiredHashSymbols = desiredHashSymbols.stream()
                    .filter(symbol -> !isNaN(sourceStats.getSymbolStatistics(symbol).getDistinctValuesCount()))
                    .max(comparing(symbol -> sourceStats.getSymbolStatistics(symbol).getDistinctValuesCount()))
                    .map(symbol -> (List<Symbol>) ImmutableList.of(symbol)).orElse(desiredHashSymbols);

            StreamPreferredProperties requiredProperties = fixedParallelism().withPartitioning(desiredHashSymbols);
            StreamProperties sourceProperties = derivePropertiesRecursively(groupId.getSource(), context);
            if (requiredProperties.isSatisfiedBy(sourceProperties)) {
                // Stream is already (locally) partitioned just as we want.
                // In fact, there might be just a LocalExchange below and no Remote. For now, we give up in this situation anyway. To properly support such situation:
                //  1. aggregation effectiveness estimation below need to consider the (helpful) fact that stream is already partitioned, so each operator will need less memory
                //  2. if the local exchange becomes unnecessary (after we add a remove on top of it), it should be removed. What if the local exchange is somewhere further
                //     down the tree?
                return Optional.empty();
            }

            double estimatedGroups = estimatedGroupCount(desiredHashSymbols, context.getStatsProvider().getStats(groupId.getSource()));
            if (isNaN(estimatedGroups) || estimatedGroups * ANTI_SKEWNESS_MARGIN < maximalConcurrencyAfterRepartition(context)) {
                // Desired hash symbols form too few groups. Hashing over them would harm concurrency.
                // TODO instead of taking symbols with >GROUPING_SETS_SYMBOL_REQUIRED_FREQUENCY presence, we could take symbols from high freq to low until there are enough groups
                return Optional.empty();
            }

            PlanNode source = groupId.getSource();

            // Above we only checked the data is not yet locally partitioned and it could be already globally partitioned (but not locally). TODO avoid remote exchange in this case
            // TODO If the aggregation memory requirements are only slightly above `maxPartialAggregationMemoryUsage`, adding only LocalExchange could be enough
            source = partitionedExchange(
                    context.getIdAllocator().getNextId(),
                    REMOTE,
                    source,
                    new PartitioningScheme(
                            Partitioning.create(FIXED_HASH_DISTRIBUTION, desiredHashSymbols),
                            source.getOutputSymbols(),
                            Optional.empty(),
                            false,
                            Optional.empty(),
                            // It's fine to reuse partitionCount since that is computed by considering all the expanding nodes and table scans in a query
                            partitionCount));

            source = partitionedExchange(
                    context.getIdAllocator().getNextId(),
                    LOCAL,
                    source,
                    new PartitioningScheme(
                            Partitioning.create(FIXED_HASH_DISTRIBUTION, desiredHashSymbols),
                            source.getOutputSymbols()));

            PlanNode newGroupId = groupId.replaceChildren(ImmutableList.of(source));
            PlanNode newAggregation = aggregation.replaceChildren(ImmutableList.of(newGroupId));

            return Optional.of(newAggregation);
        }

        private int maximalConcurrencyAfterRepartition(Context context)
        {
            return getTaskConcurrency(context.getSession()) * taskCountEstimator.estimateHashedTaskCount(context.getSession());
        }

        private double estimateAggregationMemoryRequirements(Set<Symbol> groupingKeys, GroupIdNode groupId, Multiset<Symbol> groupingSetHistogram, Context context)
        {
            checkArgument(Objects.equals(groupingSetHistogram.elementSet(), groupingKeys)); // Otherwise math below would be off-topic

            PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(groupId.getSource());
            double keysMemoryRequirements = 0;

            for (List<Symbol> groupingSet : groupId.getGroupingSets()) {
                List<Symbol> sourceSymbols = groupingSet.stream()
                        .map(groupId.getGroupingColumns()::get)
                        .collect(toImmutableList());

                double keyWidth = sourceStats.getOutputSizeInBytes(sourceSymbols) / sourceStats.getOutputRowCount();
                double keyNdv = min(estimatedGroupCount(sourceSymbols, sourceStats), sourceStats.getOutputRowCount());

                keysMemoryRequirements += keyWidth * keyNdv;
            }

            // TODO consider also memory requirements for aggregation values
            return keysMemoryRequirements;
        }

        private double estimatedGroupCount(List<Symbol> symbols, PlanNodeStatsEstimate statsEstimate)
        {
            return symbols.stream()
                    .map(statsEstimate::getSymbolStatistics)
                    .mapToDouble(this::ndvIncludingNull)
                    // This assumes no correlation, maximum number of aggregation keys
                    .reduce(1, (a, b) -> a * b);
        }

        private double ndvIncludingNull(SymbolStatsEstimate symbolStatsEstimate)
        {
            if (symbolStatsEstimate.getNullsFraction() == 0.) {
                return symbolStatsEstimate.getDistinctValuesCount();
            }
            return symbolStatsEstimate.getDistinctValuesCount() + 1;
        }

        private StreamProperties derivePropertiesRecursively(PlanNode node, Context context)
        {
            PlanNode resolvedPlanNode = context.getLookup().resolve(node);
            List<StreamProperties> inputProperties = resolvedPlanNode.getSources().stream()
                    .map(source -> derivePropertiesRecursively(source, context))
                    .collect(toImmutableList());
            return deriveProperties(resolvedPlanNode, inputProperties, plannerContext, context.getSession());
        }
    }
}
