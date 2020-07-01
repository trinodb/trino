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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.planner.plan.DynamicFilterId;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.sql.DynamicFilters.Descriptor;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class LocalDynamicFilterConsumer
{
    private static final Logger log = Logger.get(LocalDynamicFilterConsumer.class);

    // Mapping from dynamic filter ID to its probe symbols.
    private final Multimap<DynamicFilterId, Symbol> probeSymbols;

    // Mapping from dynamic filter ID to its build channel indices.
    private final Map<DynamicFilterId, Integer> buildChannels;

    // Mapping from dynamic filter ID to its build channel type.
    private final Map<DynamicFilterId, Type> filterBuildTypes;

    private final SettableFuture<TupleDomain<DynamicFilterId>> resultFuture;

    // Number of build-side partitions to be collected.
    private final int partitionCount;

    // The resulting predicates from each build-side partition.
    private final List<TupleDomain<DynamicFilterId>> partitions;

    public LocalDynamicFilterConsumer(Multimap<DynamicFilterId, Symbol> probeSymbols, Map<DynamicFilterId, Integer> buildChannels, Map<DynamicFilterId, Type> filterBuildTypes, int partitionCount)
    {
        this.probeSymbols = requireNonNull(probeSymbols, "probeSymbols is null");
        this.buildChannels = requireNonNull(buildChannels, "buildChannels is null");
        verify(buildChannels.keySet().containsAll(probeSymbols.keySet()), "probeSymbols should be subset of buildChannels");

        this.filterBuildTypes = requireNonNull(filterBuildTypes, "filterBuildTypes is null");
        verify(buildChannels.keySet().equals(filterBuildTypes.keySet()), "filterBuildTypes and buildChannels must have same keys");

        this.resultFuture = SettableFuture.create();

        this.partitionCount = partitionCount;
        this.partitions = new ArrayList<>(partitionCount);
    }

    public ListenableFuture<Map<DynamicFilterId, Domain>> getDynamicFilterDomains()
    {
        return Futures.transform(resultFuture, this::convertTupleDomain, directExecutor());
    }

    public ListenableFuture<Map<Symbol, Domain>> getNodeLocalDynamicFilterForSymbols()
    {
        return Futures.transform(resultFuture, this::convertTupleDomainForLocalFilters, directExecutor());
    }

    private synchronized void addPartition(TupleDomain<DynamicFilterId> tupleDomain)
    {
        // Called concurrently by each DynamicFilterSourceOperator instance (when collection is over).
        verify(partitions.size() < partitionCount);
        // NOTE: may result in a bit more relaxed constraint if there are multiple columns and multiple rows.
        // See the comment at TupleDomain::columnWiseUnion() for more details.
        partitions.add(tupleDomain);
        if (partitions.size() == partitionCount) {
            TupleDomain<DynamicFilterId> result = TupleDomain.columnWiseUnion(partitions);
            // No more partitions are left to be processed.
            resultFuture.set(result);
        }
    }

    private Map<Symbol, Domain> convertTupleDomainForLocalFilters(TupleDomain<DynamicFilterId> result)
    {
        if (result.isNone()) {
            // One of the join build symbols has no non-null values, therefore no symbols can match predicate
            ImmutableMap.Builder<Symbol, Domain> builder = ImmutableMap.builder();
            for (Map.Entry<DynamicFilterId, Type> entry : filterBuildTypes.entrySet()) {
                // Store `none` domain explicitly for each probe symbol
                for (Symbol probeSymbol : probeSymbols.get(entry.getKey())) {
                    builder.put(probeSymbol, Domain.none(entry.getValue()));
                }
            }
            return builder.build();
        }
        // Convert the predicate to use probe symbols (instead dynamic filter IDs).
        // Note that in case of a probe-side union, a single dynamic filter may match multiple probe symbols.
        ImmutableMap.Builder<Symbol, Domain> builder = ImmutableMap.builder();
        for (Map.Entry<DynamicFilterId, Domain> entry : result.getDomains().get().entrySet()) {
            Domain domain = entry.getValue();
            // Store all matching symbols for each build channel index.
            for (Symbol probeSymbol : probeSymbols.get(entry.getKey())) {
                builder.put(probeSymbol, domain);
            }
        }
        return builder.build();
    }

    private Map<DynamicFilterId, Domain> convertTupleDomain(TupleDomain<DynamicFilterId> result)
    {
        if (result.isNone()) {
            // One of the join build symbols has no non-null values, therefore no filters can match predicate
            return buildChannels.keySet().stream()
                    .collect(toImmutableMap(identity(), filterId -> Domain.none(filterBuildTypes.get(filterId))));
        }
        return result.getDomains().get();
    }

    public static LocalDynamicFilterConsumer create(JoinNode planNode, List<Type> buildSourceTypes, int partitionCount)
    {
        checkArgument(!planNode.getDynamicFilters().isEmpty(), "Join node dynamicFilters is empty.");

        Set<DynamicFilterId> joinDynamicFilters = planNode.getDynamicFilters().keySet();
        List<FilterNode> filterNodes = PlanNodeSearcher
                .searchFrom(planNode.getLeft())
                .where(LocalDynamicFilterConsumer::isFilterAboveTableScan)
                .findAll();

        // Mapping from probe-side dynamic filters' IDs to their matching probe symbols.
        ImmutableMultimap.Builder<DynamicFilterId, Symbol> probeSymbolsBuilder = ImmutableMultimap.builder();
        for (FilterNode filterNode : filterNodes) {
            DynamicFilters.ExtractResult extractResult = extractDynamicFilters(filterNode.getPredicate());
            for (Descriptor descriptor : extractResult.getDynamicConjuncts()) {
                if (descriptor.getInput() instanceof SymbolReference) {
                    // Add descriptors that match the local dynamic filter (from the current join node).
                    if (joinDynamicFilters.contains(descriptor.getId())) {
                        Symbol probeSymbol = Symbol.from(descriptor.getInput());
                        log.debug("Adding dynamic filter %s: %s", descriptor, probeSymbol);
                        probeSymbolsBuilder.put(descriptor.getId(), probeSymbol);
                    }
                }
            }
        }

        Multimap<DynamicFilterId, Symbol> probeSymbols = probeSymbolsBuilder.build();
        PlanNode buildNode = planNode.getRight();
        // Collect dynamic filters for all dynamic filters produced by join
        Map<DynamicFilterId, Integer> buildChannels = planNode.getDynamicFilters().entrySet().stream()
                .collect(toImmutableMap(
                        // Dynamic filter ID
                        Map.Entry::getKey,
                        // Build-side channel index
                        entry -> {
                            Symbol buildSymbol = entry.getValue();
                            int buildChannelIndex = buildNode.getOutputSymbols().indexOf(buildSymbol);
                            verify(buildChannelIndex >= 0);
                            return buildChannelIndex;
                        }));

        Map<DynamicFilterId, Type> filterBuildTypes = buildChannels.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> buildSourceTypes.get(entry.getValue())));
        return new LocalDynamicFilterConsumer(probeSymbols, buildChannels, filterBuildTypes, partitionCount);
    }

    private static boolean isFilterAboveTableScan(PlanNode node)
    {
        return node instanceof FilterNode && ((FilterNode) node).getSource() instanceof TableScanNode;
    }

    public Map<DynamicFilterId, Integer> getBuildChannels()
    {
        return buildChannels;
    }

    public Consumer<TupleDomain<DynamicFilterId>> getTupleDomainConsumer()
    {
        return this::addPartition;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("probeSymbols", probeSymbols)
                .add("buildChannels", buildChannels)
                .add("partitionCount", partitionCount)
                .add("partitions", partitions)
                .toString();
    }
}
