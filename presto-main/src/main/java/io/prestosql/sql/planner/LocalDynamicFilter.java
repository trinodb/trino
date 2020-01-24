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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static io.prestosql.sql.DynamicFilters.Descriptor;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class LocalDynamicFilter
{
    private static final Logger log = Logger.get(LocalDynamicFilter.class);

    // Mapping from dynamic filter ID to its probe symbols.
    private final Multimap<String, Symbol> probeSymbols;

    // Mapping from dynamic filter ID to its build channel indices.
    private final Map<String, Integer> buildChannels;

    private final SettableFuture<TupleDomain<Symbol>> resultFuture;

    // The resulting predicate for local dynamic filtering.
    private TupleDomain<String> result;

    // Number of partitions left to be processed.
    private int partitionsLeft;

    public LocalDynamicFilter(Multimap<String, Symbol> probeSymbols, Map<String, Integer> buildChannels, int partitionCount)
    {
        this.probeSymbols = requireNonNull(probeSymbols, "probeSymbols is null");
        this.buildChannels = requireNonNull(buildChannels, "buildChannels is null");
        verify(probeSymbols.keySet().equals(buildChannels.keySet()), "probeSymbols and buildChannels must have same keys");

        this.resultFuture = SettableFuture.create();

        this.result = TupleDomain.none();
        this.partitionsLeft = partitionCount;
    }

    private synchronized void addPartition(TupleDomain<String> tupleDomain)
    {
        // Called concurrently by each DynamicFilterSourceOperator instance (when collection is over).
        partitionsLeft -= 1;
        verify(partitionsLeft >= 0);
        // NOTE: may result in a bit more relaxed constraint if there are multiple columns and multiple rows.
        // See the comment at TupleDomain::columnWiseUnion() for more details.
        result = TupleDomain.columnWiseUnion(result, tupleDomain);
        if (partitionsLeft == 0) {
            // No more partitions are left to be processed.
            verify(resultFuture.set(convertTupleDomain(result)), "dynamic filter result is provided more than once");
        }
    }

    private TupleDomain<Symbol> convertTupleDomain(TupleDomain<String> result)
    {
        if (result.isNone()) {
            return TupleDomain.none();
        }
        // Convert the predicate to use probe symbols (instead dynamic filter IDs).
        // Note that in case of a probe-side union, a single dynamic filter may match multiple probe symbols.
        ImmutableMap.Builder<Symbol, Domain> builder = ImmutableMap.builder();
        for (Map.Entry<String, Domain> entry : result.getDomains().get().entrySet()) {
            Domain domain = entry.getValue();
            // Store all matching symbols for each build channel index.
            for (Symbol probeSymbol : probeSymbols.get(entry.getKey())) {
                builder.put(probeSymbol, domain);
            }
        }
        return TupleDomain.withColumnDomains(builder.build());
    }

    public static Optional<LocalDynamicFilter> create(JoinNode planNode, int partitionCount)
    {
        Set<String> joinDynamicFilters = planNode.getDynamicFilters().keySet();
        List<FilterNode> filterNodes = PlanNodeSearcher
                .searchFrom(planNode.getLeft())
                .where(LocalDynamicFilter::isFilterAboveTableScan)
                .findAll();

        // Mapping from probe-side dynamic filters' IDs to their matching probe symbols.
        ImmutableMultimap.Builder<String, Symbol> probeSymbolsBuilder = ImmutableMultimap.builder();
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

        Multimap<String, Symbol> probeSymbols = probeSymbolsBuilder.build();
        PlanNode buildNode = planNode.getRight();
        Map<String, Integer> buildChannels = planNode.getDynamicFilters().entrySet().stream()
                // Skip build channels that don't match local probe dynamic filters.
                .filter(entry -> probeSymbols.containsKey(entry.getKey()))
                .collect(toMap(
                        // Dynamic filter ID
                        entry -> entry.getKey(),
                        // Build-side channel index
                        entry -> {
                            Symbol buildSymbol = entry.getValue();
                            int buildChannelIndex = buildNode.getOutputSymbols().indexOf(buildSymbol);
                            verify(buildChannelIndex >= 0);
                            return buildChannelIndex;
                        }));

        if (buildChannels.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LocalDynamicFilter(probeSymbols, buildChannels, partitionCount));
    }

    private static boolean isFilterAboveTableScan(PlanNode node)
    {
        if (node instanceof FilterNode) {
            if (((FilterNode) node).getSource() instanceof TableScanNode) {
                return true;
            }
        }
        return false;
    }

    public Map<String, Integer> getBuildChannels()
    {
        return buildChannels;
    }

    public ListenableFuture<TupleDomain<Symbol>> getResultFuture()
    {
        return resultFuture;
    }

    public Consumer<TupleDomain<String>> getTupleDomainConsumer()
    {
        return this::addPartition;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("probeSymbols", probeSymbols)
                .add("buildChannels", buildChannels)
                .add("result", result)
                .add("partitionsLeft", partitionsLeft)
                .toString();
    }
}
