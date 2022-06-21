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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class LocalDynamicFilterConsumer
        implements DynamicFilterSourceConsumer
{
    private static final int PARTITION_COUNT_INITIAL_VALUE = -1;
    // Mapping from dynamic filter ID to its build channel indices.
    private final Map<DynamicFilterId, Integer> buildChannels;

    // Mapping from dynamic filter ID to its build channel type.
    private final Map<DynamicFilterId, Type> filterBuildTypes;

    private final List<Consumer<Map<DynamicFilterId, Domain>>> collectors;

    // Number of build-side partitions to be collected, must be provided by setPartitionCount
    @GuardedBy("this")
    private int expectedPartitionCount = PARTITION_COUNT_INITIAL_VALUE;

    @GuardedBy("this")
    private boolean collected;

    // The resulting predicates from each build-side partition.
    @Nullable
    @GuardedBy("this")
    private List<TupleDomain<DynamicFilterId>> partitions;

    public LocalDynamicFilterConsumer(
            Map<DynamicFilterId, Integer> buildChannels,
            Map<DynamicFilterId, Type> filterBuildTypes,
            List<Consumer<Map<DynamicFilterId, Domain>>> collectors)
    {
        this.buildChannels = requireNonNull(buildChannels, "buildChannels is null");
        this.filterBuildTypes = requireNonNull(filterBuildTypes, "filterBuildTypes is null");
        verify(buildChannels.keySet().equals(filterBuildTypes.keySet()), "filterBuildTypes and buildChannels must have same keys");

        requireNonNull(collectors, "collectors is null");
        checkArgument(!collectors.isEmpty(), "collectors is empty");
        this.collectors = collectors;
        this.partitions = new ArrayList<>();
    }

    @Override
    public void addPartition(TupleDomain<DynamicFilterId> tupleDomain)
    {
        TupleDomain<DynamicFilterId> result;
        synchronized (this) {
            if (collected) {
                return;
            }
            requireNonNull(partitions, "partitions is null");
            // Called concurrently by each DynamicFilterSourceOperator instance (when collection is over).
            verify(expectedPartitionCount == PARTITION_COUNT_INITIAL_VALUE || partitions.size() < expectedPartitionCount);
            // NOTE: may result in a bit more relaxed constraint if there are multiple columns and multiple rows.
            // See the comment at TupleDomain::columnWiseUnion() for more details.
            partitions.add(tupleDomain);
            if (tupleDomain.isAll()) {
                result = tupleDomain;
            }
            else if (partitions.size() == expectedPartitionCount) {
                // No more partitions are left to be processed.
                if (partitions.isEmpty()) {
                    result = TupleDomain.none();
                }
                else {
                    result = TupleDomain.columnWiseUnion(partitions);
                }
            }
            else {
                return;
            }
            collected = true;
            partitions = null;
        }

        notifyConsumers(result);
    }

    @Override
    public void setPartitionCount(int partitionCount)
    {
        TupleDomain<DynamicFilterId> result;
        synchronized (this) {
            if (collected) {
                return;
            }
            checkState(expectedPartitionCount == PARTITION_COUNT_INITIAL_VALUE, "setPartitionCount should be called only once");
            requireNonNull(partitions, "partitions is null");
            expectedPartitionCount = partitionCount;
            if (partitions.size() == expectedPartitionCount) {
                // No more partitions are left to be processed.
                if (partitions.isEmpty()) {
                    result = TupleDomain.none();
                }
                else {
                    result = TupleDomain.columnWiseUnion(partitions);
                }
                collected = true;
                partitions = null;
            }
            else {
                return;
            }
        }

        notifyConsumers(result);
    }

    private void notifyConsumers(TupleDomain<DynamicFilterId> result)
    {
        requireNonNull(result, "result is null");
        Map<DynamicFilterId, Domain> dynamicFilterDomains = convertTupleDomain(result);
        collectors.forEach(consumer -> consumer.accept(dynamicFilterDomains));
    }

    private Map<DynamicFilterId, Domain> convertTupleDomain(TupleDomain<DynamicFilterId> result)
    {
        if (result.isNone()) {
            // One of the join build symbols has no non-null values, therefore no filters can match predicate
            return buildChannels.keySet().stream()
                    .collect(toImmutableMap(identity(), filterId -> Domain.none(filterBuildTypes.get(filterId))));
        }

        Map<DynamicFilterId, Domain> domains = new HashMap<>(result.getDomains().get());
        // Add `all` domain explicitly for dynamic filters to notify dynamic filter listeners
        buildChannels.keySet().forEach(filterId -> domains.putIfAbsent(filterId, Domain.all(filterBuildTypes.get(filterId))));
        return ImmutableMap.copyOf(domains);
    }

    public static LocalDynamicFilterConsumer create(
            JoinNode planNode,
            List<Type> buildSourceTypes,
            Set<DynamicFilterId> collectedFilters,
            List<Consumer<Map<DynamicFilterId, Domain>>> collectors)
    {
        checkArgument(!planNode.getDynamicFilters().isEmpty(), "Join node dynamicFilters is empty.");
        checkArgument(!collectedFilters.isEmpty(), "Collected dynamic filters set is empty");
        checkArgument(planNode.getDynamicFilters().keySet().containsAll(collectedFilters), "Collected dynamic filters set is not subset of join dynamic filters");

        PlanNode buildNode = planNode.getRight();
        Map<DynamicFilterId, Integer> buildChannels = planNode.getDynamicFilters().entrySet().stream()
                .filter(entry -> collectedFilters.contains(entry.getKey()))
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
        return new LocalDynamicFilterConsumer(buildChannels, filterBuildTypes, collectors);
    }

    public Map<DynamicFilterId, Integer> getBuildChannels()
    {
        return buildChannels;
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("buildChannels", buildChannels)
                .add("expectedPartitionCount", expectedPartitionCount)
                .add("collected", collected)
                .add("partitions", partitions)
                .toString();
    }
}
