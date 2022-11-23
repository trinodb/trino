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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.DynamicFilterId;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.predicate.TupleDomain.columnWiseUnion;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class LocalDynamicFilterConsumer
        implements DynamicFilterSourceConsumer
{
    // Mapping from dynamic filter ID to its build channel indices.
    private final Map<DynamicFilterId, Integer> buildChannels;
    // Mapping from dynamic filter ID to its build channel type.
    private final Map<DynamicFilterId, Type> filterBuildTypes;
    private final List<Consumer<Map<DynamicFilterId, Domain>>> collectors;
    private final long domainSizeLimitInBytes;

    // Number of build-side partitions to be collected, must be provided by setPartitionCount
    @GuardedBy("this")
    private Integer expectedPartitionCount;
    @GuardedBy("this")
    private int collectedPartitionCount;
    @GuardedBy("this")
    private volatile boolean collected;

    private final Queue<TupleDomain<DynamicFilterId>> summaryDomains = new ConcurrentLinkedQueue<>();
    private final AtomicLong summaryDomainsRetainedSizeInBytes = new AtomicLong();

    public LocalDynamicFilterConsumer(Map<DynamicFilterId, Integer> buildChannels, Map<DynamicFilterId, Type> filterBuildTypes, List<Consumer<Map<DynamicFilterId, Domain>>> collectors, DataSize domainSizeLimit)
    {
        this.buildChannels = requireNonNull(buildChannels, "buildChannels is null");
        this.filterBuildTypes = requireNonNull(filterBuildTypes, "filterBuildTypes is null");
        verify(buildChannels.keySet().equals(filterBuildTypes.keySet()), "filterBuildTypes and buildChannels must have same keys");
        requireNonNull(collectors, "collectors is null");
        checkArgument(!collectors.isEmpty(), "collectors is empty");
        this.collectors = ImmutableList.copyOf(collectors);
        this.domainSizeLimitInBytes = domainSizeLimit.toBytes();
    }

    @Override
    public void addPartition(TupleDomain<DynamicFilterId> domain)
    {
        if (collected) {
            return;
        }

        long domainRetainedSizeInBytes = getRetainedSizeInBytes(domain);
        summaryDomainsRetainedSizeInBytes.addAndGet(domainRetainedSizeInBytes);
        summaryDomains.add(domain);
        // Operators collecting dynamic filters tend to finish all at the same time
        // when filters are collected right before the HashBuilderOperator.
        // To avoid multiple task executor threads being blocked on waiting
        // for each other when collecting the filters run the heavy union operation
        // outside the lock.
        unionSummaryDomainsIfNecessary(false);

        TupleDomain<DynamicFilterId> result;
        synchronized (this) {
            verify(expectedPartitionCount == null || collectedPartitionCount < expectedPartitionCount);

            if (collected) {
                clearSummaryDomains();
                return;
            }
            collectedPartitionCount++;

            boolean allPartitionsCollected = expectedPartitionCount != null && collectedPartitionCount == expectedPartitionCount;
            if (allPartitionsCollected) {
                // run final compaction as previous concurrent compactions may have left more than a single domain
                unionSummaryDomainsIfNecessary(true);
            }

            boolean sizeLimitExceeded = false;
            TupleDomain<DynamicFilterId> summary = summaryDomains.poll();
            // summary can be null as another concurrent summary compaction may be running
            if (summary != null) {
                long originalSize = getRetainedSizeInBytes(summary);
                if (originalSize > domainSizeLimitInBytes) {
                    summary = summary.simplify(1);
                }
                if (getRetainedSizeInBytes(summary) > domainSizeLimitInBytes) {
                    summaryDomainsRetainedSizeInBytes.addAndGet(-originalSize);
                    sizeLimitExceeded = true;
                }
                else {
                    summaryDomainsRetainedSizeInBytes.addAndGet(getRetainedSizeInBytes(summary) - originalSize);
                    summaryDomains.add(summary);
                }
            }

            if (!allPartitionsCollected && !sizeLimitExceeded && !domain.isAll()) {
                return;
            }

            if (sizeLimitExceeded || domain.isAll()) {
                clearSummaryDomains();
                result = TupleDomain.all();
            }
            else {
                verify(expectedPartitionCount != null && collectedPartitionCount == expectedPartitionCount);
                verify(summaryDomains.size() == 1);
                result = summaryDomains.poll();
                verify(result != null);
                long currentSize = summaryDomainsRetainedSizeInBytes.addAndGet(-getRetainedSizeInBytes(result));
                verify(currentSize == 0, "currentSize is expected to be zero: %s", currentSize);
            }
            collected = true;
        }

        collectors.forEach(collector -> collector.accept(convertTupleDomain(result)));
    }

    @Override
    public void setPartitionCount(int partitionCount)
    {
        TupleDomain<DynamicFilterId> result;
        synchronized (this) {
            if (collected) {
                return;
            }
            checkState(expectedPartitionCount == null, "setPartitionCount should be called only once");
            expectedPartitionCount = partitionCount;
            if (collectedPartitionCount < expectedPartitionCount) {
                return;
            }
            if (partitionCount == 0) {
                result = TupleDomain.none();
            }
            else {
                // run final compaction as previous concurrent compactions may have left more than a single domain
                unionSummaryDomainsIfNecessary(true);
                verify(summaryDomains.size() == 1);
                result = summaryDomains.poll();
                verify(result != null);
                long currentSize = summaryDomainsRetainedSizeInBytes.addAndGet(-getRetainedSizeInBytes(result));
                verify(currentSize == 0, "currentSize is expected to be zero: %s", currentSize);
            }
            collected = true;
        }

        collectors.forEach(collector -> collector.accept(convertTupleDomain(result)));
    }

    private void unionSummaryDomainsIfNecessary(boolean force)
    {
        if (summaryDomainsRetainedSizeInBytes.get() < domainSizeLimitInBytes && !force) {
            return;
        }

        List<TupleDomain<DynamicFilterId>> domains = new ArrayList<>();
        long domainsRetainedSizeInBytes = 0;
        while (true) {
            TupleDomain<DynamicFilterId> domain = summaryDomains.poll();
            if (domain == null) {
                break;
            }
            domains.add(domain);
            domainsRetainedSizeInBytes += getRetainedSizeInBytes(domain);
        }

        if (domains.isEmpty()) {
            return;
        }

        TupleDomain<DynamicFilterId> union = columnWiseUnion(domains);
        summaryDomainsRetainedSizeInBytes.addAndGet(getRetainedSizeInBytes(union) - domainsRetainedSizeInBytes);
        long currentSize = summaryDomainsRetainedSizeInBytes.get();
        verify(currentSize >= 0, "currentSize is expected to be greater than or equal to zero: %s", currentSize);
        summaryDomains.add(union);
    }

    @Override
    public synchronized boolean isDomainCollectionComplete()
    {
        return collected;
    }

    private void clearSummaryDomains()
    {
        long domainsRetainedSizeInBytes = 0;
        while (true) {
            TupleDomain<DynamicFilterId> domain = summaryDomains.poll();
            if (domain == null) {
                break;
            }
            domainsRetainedSizeInBytes += getRetainedSizeInBytes(domain);
        }
        summaryDomainsRetainedSizeInBytes.addAndGet(-domainsRetainedSizeInBytes);
        long currentSize = summaryDomainsRetainedSizeInBytes.get();
        verify(currentSize >= 0, "currentSize is expected to be greater than or equal to zero: %s", currentSize);
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
            List<Consumer<Map<DynamicFilterId, Domain>>> collectors,
            DataSize domainSizeLimit)
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
        return new LocalDynamicFilterConsumer(buildChannels, filterBuildTypes, collectors, domainSizeLimit);
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
                .add("filterBuildTypes", filterBuildTypes)
                .add("domainSizeLimitInBytes", domainSizeLimitInBytes)
                .add("expectedPartitionCount", expectedPartitionCount)
                .add("collectedPartitionCount", collectedPartitionCount)
                .add("collected", collected)
                .add("summaryDomains", summaryDomains)
                .add("summaryDomainsRetainedSizeInBytes", summaryDomainsRetainedSizeInBytes)
                .toString();
    }

    private static long getRetainedSizeInBytes(TupleDomain<DynamicFilterId> summary)
    {
        return summary.getRetainedSizeInBytes(DynamicFilterId::getRetainedSizeInBytes);
    }
}
