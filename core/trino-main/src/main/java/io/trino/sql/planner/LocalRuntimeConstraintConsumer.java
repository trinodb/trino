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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.units.DataSize;
import io.trino.operator.RuntimeConstraintSourceConsumer;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload.Lane;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static java.util.Objects.requireNonNull;

public final class LocalRuntimeConstraintConsumer
        implements RuntimeConstraintSourceConsumer
{
    private final List<Integer> buildChannels;
    private final List<Type> laneTypes;
    private final Consumer<RuntimeMembershipPayload> collector;
    private final long domainSizeLimitInBytes;

    @GuardedBy("this")
    private Integer expectedPartitionCount;
    @GuardedBy("this")
    private int collectedPartitionCount;
    @GuardedBy("this")
    private volatile boolean collected;

    private final Queue<List<Domain>> summaryDomains = new ConcurrentLinkedQueue<>();
    private final AtomicLong summaryDomainsRetainedSizeInBytes = new AtomicLong();
    private final AtomicBoolean sawInputRow = new AtomicBoolean();
    private final Set<Integer> lanesWithNull = new HashSet<>();

    public LocalRuntimeConstraintConsumer(
            List<Integer> buildChannels,
            List<Type> laneTypes,
            Consumer<RuntimeMembershipPayload> collector,
            DataSize domainSizeLimit)
    {
        this.buildChannels = ImmutableList.copyOf(requireNonNull(buildChannels, "buildChannels is null"));
        this.laneTypes = ImmutableList.copyOf(requireNonNull(laneTypes, "laneTypes is null"));
        checkArgument(!this.buildChannels.isEmpty(), "buildChannels is empty");
        checkArgument(this.buildChannels.size() == this.laneTypes.size(), "buildChannels and laneTypes sizes differ");
        checkArgument(this.buildChannels.stream().distinct().count() == this.buildChannels.size(), "duplicate build channels are not allowed");
        this.collector = requireNonNull(collector, "collector is null");
        this.domainSizeLimitInBytes = requireNonNull(domainSizeLimit, "domainSizeLimit is null").toBytes();
    }

    public List<Integer> getBuildChannels()
    {
        return buildChannels;
    }

    @Override
    public void addPartition(List<Domain> domains, Observation observation)
    {
        domains = ImmutableList.copyOf(requireNonNull(domains, "domains is null"));
        checkArgument(domains.size() == laneTypes.size(), "domain lane count does not match consumer");
        requireNonNull(observation, "observation is null");
        checkArgument(observation.sawNulls().size() == laneTypes.size(), "observation lane count does not match consumer");
        sawInputRow.compareAndSet(false, observation.sawInputRow());
        synchronized (lanesWithNull) {
            IntStream.range(0, laneTypes.size())
                    .filter(lane -> observation.sawNulls().get(lane))
                    .forEach(lanesWithNull::add);
        }
        addPartition(domains);
    }

    private void addPartition(List<Domain> domains)
    {
        if (collected) {
            return;
        }

        long domainRetainedSizeInBytes = getRetainedSizeInBytes(domains);
        summaryDomainsRetainedSizeInBytes.addAndGet(domainRetainedSizeInBytes);
        summaryDomains.add(domains);
        unionSummaryDomainsIfNecessary(false);

        List<Domain> result;
        synchronized (this) {
            verify(expectedPartitionCount == null || collectedPartitionCount < expectedPartitionCount);
            if (collected) {
                clearSummaryDomains();
                return;
            }
            collectedPartitionCount++;

            boolean allPartitionsCollected = expectedPartitionCount != null && collectedPartitionCount == expectedPartitionCount;
            if (allPartitionsCollected) {
                unionSummaryDomainsIfNecessary(true);
            }

            boolean sizeLimitExceeded = false;
            List<Domain> summary = summaryDomains.poll();
            if (summary != null) {
                long summarySize = getRetainedSizeInBytes(summary);
                if (summarySize > domainSizeLimitInBytes) {
                    summaryDomainsRetainedSizeInBytes.addAndGet(-summarySize);
                    sizeLimitExceeded = true;
                }
                else {
                    summaryDomains.add(summary);
                }
            }

            if (!allPartitionsCollected && !sizeLimitExceeded && !isAll(domains)) {
                return;
            }
            if (sizeLimitExceeded || isAll(domains)) {
                clearSummaryDomains();
                result = laneTypes.stream().map(Domain::all).collect(toImmutableList());
            }
            else {
                verify(expectedPartitionCount != null && collectedPartitionCount == expectedPartitionCount);
                verify(summaryDomains.size() == 1);
                result = requireNonNull(summaryDomains.poll(), "summary is null");
                long currentSize = summaryDomainsRetainedSizeInBytes.addAndGet(-getRetainedSizeInBytes(result));
                verify(currentSize == 0, "currentSize is expected to be zero: %s", currentSize);
            }
            collected = true;
        }
        notifyCollector(result);
    }

    @Override
    public void setPartitionCount(int partitionCount)
    {
        checkArgument(partitionCount >= 0, "partitionCount is negative");
        List<Domain> result;
        synchronized (this) {
            if (collected) {
                return;
            }
            checkState(expectedPartitionCount == null, "setPartitionCount should be called only once");
            expectedPartitionCount = partitionCount;
            if (collectedPartitionCount < partitionCount) {
                return;
            }
            if (partitionCount == 0) {
                result = laneTypes.stream().map(Domain::none).collect(toImmutableList());
            }
            else {
                unionSummaryDomainsIfNecessary(true);
                verify(summaryDomains.size() == 1);
                result = requireNonNull(summaryDomains.poll(), "summary is null");
                long currentSize = summaryDomainsRetainedSizeInBytes.addAndGet(-getRetainedSizeInBytes(result));
                verify(currentSize == 0, "currentSize is expected to be zero: %s", currentSize);
            }
            collected = true;
        }
        notifyCollector(result);
    }

    @Override
    public synchronized boolean isDomainCollectionComplete()
    {
        return collected;
    }

    private void notifyCollector(List<Domain> result)
    {
        List<Lane> lanes;
        synchronized (lanesWithNull) {
            lanes = IntStream.range(0, laneTypes.size())
                    .mapToObj(index -> new Lane(result.get(index), lanesWithNull.contains(index)))
                    .collect(toImmutableList());
        }
        collector.accept(new RuntimeMembershipPayload(lanes, ORDINARY, sawInputRow.get()));
    }

    private void unionSummaryDomainsIfNecessary(boolean force)
    {
        if (summaryDomainsRetainedSizeInBytes.get() < domainSizeLimitInBytes && !force) {
            return;
        }

        List<List<Domain>> domains = new ArrayList<>();
        long domainsRetainedSizeInBytes = 0;
        List<Domain> domain;
        while ((domain = summaryDomains.poll()) != null) {
            domains.add(domain);
            domainsRetainedSizeInBytes += getRetainedSizeInBytes(domain);
        }
        if (domains.isEmpty()) {
            return;
        }

        List<Domain> union = IntStream.range(0, laneTypes.size())
                .mapToObj(lane -> Domain.union(domains.stream().map(value -> value.get(lane)).toList()))
                .collect(toImmutableList());
        long unionSize = getRetainedSizeInBytes(union);
        if (summaryDomainsRetainedSizeInBytes.get() - domainsRetainedSizeInBytes + unionSize > domainSizeLimitInBytes) {
            union = union.stream().map(value -> value.simplify(1)).collect(toImmutableList());
            unionSize = getRetainedSizeInBytes(union);
        }
        summaryDomainsRetainedSizeInBytes.addAndGet(unionSize - domainsRetainedSizeInBytes);
        verify(summaryDomainsRetainedSizeInBytes.get() >= 0, "retained size is negative");
        summaryDomains.add(union);
    }

    private void clearSummaryDomains()
    {
        long domainsRetainedSizeInBytes = 0;
        List<Domain> domain;
        while ((domain = summaryDomains.poll()) != null) {
            domainsRetainedSizeInBytes += getRetainedSizeInBytes(domain);
        }
        summaryDomainsRetainedSizeInBytes.addAndGet(-domainsRetainedSizeInBytes);
        verify(summaryDomainsRetainedSizeInBytes.get() >= 0, "retained size is negative");
    }

    private static boolean isAll(List<Domain> domains)
    {
        return domains.stream().allMatch(Domain::isAll);
    }

    private static long getRetainedSizeInBytes(List<Domain> domains)
    {
        return domains.stream().mapToLong(Domain::getRetainedSizeInBytes).sum();
    }
}
