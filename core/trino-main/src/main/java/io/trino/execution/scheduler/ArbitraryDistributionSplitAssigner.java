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
package io.trino.execution.scheduler;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.trino.exchange.SpoolingExchangeInput;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.split.RemoteSplit;
import io.trino.sql.planner.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.operator.ExchangeOperator.REMOTE_CATALOG_HANDLE;
import static java.lang.Math.ceil;
import static java.lang.Math.min;
import static java.lang.Math.round;
import static java.util.Objects.requireNonNull;

class ArbitraryDistributionSplitAssigner
        implements SplitAssigner
{
    private final Optional<CatalogHandle> catalogRequirement;
    private final Set<PlanNodeId> partitionedSources;
    private final Set<PlanNodeId> replicatedSources;
    private final Set<PlanNodeId> allSources;
    private final int adaptiveGrowthPeriod;
    private final double adaptiveGrowthFactor;
    private final long minTargetPartitionSizeInBytes;
    private final long maxTargetPartitionSizeInBytes;
    private final long standardSplitSizeInBytes;
    private final int maxTaskSplitCount;

    private int nextPartitionId;
    private int adaptiveCounter;
    private long targetPartitionSizeInBytes;
    private long roundedTargetPartitionSizeInBytes;
    private final List<PartitionAssignment> allAssignments = new ArrayList<>();
    private final Map<Optional<HostAddress>, PartitionAssignment> openAssignments = new HashMap<>();

    private final Set<PlanNodeId> completedSources = new HashSet<>();

    private final ListMultimap<PlanNodeId, Split> replicatedSplits = ArrayListMultimap.create();
    private boolean noMoreReplicatedSplits;

    ArbitraryDistributionSplitAssigner(
            Optional<CatalogHandle> catalogRequirement,
            Set<PlanNodeId> partitionedSources,
            Set<PlanNodeId> replicatedSources,
            int adaptiveGrowthPeriod,
            double adaptiveGrowthFactor,
            long minTargetPartitionSizeInBytes,
            long maxTargetPartitionSizeInBytes,
            long standardSplitSizeInBytes,
            int maxTaskSplitCount)
    {
        this.catalogRequirement = requireNonNull(catalogRequirement, "catalogRequirement is null");
        this.partitionedSources = ImmutableSet.copyOf(requireNonNull(partitionedSources, "partitionedSources is null"));
        this.replicatedSources = ImmutableSet.copyOf(requireNonNull(replicatedSources, "replicatedSources is null"));
        allSources = ImmutableSet.<PlanNodeId>builder()
                .addAll(partitionedSources)
                .addAll(replicatedSources)
                .build();
        this.adaptiveGrowthPeriod = adaptiveGrowthPeriod;
        this.adaptiveGrowthFactor = adaptiveGrowthFactor;
        this.minTargetPartitionSizeInBytes = minTargetPartitionSizeInBytes;
        this.maxTargetPartitionSizeInBytes = maxTargetPartitionSizeInBytes;
        this.standardSplitSizeInBytes = standardSplitSizeInBytes;
        this.maxTaskSplitCount = maxTaskSplitCount;

        this.targetPartitionSizeInBytes = minTargetPartitionSizeInBytes;
        this.roundedTargetPartitionSizeInBytes = minTargetPartitionSizeInBytes;
    }

    @Override
    public AssignmentResult assign(PlanNodeId planNodeId, ListMultimap<Integer, Split> splits, boolean noMoreSplits)
    {
        for (Split split : splits.values()) {
            Optional<CatalogHandle> splitCatalogRequirement = Optional.of(split.getCatalogHandle())
                    .filter(catalog -> !catalog.getType().isInternal() && !catalog.equals(REMOTE_CATALOG_HANDLE));
            checkArgument(
                    catalogRequirement.isEmpty() || catalogRequirement.equals(splitCatalogRequirement),
                    "unexpected split catalog requirement: %s",
                    splitCatalogRequirement);
        }
        if (replicatedSources.contains(planNodeId)) {
            return assignReplicatedSplits(planNodeId, ImmutableList.copyOf(splits.values()), noMoreSplits);
        }
        return assignPartitionedSplits(planNodeId, ImmutableList.copyOf(splits.values()), noMoreSplits);
    }

    @Override
    public AssignmentResult finish()
    {
        checkState(!allAssignments.isEmpty(), "allAssignments is not expected to be empty");
        return AssignmentResult.builder().build();
    }

    private AssignmentResult assignReplicatedSplits(PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
    {
        AssignmentResult.Builder assignment = AssignmentResult.builder();
        replicatedSplits.putAll(planNodeId, splits);
        for (PartitionAssignment partitionAssignment : allAssignments) {
            assignment.updatePartition(new PartitionUpdate(
                    partitionAssignment.getPartitionId(),
                    planNodeId,
                    false,
                    splits,
                    noMoreSplits));
        }
        if (noMoreSplits) {
            completedSources.add(planNodeId);
            if (completedSources.containsAll(replicatedSources)) {
                noMoreReplicatedSplits = true;
            }
        }
        if (noMoreReplicatedSplits) {
            for (PartitionAssignment partitionAssignment : allAssignments) {
                if (partitionAssignment.isFull()) {
                    assignment.sealPartition(partitionAssignment.getPartitionId());
                }
            }
        }
        if (completedSources.containsAll(allSources)) {
            if (allAssignments.isEmpty()) {
                // at least a single partition is expected to be created
                allAssignments.add(new PartitionAssignment(0));
                assignment.addPartition(new Partition(0, new NodeRequirements(catalogRequirement, ImmutableSet.of())));
                for (PlanNodeId replicatedSourceId : replicatedSources) {
                    assignment.updatePartition(new PartitionUpdate(
                            0,
                            replicatedSourceId,
                            false,
                            replicatedSplits.get(replicatedSourceId),
                            true));
                }
                for (PlanNodeId partitionedSourceId : partitionedSources) {
                    assignment.updatePartition(new PartitionUpdate(
                            0,
                            partitionedSourceId,
                            false,
                            ImmutableList.of(),
                            true));
                }
                assignment.sealPartition(0);
            }
            else {
                for (PartitionAssignment partitionAssignment : allAssignments) {
                    // set noMoreSplits for partitioned sources
                    if (!partitionAssignment.isFull()) {
                        for (PlanNodeId partitionedSourceNodeId : partitionedSources) {
                            assignment.updatePartition(new PartitionUpdate(
                                    partitionAssignment.getPartitionId(),
                                    partitionedSourceNodeId,
                                    false,
                                    ImmutableList.of(),
                                    true));
                        }
                        // seal partition
                        assignment.sealPartition(partitionAssignment.getPartitionId());
                    }
                }
            }
            replicatedSplits.clear();
            // no more partitions will be created
            assignment.setNoMorePartitions();
        }
        return assignment.build();
    }

    private AssignmentResult assignPartitionedSplits(PlanNodeId planNodeId, List<Split> splits, boolean noMoreSplits)
    {
        AssignmentResult.Builder assignment = AssignmentResult.builder();

        for (Split split : splits) {
            Optional<HostAddress> hostRequirement = getHostRequirement(split);
            PartitionAssignment partitionAssignment = openAssignments.get(hostRequirement);
            long splitSizeInBytes = getSplitSizeInBytes(split);
            if (partitionAssignment != null && ((partitionAssignment.getAssignedDataSizeInBytes() + splitSizeInBytes > roundedTargetPartitionSizeInBytes)
                    || (partitionAssignment.getAssignedSplitCount() + 1 > maxTaskSplitCount))) {
                partitionAssignment.setFull(true);
                for (PlanNodeId partitionedSourceNodeId : partitionedSources) {
                    assignment.updatePartition(new PartitionUpdate(
                            partitionAssignment.getPartitionId(),
                            partitionedSourceNodeId,
                            false,
                            ImmutableList.of(),
                            true));
                }
                if (completedSources.containsAll(replicatedSources)) {
                    assignment.sealPartition(partitionAssignment.getPartitionId());
                }
                partitionAssignment = null;
                openAssignments.remove(hostRequirement);

                adaptiveCounter++;
                if (adaptiveCounter >= adaptiveGrowthPeriod) {
                    targetPartitionSizeInBytes = (long) min(maxTargetPartitionSizeInBytes, ceil(targetPartitionSizeInBytes * adaptiveGrowthFactor));
                    // round to a multiple of minTargetPartitionSizeInBytes so work will be evenly distributed among drivers of a task
                    roundedTargetPartitionSizeInBytes = round(targetPartitionSizeInBytes * 1.0 / minTargetPartitionSizeInBytes) * minTargetPartitionSizeInBytes;
                    verify(roundedTargetPartitionSizeInBytes > 0, "roundedTargetPartitionSizeInBytes %s not positive", roundedTargetPartitionSizeInBytes);
                    adaptiveCounter = 0;
                }
            }
            if (partitionAssignment == null) {
                partitionAssignment = new PartitionAssignment(nextPartitionId++);
                allAssignments.add(partitionAssignment);
                openAssignments.put(hostRequirement, partitionAssignment);
                assignment.addPartition(new Partition(
                        partitionAssignment.getPartitionId(),
                        new NodeRequirements(catalogRequirement, hostRequirement.map(ImmutableSet::of).orElseGet(ImmutableSet::of))));

                for (PlanNodeId replicatedSourceId : replicatedSources) {
                    assignment.updatePartition(new PartitionUpdate(
                            partitionAssignment.getPartitionId(),
                            replicatedSourceId,
                            false,
                            replicatedSplits.get(replicatedSourceId),
                            completedSources.contains(replicatedSourceId)));
                }
            }
            assignment.updatePartition(new PartitionUpdate(
                    partitionAssignment.getPartitionId(),
                    planNodeId,
                    true,
                    ImmutableList.of(split),
                    false));
            partitionAssignment.assignSplit(splitSizeInBytes);
        }

        if (noMoreSplits) {
            completedSources.add(planNodeId);
        }

        if (completedSources.containsAll(allSources)) {
            if (allAssignments.isEmpty()) {
                // at least a single partition is expected to be created
                allAssignments.add(new PartitionAssignment(0));
                assignment.addPartition(new Partition(0, new NodeRequirements(catalogRequirement, ImmutableSet.of())));
                for (PlanNodeId replicatedSourceId : replicatedSources) {
                    assignment.updatePartition(new PartitionUpdate(
                            0,
                            replicatedSourceId,
                            false,
                            replicatedSplits.get(replicatedSourceId),
                            true));
                }
                for (PlanNodeId partitionedSourceId : partitionedSources) {
                    assignment.updatePartition(new PartitionUpdate(
                            0,
                            partitionedSourceId,
                            false,
                            ImmutableList.of(),
                            true));
                }

                assignment.sealPartition(0);
            }
            else {
                for (PartitionAssignment partitionAssignment : openAssignments.values()) {
                    // set noMoreSplits for partitioned sources
                    for (PlanNodeId partitionedSourceNodeId : partitionedSources) {
                        assignment.updatePartition(new PartitionUpdate(
                                partitionAssignment.getPartitionId(),
                                partitionedSourceNodeId,
                                false,
                                ImmutableList.of(),
                                true));
                    }
                    // seal partition
                    assignment.sealPartition(partitionAssignment.getPartitionId());
                }
                openAssignments.clear();
            }
            replicatedSplits.clear();
            // no more partitions will be created
            assignment.setNoMorePartitions();
        }

        return assignment.build();
    }

    private Optional<HostAddress> getHostRequirement(Split split)
    {
        if (split.getConnectorSplit().isRemotelyAccessible()) {
            return Optional.empty();
        }
        List<HostAddress> addresses = split.getAddresses();
        checkArgument(!addresses.isEmpty(), "split is not remotely accessible but the list of hosts is empty: %s", split);
        HostAddress selectedAddress = null;
        long selectedAssignmentDataSize = Long.MAX_VALUE;
        for (HostAddress address : addresses) {
            PartitionAssignment assignment = openAssignments.get(Optional.of(address));
            if (assignment == null) {
                // prioritize unused addresses
                selectedAddress = address;
                break;
            }
            if (assignment.getAssignedDataSizeInBytes() < selectedAssignmentDataSize) {
                // otherwise prioritize the smallest assignment
                selectedAddress = address;
                selectedAssignmentDataSize = assignment.getAssignedDataSizeInBytes();
            }
        }
        verify(selectedAddress != null, "selectedAddress is null");
        return Optional.of(selectedAddress);
    }

    private long getSplitSizeInBytes(Split split)
    {
        if (split.getCatalogHandle().equals(REMOTE_CATALOG_HANDLE)) {
            RemoteSplit remoteSplit = (RemoteSplit) split.getConnectorSplit();
            SpoolingExchangeInput exchangeInput = (SpoolingExchangeInput) remoteSplit.getExchangeInput();
            long size = 0;
            for (ExchangeSourceHandle handle : exchangeInput.getExchangeSourceHandles()) {
                size += handle.getDataSizeInBytes();
            }
            return size;
        }
        return round(((split.getSplitWeight().getRawValue() * 1.0) / SplitWeight.standard().getRawValue()) * standardSplitSizeInBytes);
    }

    private static class PartitionAssignment
    {
        private final int partitionId;
        private long assignedDataSizeInBytes;
        private int assignedSplitCount;
        private boolean full;

        private PartitionAssignment(int partitionId)
        {
            this.partitionId = partitionId;
        }

        public int getPartitionId()
        {
            return partitionId;
        }

        public void assignSplit(long sizeInBytes)
        {
            assignedDataSizeInBytes += sizeInBytes;
            assignedSplitCount++;
        }

        public long getAssignedDataSizeInBytes()
        {
            return assignedDataSizeInBytes;
        }

        public int getAssignedSplitCount()
        {
            return assignedSplitCount;
        }

        public boolean isFull()
        {
            return full;
        }

        public void setFull(boolean full)
        {
            this.full = full;
        }
    }
}
