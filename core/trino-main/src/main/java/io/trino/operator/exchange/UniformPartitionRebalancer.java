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

package io.trino.operator.exchange;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.execution.resourcegroups.IndexedPriorityQueue;
import it.unimi.dsi.fastutil.longs.Long2LongMap;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Double.isNaN;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Help in finding the skewness across writers when writing partitioned data using preferred partitioning.
 * It then tries to uniformly distribute the biggest partitions from skewed writers to all the available writers.
 * <p>
 * Example:
 * <p>
 * Before: For three writers with skewed partitions
 * Writer 1 -> No partition assigned -> 0 bytes
 * Writer 2 -> No partition assigned -> 0 bytes
 * Writer 3 -> Partition 1 (100MB) + Partition 2 (100MB) + Partition 3 (100MB) ->  300 MB
 * <p>
 * After scaling:
 * Writer 1 -> Partition 1 (50MB) + Partition 3 (50MB) -> 100 MB
 * Writer 2 -> Partition 2 (50MB) -> 50 MB
 * Writer 3 -> Partition 1 (150MB) + Partition 2 (150MB) + Partition 3 (150MB) ->  450 MB
 */
@ThreadSafe
public class UniformPartitionRebalancer
{
    private static final Logger log = Logger.get(UniformPartitionRebalancer.class);
    // If the percentage difference between the two writers with maximum and minimum physical written bytes
    // since last rebalance is above 0.7 (or 70%), then we consider them skewed.
    private static final double SKEWNESS_THRESHOLD = 0.7;

    private final List<Supplier<Long>> writerPhysicalWrittenBytesSuppliers;
    // Use Long2LongMap instead of Map<WriterPartitionId, Integer> which helps to save memory in the worst case scenario.
    // Here first 32 bit of Long key contains writerId whereas last 32 bit contains partitionId.
    private final Supplier<Long2LongMap> partitionRowCountsSupplier;
    private final long writerMinSize;
    private final int numberOfWriters;
    private final long rebalanceThresholdMinPhysicalWrittenBytes;

    private final AtomicLongArray writerPhysicalWrittenBytesAtLastRebalance;

    private final PartitionInfo[] partitionInfos;

    public UniformPartitionRebalancer(
            List<Supplier<Long>> writerPhysicalWrittenBytesSuppliers,
            Supplier<Long2LongMap> partitionRowCountsSupplier,
            int partitionCount,
            int numberOfWriters,
            long writerMinSize)
    {
        this.writerPhysicalWrittenBytesSuppliers = requireNonNull(writerPhysicalWrittenBytesSuppliers, "writerPhysicalWrittenBytesSuppliers is null");
        this.partitionRowCountsSupplier = requireNonNull(partitionRowCountsSupplier, "partitionRowCountsSupplier is null");
        this.writerMinSize = writerMinSize;
        this.numberOfWriters = numberOfWriters;
        this.rebalanceThresholdMinPhysicalWrittenBytes = max(DataSize.of(50, MEGABYTE).toBytes(), writerMinSize);

        this.writerPhysicalWrittenBytesAtLastRebalance = new AtomicLongArray(numberOfWriters);

        partitionInfos = new PartitionInfo[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitionInfos[i] = new PartitionInfo(i % numberOfWriters);
        }
    }

    public int getWriterId(int partitionId, int index)
    {
        return partitionInfos[partitionId].getWriterId(index);
    }

    @VisibleForTesting
    List<Integer> getWriterIds(int partitionId)
    {
        return partitionInfos[partitionId].getWriterIds();
    }

    public void rebalancePartitions()
    {
        List<Long> writerPhysicalWrittenBytes = writerPhysicalWrittenBytesSuppliers.stream()
                .map(Supplier::get)
                .collect(toImmutableList());

        // Rebalance only when total bytes written since last rebalance is greater than rebalance threshold
        if (getPhysicalWrittenBytesSinceLastRebalance(writerPhysicalWrittenBytes) > rebalanceThresholdMinPhysicalWrittenBytes) {
            rebalancePartitions(writerPhysicalWrittenBytes);
        }
    }

    private int getPhysicalWrittenBytesSinceLastRebalance(List<Long> writerPhysicalWrittenBytes)
    {
        int physicalWrittenBytesSinceLastRebalance = 0;
        for (int writerId = 0; writerId < writerPhysicalWrittenBytes.size(); writerId++) {
            physicalWrittenBytesSinceLastRebalance +=
                    writerPhysicalWrittenBytes.get(writerId) - writerPhysicalWrittenBytesAtLastRebalance.get(writerId);
        }

        return physicalWrittenBytesSinceLastRebalance;
    }

    private synchronized void rebalancePartitions(List<Long> writerPhysicalWrittenBytes)
    {
        Long2LongMap partitionRowCounts = partitionRowCountsSupplier.get();
        RebalanceContext context = new RebalanceContext(writerPhysicalWrittenBytes, partitionRowCounts);

        IndexedPriorityQueue<WriterId> maxWriters = new IndexedPriorityQueue<>();
        IndexedPriorityQueue<WriterId> minWriters = new IndexedPriorityQueue<>();
        for (int writerId = 0; writerId < numberOfWriters; writerId++) {
            WriterId writer = new WriterId(writerId);
            maxWriters.addOrUpdate(writer, context.getWriterEstimatedWrittenBytes(writer));
            minWriters.addOrUpdate(writer, Long.MAX_VALUE - context.getWriterEstimatedWrittenBytes(writer));
        }

        // Find skewed partitions and scale them across multiple writers
        while (true) {
            // Find the writer with maximum physical written bytes since last rebalance
            WriterId maxWriter = maxWriters.poll();

            if (maxWriter == null) {
                break;
            }

            // Find the skewness against writer with max physical written bytes since last rebalance
            List<WriterId> minSkewedWriters = findSkewedMinWriters(context, maxWriter, minWriters);
            if (minSkewedWriters.isEmpty()) {
                break;
            }

            for (WriterId minSkewedWriter : minSkewedWriters) {
                // There's no need to add the maxWriter back to priority queues if no partition rebalancing happened
                List<WriterId> affectedWriters = context.rebalancePartition(maxWriter, minSkewedWriter);
                if (!affectedWriters.isEmpty()) {
                    for (WriterId affectedWriter : affectedWriters) {
                        maxWriters.addOrUpdate(affectedWriter, context.getWriterEstimatedWrittenBytes(maxWriter));
                        minWriters.addOrUpdate(affectedWriter, Long.MAX_VALUE - context.getWriterEstimatedWrittenBytes(maxWriter));
                    }
                    break;
                }
            }

            // Add all the min skewed writers back to the minWriters queue with updated priorities
            for (WriterId minSkewedWriter : minSkewedWriters) {
                maxWriters.addOrUpdate(minSkewedWriter, context.getWriterEstimatedWrittenBytes(minSkewedWriter));
                minWriters.addOrUpdate(minSkewedWriter, Long.MAX_VALUE - context.getWriterEstimatedWrittenBytes(minSkewedWriter));
            }
        }

        resetStateForNextRebalance(context, writerPhysicalWrittenBytes, partitionRowCounts);
    }

    private List<WriterId> findSkewedMinWriters(RebalanceContext context, WriterId maxWriter, IndexedPriorityQueue<WriterId> minWriters)
    {
        ImmutableList.Builder<WriterId> minSkewedWriters = ImmutableList.builder();
        long maxWriterWrittenBytes = context.getWriterEstimatedWrittenBytes(maxWriter);
        while (true) {
            // Find the writer with minimum written bytes since last rebalance
            WriterId minWriter = minWriters.poll();
            if (minWriter == null) {
                break;
            }

            long minWriterWrittenBytes = context.getWriterEstimatedWrittenBytes(minWriter);

            // find the skewness against writer with max written bytes since last rebalance
            double skewness = ((double) (maxWriterWrittenBytes - minWriterWrittenBytes)) / maxWriterWrittenBytes;
            if (skewness <= SKEWNESS_THRESHOLD || isNaN(skewness)) {
                break;
            }

            minSkewedWriters.add(minWriter);
        }
        return minSkewedWriters.build();
    }

    private void resetStateForNextRebalance(RebalanceContext context, List<Long> writerPhysicalWrittenBytes, Long2LongMap partitionRowCounts)
    {
        partitionRowCounts.forEach((serializedKey, rowCount) -> {
            WriterPartitionId writerPartitionId = WriterPartitionId.deserialize(serializedKey);
            PartitionInfo partitionInfo = partitionInfos[writerPartitionId.partitionId];
            if (context.isPartitionRebalanced(writerPartitionId.partitionId)) {
                // Reset physical written bytes for rebalanced partitions
                partitionInfo.resetPhysicalWrittenBytesAtLastRebalance();
            }
            else {
                long writtenBytes = context.estimatePartitionWrittenBytesSinceLastRebalance(new WriterId(writerPartitionId.writerId), rowCount);
                partitionInfo.addToPhysicalWrittenBytesAtLastRebalance(writtenBytes);
            }
        });

        for (int i = 0; i < numberOfWriters; i++) {
            writerPhysicalWrittenBytesAtLastRebalance.set(i, writerPhysicalWrittenBytes.get(i));
        }
    }

    private class RebalanceContext
    {
        private final Set<Integer> rebalancedPartitions = new HashSet<>();
        private final long[] writerPhysicalWrittenBytesSinceLastRebalance;
        private final long[] writerRowCountSinceLastRebalance;
        private final long[] writerEstimatedWrittenBytes;
        private final List<IndexedPriorityQueue<PartitionIdWithRowCount>> writerMaxPartitions;

        private RebalanceContext(List<Long> writerPhysicalWrittenBytes, Long2LongMap partitionRowCounts)
        {
            writerPhysicalWrittenBytesSinceLastRebalance = new long[numberOfWriters];
            writerEstimatedWrittenBytes = new long[numberOfWriters];
            for (int writerId = 0; writerId < writerPhysicalWrittenBytes.size(); writerId++) {
                long physicalWrittenBytesSinceLastRebalance =
                        writerPhysicalWrittenBytes.get(writerId) - writerPhysicalWrittenBytesAtLastRebalance.get(writerId);
                writerPhysicalWrittenBytesSinceLastRebalance[writerId] = physicalWrittenBytesSinceLastRebalance;
                writerEstimatedWrittenBytes[writerId] = physicalWrittenBytesSinceLastRebalance;
            }

            writerRowCountSinceLastRebalance = new long[numberOfWriters];
            writerMaxPartitions = new ArrayList<>(numberOfWriters);
            for (int writerId = 0; writerId < numberOfWriters; writerId++) {
                writerMaxPartitions.add(new IndexedPriorityQueue<>());
            }

            partitionRowCounts.forEach((serializedKey, rowCount) -> {
                WriterPartitionId writerPartitionId = WriterPartitionId.deserialize(serializedKey);
                writerRowCountSinceLastRebalance[writerPartitionId.writerId] += rowCount;
                writerMaxPartitions
                        .get(writerPartitionId.writerId)
                        .addOrUpdate(new PartitionIdWithRowCount(writerPartitionId.partitionId, rowCount), rowCount);
            });
        }

        private List<WriterId> rebalancePartition(WriterId from, WriterId to)
        {
            IndexedPriorityQueue<PartitionIdWithRowCount> maxPartitions = writerMaxPartitions.get(from.id);
            ImmutableList.Builder<WriterId> affectedWriters = ImmutableList.builder();

            for (PartitionIdWithRowCount partitionToRebalance : maxPartitions) {
                // Find the partition with maximum written bytes since last rebalance
                PartitionInfo partitionInfo = partitionInfos[partitionToRebalance.id];

                // If a partition is already rebalanced or min skewed writer is already writing to that partition, then skip
                // this partition and move on to the other partition inside max writer. Also, we don't rebalance same partition
                // twice because we want to make sure that every writer wrote writerMinSize for a given partition
                if (!isPartitionRebalanced(partitionToRebalance.id) && !partitionInfo.containsWriter(to.id)) {
                    // First remove the partition from the priority queue since there's no need go over it again. As in the next
                    // section we will check whether it can be scaled or not.
                    maxPartitions.remove(partitionToRebalance);

                    long estimatedPartitionWrittenBytesSinceLastRebalance = estimatePartitionWrittenBytesSinceLastRebalance(from, partitionToRebalance.rowCount);
                    long estimatedPartitionWrittenBytes =
                            estimatedPartitionWrittenBytesSinceLastRebalance + partitionInfo.getPhysicalWrittenBytesAtLastRebalancePerWriter();

                    // Scale the partition when estimated physicalWrittenBytes is greater than writerMinSize.
                    if (partitionInfo.getWriterCount() <= numberOfWriters && estimatedPartitionWrittenBytes >= writerMinSize) {
                        partitionInfo.addWriter(to.id);
                        rebalancedPartitions.add(partitionToRebalance.id);
                        updateWriterEstimatedWrittenBytes(to, estimatedPartitionWrittenBytesSinceLastRebalance, partitionInfo);
                        for (int writer : partitionInfo.getWriterIds()) {
                            affectedWriters.add(new WriterId(writer));
                        }
                        log.debug("Scaled partition (%s) to writer %s with writer count %s", partitionToRebalance.id, to.id, partitionInfo.getWriterCount());
                    }

                    break;
                }
            }

            return affectedWriters.build();
        }

        private void updateWriterEstimatedWrittenBytes(WriterId to, long estimatedPartitionWrittenBytesSinceLastRebalance, PartitionInfo partitionInfo)
        {
            // Since a partition is rebalanced from max to min skewed writer, decrease the priority of max
            // writer as well as increase the priority of min writer.
            int newWriterCount = partitionInfo.getWriterCount();
            int oldWriterCount = newWriterCount - 1;
            for (int writer : partitionInfo.getWriterIds()) {
                if (writer != to.id) {
                    writerEstimatedWrittenBytes[writer] -= estimatedPartitionWrittenBytesSinceLastRebalance / newWriterCount;
                }
            }

            writerEstimatedWrittenBytes[to.id] += estimatedPartitionWrittenBytesSinceLastRebalance * oldWriterCount / newWriterCount;
        }

        private long getWriterEstimatedWrittenBytes(WriterId writer)
        {
            return writerEstimatedWrittenBytes[writer.id];
        }

        private boolean isPartitionRebalanced(int partitionId)
        {
            return rebalancedPartitions.contains(partitionId);
        }

        private long estimatePartitionWrittenBytesSinceLastRebalance(WriterId writer, long partitionRowCount)
        {
            if (writerRowCountSinceLastRebalance[writer.id] == 0) {
                return 0L;
            }
            return (writerPhysicalWrittenBytesSinceLastRebalance[writer.id] * partitionRowCount) / writerRowCountSinceLastRebalance[writer.id];
        }
    }

    @ThreadSafe
    private static class PartitionInfo
    {
        private final List<Integer> writerAssignments;
        // Partition estimated physical written bytes at the end of last rebalance cycle
        private final AtomicLong physicalWrittenBytesAtLastRebalance = new AtomicLong(0);

        private PartitionInfo(int initialWriterId)
        {
            this.writerAssignments = new CopyOnWriteArrayList<>(ImmutableList.of(initialWriterId));
        }

        private boolean containsWriter(int writerId)
        {
            return writerAssignments.contains(writerId);
        }

        private void addWriter(int writerId)
        {
            writerAssignments.add(writerId);
        }

        private int getWriterId(int index)
        {
            return writerAssignments.get(floorMod(index, getWriterCount()));
        }

        private List<Integer> getWriterIds()
        {
            return ImmutableList.copyOf(writerAssignments);
        }

        private int getWriterCount()
        {
            return writerAssignments.size();
        }

        private void resetPhysicalWrittenBytesAtLastRebalance()
        {
            physicalWrittenBytesAtLastRebalance.set(0);
        }

        private void addToPhysicalWrittenBytesAtLastRebalance(long writtenBytes)
        {
            physicalWrittenBytesAtLastRebalance.addAndGet(writtenBytes);
        }

        private long getPhysicalWrittenBytesAtLastRebalancePerWriter()
        {
            return physicalWrittenBytesAtLastRebalance.get() / writerAssignments.size();
        }
    }

    public record WriterPartitionId(int writerId, int partitionId)
    {
        public static WriterPartitionId deserialize(long value)
        {
            int writerId = (int) (value >> 32);
            int partitionId = (int) value;

            return new WriterPartitionId(writerId, partitionId);
        }

        public static long serialize(WriterPartitionId writerPartitionId)
        {
            // Serialize to long to save memory where first 32 bit contains writerId whereas last 32 bit
            // contains partitionId.
            return ((long) writerPartitionId.writerId << 32 | writerPartitionId.partitionId & 0xFFFFFFFFL);
        }

        public WriterPartitionId(int writerId, int partitionId)
        {
            this.writerId = writerId;
            this.partitionId = partitionId;
        }
    }

    private record WriterId(int id)
    {
        private WriterId(int id)
        {
            this.id = id;
        }
    }

    private record PartitionIdWithRowCount(int id, long rowCount)
    {
        private PartitionIdWithRowCount(int id, long rowCount)
        {
            this.id = id;
            this.rowCount = rowCount;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionIdWithRowCount that = (PartitionIdWithRowCount) o;
            return id == that.id;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(id);
        }
    }
}
