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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static io.trino.operator.exchange.UniformPartitionRebalancer.WriterPartitionId;
import static io.trino.operator.exchange.UniformPartitionRebalancer.WriterPartitionId.serialize;
import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;

public class ScaleWriterPartitioningExchanger
        implements LocalExchanger
{
    private final List<Consumer<Page>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final long maxBufferedBytes;
    private final Function<Page, Page> partitionedPagePreparer;
    private final PartitionFunction partitionFunction;
    private final UniformPartitionRebalancer partitionRebalancer;

    private final IntArrayList[] writerAssignments;
    private final int[] partitionRowCounts;
    private final int[] partitionWriterIds;
    private final int[] partitionWriterIndexes;

    private final IntArrayList usedPartitions = new IntArrayList();

    // Use Long2IntMap instead of Map<WriterPartitionId, Integer> which helps to save memory in the worst case scenario.
    // Here first 32 bit of long key contains writerId whereas last 32 bit contains partitionId.
    private final Long2IntMap pageWriterPartitionRowCounts = new Long2IntOpenHashMap();

    // Use Long2LongMap instead of Map<WriterPartitionId, Long> which helps to save memory in the worst case scenario.
    // Here first 32 bit of long key contains writerId whereas last 32 bit contains partitionId.
    @GuardedBy("this")
    private final Long2LongMap writerPartitionRowCounts = new Long2LongOpenHashMap();

    public ScaleWriterPartitioningExchanger(
            List<Consumer<Page>> buffers,
            LocalExchangeMemoryManager memoryManager,
            long maxBufferedBytes,
            Function<Page, Page> partitionedPagePreparer,
            PartitionFunction partitionFunction,
            int partitionCount,
            UniformPartitionRebalancer partitionRebalancer)
    {
        this.buffers = requireNonNull(buffers, "buffers is null");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.maxBufferedBytes = maxBufferedBytes;
        this.partitionedPagePreparer = requireNonNull(partitionedPagePreparer, "partitionedPagePreparer is null");
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitionRebalancer = requireNonNull(partitionRebalancer, "partitionRebalancer is null");

        // Initialize writerAssignments with the buffer size
        writerAssignments = new IntArrayList[buffers.size()];
        for (int i = 0; i < writerAssignments.length; i++) {
            writerAssignments[i] = new IntArrayList();
        }

        partitionRowCounts = new int[partitionCount];
        partitionWriterIndexes = new int[partitionCount];
        partitionWriterIds = new int[partitionCount];

        // Initialize partitionWriterIds with negative values since it indicates that writerIds should be fetched
        // from the scaling state while page processing.
        fill(partitionWriterIds, -1);
    }

    @Override
    public void accept(Page page)
    {
        // Scale up writers when current buffer memory utilization is more than 50% of the maximum
        if (memoryManager.getBufferedBytes() > maxBufferedBytes * 0.5) {
            partitionRebalancer.rebalancePartitions();
        }

        Page partitionPage = partitionedPagePreparer.apply(page);

        // Assign each row to a writer by looking at partitions scaling state using partitionRebalancer
        for (int position = 0; position < partitionPage.getPositionCount(); position++) {
            // Get row partition id (or bucket id) which limits to the partitionCount. If there are more physical partitions than
            // this artificial partition limit, then it is possible that multiple physical partitions will get assigned the same
            // bucket id. Thus, multiple partitions will be scaled together since we track partition physicalWrittenBytes
            // using the artificial limit (partitionCount).
            int partitionId = partitionFunction.getPartition(partitionPage, position);
            partitionRowCounts[partitionId] += 1;

            // Get writer id for this partition by looking at the scaling state
            int writerId = partitionWriterIds[partitionId];
            if (writerId == -1) {
                writerId = getNextWriterId(partitionId);
                partitionWriterIds[partitionId] = writerId;
                usedPartitions.add(partitionId);
            }
            writerAssignments[writerId].add(position);
        }

        for (int partitionId : usedPartitions) {
            int writerId = partitionWriterIds[partitionId];
            pageWriterPartitionRowCounts.put(serialize(new WriterPartitionId(writerId, partitionId)), partitionRowCounts[partitionId]);

            // Reset the value of partition row count and writer id for the next page processing cycle
            partitionRowCounts[partitionId] = 0;
            partitionWriterIds[partitionId] = -1;
        }

        // Update partitions row count state which will help with scaling partitions across writers
        updatePartitionRowCounts(pageWriterPartitionRowCounts);

        // Reset pageWriterPartitionRowCounts and usedPartitions for the next page processing cycle
        pageWriterPartitionRowCounts.clear();
        usedPartitions.clear();

        // build a page for each writer
        for (int bucket = 0; bucket < writerAssignments.length; bucket++) {
            IntArrayList positionsList = writerAssignments[bucket];
            int bucketSize = positionsList.size();
            if (bucketSize == 0) {
                continue;
            }
            // clear the assigned positions list size for the next iteration to start empty. This
            // only resets the size() to 0 which controls the index where subsequent calls to add()
            // will store new values, but does not modify the positions array
            int[] positions = positionsList.elements();
            positionsList.clear();

            if (bucketSize == page.getPositionCount()) {
                // whole input page will go to this partition, compact the input page avoid over-retaining memory and to
                // match the behavior of sub-partitioned pages that copy positions out
                page.compact();
                sendPageToPartition(buffers.get(bucket), page);
                return;
            }

            Page pageSplit = page.copyPositions(positions, 0, bucketSize);
            sendPageToPartition(buffers.get(bucket), pageSplit);
        }
    }

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }

    public synchronized Long2LongMap getAndResetPartitionRowCounts()
    {
        Long2LongMap result = new Long2LongOpenHashMap(writerPartitionRowCounts);
        writerPartitionRowCounts.clear();
        return result;
    }

    private synchronized void updatePartitionRowCounts(Long2IntMap pagePartitionRowCounts)
    {
        pagePartitionRowCounts.forEach((writerPartitionId, rowCount) ->
                writerPartitionRowCounts.merge(writerPartitionId, rowCount, Long::sum));
    }

    private int getNextWriterId(int partitionId)
    {
        return partitionRebalancer.getWriterId(partitionId, partitionWriterIndexes[partitionId]++);
    }

    private void sendPageToPartition(Consumer<Page> buffer, Page pageSplit)
    {
        memoryManager.updateMemoryUsage(pageSplit.getRetainedSizeInBytes());
        buffer.accept(pageSplit);
    }
}
