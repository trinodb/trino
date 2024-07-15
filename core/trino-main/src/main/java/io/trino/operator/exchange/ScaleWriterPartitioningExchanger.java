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
import io.trino.operator.output.SkewedPartitionRebalancer;
import io.trino.spi.Page;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;

public class ScaleWriterPartitioningExchanger
        implements LocalExchanger
{
    private static final double SCALE_WRITER_MEMORY_PERCENTAGE = 0.7;
    private final List<Consumer<Page>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final long maxBufferedBytes;
    private final Function<Page, Page> partitionedPagePreparer;
    private final PartitionFunction partitionFunction;
    private final SkewedPartitionRebalancer partitionRebalancer;

    private final IntArrayList[] writerAssignments;
    private final int[] partitionRowCounts;
    private final int[] partitionWriterIds;
    private final int[] partitionWriterIndexes;
    private final Supplier<Long> totalMemoryUsed;
    private final long maxMemoryPerNode;

    public ScaleWriterPartitioningExchanger(
            List<Consumer<Page>> buffers,
            LocalExchangeMemoryManager memoryManager,
            long maxBufferedBytes,
            Function<Page, Page> partitionedPagePreparer,
            PartitionFunction partitionFunction,
            int partitionCount,
            SkewedPartitionRebalancer partitionRebalancer,
            Supplier<Long> totalMemoryUsed,
            long maxMemoryPerNode)
    {
        this.buffers = requireNonNull(buffers, "buffers is null");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.maxBufferedBytes = maxBufferedBytes;
        this.partitionedPagePreparer = requireNonNull(partitionedPagePreparer, "partitionedPagePreparer is null");
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitionRebalancer = requireNonNull(partitionRebalancer, "partitionRebalancer is null");
        this.totalMemoryUsed = requireNonNull(totalMemoryUsed, "totalMemoryUsed is null");
        this.maxMemoryPerNode = maxMemoryPerNode;

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
        // Reset the value of partition row count, writer ids and data processed for this page
        for (int partitionId = 0; partitionId < partitionRowCounts.length; partitionId++) {
            partitionRowCounts[partitionId] = 0;
            partitionWriterIds[partitionId] = -1;
        }

        // Scale up writers when current buffer memory utilization is more than 50% of the maximum.
        // Do not scale up if total memory used is greater than 70% of max memory per node.
        // We have to be conservative here otherwise scaling of writers will happen first
        // before we hit this limit, and then we won't be able to do anything to stop OOM error.
        if (memoryManager.getBufferedBytes() > maxBufferedBytes * 0.5 && totalMemoryUsed.get() < maxMemoryPerNode * SCALE_WRITER_MEMORY_PERCENTAGE) {
            partitionRebalancer.rebalance();
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
            }
            writerAssignments[writerId].add(position);
        }

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
                break;
            }

            Page pageSplit = page.copyPositions(positions, 0, bucketSize);
            sendPageToPartition(buffers.get(bucket), pageSplit);
        }

        // Only update the scaling state if the memory used is below the SCALE_WRITER_MEMORY_PERCENTAGE limit. Otherwise, if we keep updating
        // the scaling state and the memory used is fluctuating around the limit, then we could do massive scaling
        // in a single rebalancing cycle which could cause OOM error.
        if (totalMemoryUsed.get() < maxMemoryPerNode * SCALE_WRITER_MEMORY_PERCENTAGE) {
            for (int partitionId = 0; partitionId < partitionRowCounts.length; partitionId++) {
                partitionRebalancer.addPartitionRowCount(partitionId, partitionRowCounts[partitionId]);
            }
            partitionRebalancer.addDataProcessed(page.getSizeInBytes());
        }
    }

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }

    private int getNextWriterId(int partitionId)
    {
        return partitionRebalancer.getTaskId(partitionId, partitionWriterIndexes[partitionId]++);
    }

    private void sendPageToPartition(Consumer<Page> buffer, Page pageSplit)
    {
        long retainedSizeInBytes = pageSplit.getRetainedSizeInBytes();
        memoryManager.updateMemoryUsage(retainedSizeInBytes);
        buffer.accept(pageSplit);
    }
}
