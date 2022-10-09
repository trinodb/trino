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
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.operator.PartitionFunction;
import io.trino.operator.exchange.PageReference.PageReleasedListener;
import io.trino.spi.Page;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.trino.operator.exchange.PageReference.PageReleasedListener.forLocalExchangeMemoryManager;
import static java.util.Objects.requireNonNull;

/**
 * This class has two behaviours:
 *      1. Distribute the partitioned data among the available writers using partition function.
 *      2. When it finds that a specific partition has written more than writerMinSize, then it will scale up
 *         that partition by distributing the load among multiple writers in a round-robin fashion.
 *
 * For example with the following configuration:
 *      - writerCount: 3
 *      - numberOfPartitions: 3
 *
 * Step 1 - Insert a page which includes all three partitions. Assign first two partitions to
 *          first writer and third partition to second writer.
 *
 * W1    W2    W3
 * 0.33  0.33  0.33
 *
 * Step 2 - Insert a page which only include first partition.
 *
 * W1    W2    W3
 * 1.33  0.33  0.33
 *
 * Step 3 - Insert a page with only include first partition. However, now scaling will happen for first partition.
 *
 * W1    W2    W3
 * 1.66  1.33  0.33
 */
public class ScaleWriterPartitioningExchanger
        implements LocalExchanger
{
    private static final Logger log = Logger.get(ScaleWriterPartitioningExchanger.class);

    private final List<Consumer<PageReference>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final long maxBufferedBytes;
    private final Function<Page, Page> partitionedPagePreparer;
    private final PartitionFunction partitionFunction;
    private final Supplier<Map<Integer, Long>> partitionPhysicalWrittenBytesSupplier;
    private final long writerMinSize;
    private final PageReleasedListener onPageReleased;
    private final int writerCount;

    private final IntArrayList[] partitionAssignments;
    private final IntArrayList[] writerAssignments;
    private final PartitionScalingInfo[] partitionScalingInfos;

    public ScaleWriterPartitioningExchanger(
            List<Consumer<PageReference>> buffers,
            LocalExchangeMemoryManager memoryManager,
            long maxBufferedBytes,
            Function<Page, Page> partitionedPagePreparer,
            PartitionFunction partitionFunction,
            int partitionCount,
            Supplier<Map<Integer, Long>> partitionPhysicalWrittenBytesSupplier,
            DataSize writerMinSize)
    {
        this.buffers = requireNonNull(buffers, "buffers is null");
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.maxBufferedBytes = maxBufferedBytes;
        this.partitionedPagePreparer = requireNonNull(partitionedPagePreparer, "partitionedPagePreparer is null");
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitionPhysicalWrittenBytesSupplier = requireNonNull(partitionPhysicalWrittenBytesSupplier, "partitionPhysicalWrittenBytes is null");
        this.writerMinSize = requireNonNull(writerMinSize, "writerMinSize is null").toBytes();
        this.onPageReleased = forLocalExchangeMemoryManager(memoryManager);
        this.writerCount = buffers.size();

        // Initialize writerAssignments with the buffer size
        writerAssignments = new IntArrayList[buffers.size()];
        for (int i = 0; i < writerAssignments.length; i++) {
            writerAssignments[i] = new IntArrayList();
        }

        // Initialize partitionAssignments and partitionScalingInfos with the artificial partition limit.
        partitionAssignments = new IntArrayList[partitionCount];
        partitionScalingInfos = new PartitionScalingInfo[partitionCount];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
            partitionScalingInfos[i] = new PartitionScalingInfo();
        }
    }

    @Override
    public void accept(Page page)
    {
        Page partitionPage = partitionedPagePreparer.apply(page);

        // Assign each row to a partition which limits to the partitionCount. If there are more partitions than
        // this artificial partition limit, then it is possible that multiple partitions will get assigned the same
        // unique id. Thus, multiple partitions will be scaled together since we track partition physicalWrittenBytes
        // using the artificial limit (partitionCount). The assignments lists are all expected to cleared by the
        // previous iterations.
        for (int position = 0; position < partitionPage.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(partitionPage, position);
            partitionAssignments[partition].add(position);
        }

        // Read the partition level physicalWrittenBytes
        Map<Integer, Long> partitionPhysicalWrittenBytes = partitionPhysicalWrittenBytesSupplier.get();

        // assign each partition to a specific writer
        for (int partition = 0; partition < partitionAssignments.length; partition++) {
            IntArrayList positionsList = partitionAssignments[partition];
            int partitionSize = positionsList.size();
            if (partitionSize == 0) {
                continue;
            }

            // clear the assigned positions list size for the next iteration to start empty. This
            // only resets the size() to 0 which controls the index where subsequent calls to add()
            // will store new values, but does not modify the positions array
            int[] positions = positionsList.elements();
            positionsList.clear();

            int nextPartitionWriter = getNextPartitionWriter(partition, partitionPhysicalWrittenBytes.getOrDefault(partition, 0L));

            for (int i = 0; i < partitionSize; i++) {
                writerAssignments[nextPartitionWriter].add(positions[i]);
            }
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
                return;
            }

            Page pageSplit = page.copyPositions(positions, 0, bucketSize);
            sendPageToPartition(buffers.get(bucket), pageSplit);
        }
    }

    private void sendPageToPartition(Consumer<PageReference> buffer, Page pageSplit)
    {
        PageReference pageReference = new PageReference(pageSplit, 1, onPageReleased);
        memoryManager.updateMemoryUsage(pageReference.getRetainedSizeInBytes());
        buffer.accept(pageReference);
    }

    private int getNextPartitionWriter(int partition, long physicalWrittenBytes)
    {
        PartitionScalingInfo scalingInfo = partitionScalingInfos[partition];
        int partitionWriterCount = scalingInfo.getWriterCount();

        // Scale up writers for a partition when current buffer memory utilization is more than 50% of the
        // maximum and physical written bytes is greater than writerMinSize.
        // This also mean that we won't scale local writers if the writing speed can cope up
        // with incoming data. In another word, buffer utilization is below 50%.
        if (partitionWriterCount <= writerCount && memoryManager.getBufferedBytes() >= maxBufferedBytes / 2) {
            if (physicalWrittenBytes >= writerMinSize * partitionWriterCount) {
                scalingInfo.incrementWriterCount();
                log.debug("Increased task writer count to " + (partitionWriterCount + 1) + " for partition " + partition);
            }
        }

        // Find the next writer in a round-robin fashion.
        int nextLogicalWriter = scalingInfo.getNextWriter();

        return (partition + nextLogicalWriter) % writerCount;
    }

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }

    private static class PartitionScalingInfo
    {
        private int writerCount = 1;
        private int currentWriterId = -1;

        private PartitionScalingInfo() {}

        private int getNextWriter()
        {
            currentWriterId = (currentWriterId + 1) % writerCount;
            return currentWriterId;
        }

        private int getWriterCount()
        {
            return writerCount;
        }

        private void incrementWriterCount()
        {
            writerCount = writerCount + 1;
        }
    }
}
