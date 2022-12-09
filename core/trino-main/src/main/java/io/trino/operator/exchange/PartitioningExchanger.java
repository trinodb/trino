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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

@NotThreadSafe
class PartitioningExchanger
        implements LocalExchanger
{
    private final List<Consumer<Page>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final Function<Page, Page> partitionedPagePreparer;
    private final PartitionFunction partitionFunction;
    private final IntArrayList[] partitionAssignments;

    public PartitioningExchanger(
            List<Consumer<Page>> partitions,
            LocalExchangeMemoryManager memoryManager,
            Function<Page, Page> partitionPagePreparer,
            PartitionFunction partitionFunction)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.partitionedPagePreparer = requireNonNull(partitionPagePreparer, "partitionPagePreparer is null");
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");

        partitionAssignments = new IntArrayList[partitions.size()];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }
    }

    @Override
    public void accept(Page page)
    {
        Page partitionPage = partitionedPagePreparer.apply(page);
        // assign each row to a partition. The assignments lists are all expected to cleared by the previous iterations
        for (int position = 0; position < partitionPage.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(partitionPage, position);
            partitionAssignments[partition].add(position);
        }

        // build a page for each partition
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

            Page pageSplit;
            if (partitionSize == page.getPositionCount()) {
                // whole input page will go to this partition, compact the input page avoid over-retaining memory and to
                // match the behavior of sub-partitioned pages that copy positions out
                page.compact();
                pageSplit = page;
            }
            else {
                pageSplit = page.copyPositions(positions, 0, partitionSize);
            }
            memoryManager.updateMemoryUsage(pageSplit.getRetainedSizeInBytes());
            buffers.get(partition).accept(pageSplit);
        }
    }

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
