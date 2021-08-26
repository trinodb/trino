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
import io.trino.operator.exchange.PageReference.PageReleasedListener;
import io.trino.spi.Page;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

class PartitioningExchanger
        implements LocalExchanger
{
    private final List<Consumer<PageReference>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final Function<Page, Page> partitionedPagePreparer;
    private final PartitionFunction partitionFunction;
    private final IntArrayList[] partitionAssignments;
    private final PageReleasedListener onPageReleased;

    public PartitioningExchanger(
            List<Consumer<PageReference>> partitions,
            LocalExchangeMemoryManager memoryManager,
            Function<Page, Page> partitionPagePreparer,
            PartitionFunction partitionFunction)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.onPageReleased = PageReleasedListener.forLocalExchangeMemoryManager(memoryManager);
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
        partitionPage(page, partitionedPagePreparer.apply(page));
    }

    private synchronized void partitionPage(Page page, Page partitionPage)
    {
        // assign each row to a partition. The assignments lists are all expected to cleared by the previous iterations
        for (int position = 0; position < partitionPage.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(partitionPage, position);
            partitionAssignments[partition].add(position);
        }

        // build a page for each partition
        for (int partition = 0; partition < partitionAssignments.length; partition++) {
            IntArrayList positions = partitionAssignments[partition];
            int partitionSize = positions.size();
            if (partitionSize == 0) {
                continue;
            }
            Page pageSplit;
            if (partitionSize == page.getPositionCount()) {
                pageSplit = page; // entire page will be sent to this partition, no copies necessary
            }
            else {
                pageSplit = page.copyPositions(positions.elements(), 0, partitionSize);
            }
            // clear the assigned positions list for the next iteration to start empty
            positions.clear();
            memoryManager.updateMemoryUsage(pageSplit.getRetainedSizeInBytes());
            buffers.get(partition).accept(new PageReference(pageSplit, 1, onPageReleased));
        }
    }

    @Override
    public ListenableFuture<Void> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
