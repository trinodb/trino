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
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

class PartitioningExchanger
        implements LocalExchanger
{
    private final List<Consumer<PageReference>> buffers;
    private final LocalExchangeMemoryManager memoryManager;
    private final PartitionFunction partitionFunction;
    private final IntArrayList[] partitionAssignments;
    private final PageReleasedListener onPageReleased;

    public PartitioningExchanger(
            List<Consumer<PageReference>> partitions,
            LocalExchangeMemoryManager memoryManager,
            PartitionFunction partitionFunction)
    {
        this.buffers = ImmutableList.copyOf(requireNonNull(partitions, "partitions is null"));
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.onPageReleased = PageReleasedListener.forLocalExchangeMemoryManager(memoryManager);
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");

        partitionAssignments = new IntArrayList[partitions.size()];
        for (int i = 0; i < partitionAssignments.length; i++) {
            partitionAssignments[i] = new IntArrayList();
        }
    }

    @Override
    public synchronized void accept(Page page)
    {
        // reset the assignment lists
        for (IntList partitionAssignment : partitionAssignments) {
            partitionAssignment.clear();
        }

        // assign each row to a partition
        for (int position = 0; position < page.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(page, position);
            partitionAssignments[partition].add(position);
        }

        // build a page for each partition
        for (int partition = 0; partition < buffers.size(); partition++) {
            IntArrayList positions = partitionAssignments[partition];
            if (!positions.isEmpty()) {
                Page pageSplit = page.copyPositions(positions.elements(), 0, positions.size());
                memoryManager.updateMemoryUsage(pageSplit.getRetainedSizeInBytes());
                buffers.get(partition).accept(new PageReference(pageSplit, 1, onPageReleased));
            }
        }
    }

    @Override
    public ListenableFuture<?> waitForWriting()
    {
        return memoryManager.getNotFullFuture();
    }
}
