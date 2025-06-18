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
package io.trino.spiller;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.PartitionFunction;
import io.trino.operator.SpillContext;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class GenericPartitioningSpiller
        implements PartitioningSpiller
{
    private final List<Type> types;
    private final PartitionFunction partitionFunction;
    private final Closer closer = Closer.create();
    private final SingleStreamSpillerFactory spillerFactory;
    private final SpillContext spillContext;
    private final AggregatedMemoryContext memoryContext;

    private final List<PageBuilder> pageBuilders;
    private final List<Optional<SingleStreamSpiller>> spillers;

    private boolean readingStarted;
    private final Set<Integer> spilledPartitions = new HashSet<>();

    public GenericPartitioningSpiller(
            List<Type> types,
            PartitionFunction partitionFunction,
            SpillContext spillContext,
            AggregatedMemoryContext memoryContext,
            SingleStreamSpillerFactory spillerFactory)
    {
        requireNonNull(spillContext, "spillContext is null");

        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.spillContext = closer.register(requireNonNull(spillContext, "spillContext is null"));

        requireNonNull(memoryContext, "memoryContext is null");
        closer.register(memoryContext::close);
        this.memoryContext = memoryContext;
        int partitionCount = partitionFunction.partitionCount();

        ImmutableList.Builder<PageBuilder> pageBuilders = ImmutableList.builder();
        spillers = new ArrayList<>(partitionCount);
        for (int partition = 0; partition < partitionCount; partition++) {
            pageBuilders.add(new PageBuilder(types));
            spillers.add(Optional.empty());
        }
        this.pageBuilders = pageBuilders.build();
    }

    @Override
    public synchronized Iterator<Page> getSpilledPages(int partition)
    {
        readingStarted = true;
        getFutureValue(flush(partition));
        spilledPartitions.remove(partition);
        return getSpiller(partition).getSpilledPages();
    }

    @Override
    public synchronized void verifyAllPartitionsRead()
    {
        verify(spilledPartitions.isEmpty(), "Some partitions were spilled but not read: %s", spilledPartitions);
    }

    @Override
    public synchronized PartitioningSpillResult partitionAndSpill(Page page, IntPredicate spillPartitionMask)
    {
        requireNonNull(page, "page is null");
        requireNonNull(spillPartitionMask, "spillPartitionMask is null");
        checkArgument(page.getChannelCount() == types.size(), "Wrong page channel count, expected %s but got %s", types.size(), page.getChannelCount());

        checkState(!readingStarted, "reading already started");
        IntArrayList unspilledPositions = partitionPage(page, spillPartitionMask);
        ListenableFuture<DataSize> future = flushFullBuilders();

        return new PartitioningSpillResult(future, page.getPositions(unspilledPositions.elements(), 0, unspilledPositions.size()));
    }

    private synchronized IntArrayList partitionPage(Page page, IntPredicate spillPartitionMask)
    {
        IntArrayList unspilledPositions = new IntArrayList();

        for (int position = 0; position < page.getPositionCount(); position++) {
            int partition = partitionFunction.getPartition(page, position);

            if (!spillPartitionMask.test(partition)) {
                unspilledPositions.add(position);
                continue;
            }

            spilledPartitions.add(partition);
            PageBuilder pageBuilder = pageBuilders.get(partition);
            pageBuilder.declarePosition();
            for (int channel = 0; channel < types.size(); channel++) {
                Type type = types.get(channel);
                type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
            }
        }

        return unspilledPositions;
    }

    private ListenableFuture<DataSize> flushFullBuilders()
    {
        return flush(PageBuilder::isFull);
    }

    @VisibleForTesting
    ListenableFuture<DataSize> flush()
    {
        return flush(pageBuilder -> true);
    }

    private synchronized ListenableFuture<DataSize> flush(Predicate<PageBuilder> flushCondition)
    {
        requireNonNull(flushCondition, "flushCondition is null");
        ImmutableList.Builder<ListenableFuture<DataSize>> futures = ImmutableList.builder();

        for (int partition = 0; partition < spillers.size(); partition++) {
            PageBuilder pageBuilder = pageBuilders.get(partition);
            if (flushCondition.test(pageBuilder)) {
                futures.add(flush(partition));
            }
        }

        return Futures.transform(Futures.allAsList(futures.build()),
                result -> {
                    long totalBytes = 0;
                    for (DataSize size : result) {
                        totalBytes += size.toBytes();
                    }
                    return DataSize.ofBytes(totalBytes);
                }, directExecutor());
    }

    private synchronized ListenableFuture<DataSize> flush(int partition)
    {
        PageBuilder pageBuilder = pageBuilders.get(partition);
        if (pageBuilder.isEmpty()) {
            return immediateFuture(DataSize.ofBytes(0));
        }
        Page page = pageBuilder.build();
        pageBuilder.reset();
        return getSpiller(partition).spill(page);
    }

    private synchronized SingleStreamSpiller getSpiller(int partition)
    {
        Optional<SingleStreamSpiller> spiller = spillers.get(partition);
        if (spiller.isEmpty()) {
            spiller = Optional.of(closer.register(spillerFactory.create(types, spillContext, memoryContext.newLocalMemoryContext(GenericPartitioningSpiller.class.getSimpleName()))));
            spillers.set(partition, spiller);
        }
        return spiller.get();
    }

    @Override
    public synchronized void close()
            throws IOException
    {
        closer.close();
    }
}
