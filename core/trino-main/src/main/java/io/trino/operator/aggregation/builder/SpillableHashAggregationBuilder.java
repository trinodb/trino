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
package io.trino.operator.aggregation.builder;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.HashCollisionsCounter;
import io.trino.operator.MergeHashSort;
import io.trino.operator.OperatorContext;
import io.trino.operator.Work;
import io.trino.operator.WorkProcessor;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spiller.Spiller;
import io.trino.spiller.SpillerFactory;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.type.BlockTypeOperators;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.operator.Operator.NOT_BLOCKED;
import static java.lang.Math.max;

public class SpillableHashAggregationBuilder
        implements HashAggregationBuilder
{
    private InMemoryHashAggregationBuilder hashAggregationBuilder;
    private final SpillerFactory spillerFactory;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final AggregationNode.Step step;
    private final int expectedGroups;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final Optional<Integer> hashChannel;
    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final LocalMemoryContext localRevocableMemoryContext;
    private final long memoryLimitForMerge;
    private final long memoryLimitForMergeWithMemory;
    private Optional<Spiller> spiller = Optional.empty();
    private Optional<MergingHashAggregationBuilder> merger = Optional.empty();
    private Optional<MergeHashSort> mergeHashSort = Optional.empty();
    private ListenableFuture<Void> spillInProgress = immediateVoidFuture();
    private final JoinCompiler joinCompiler;
    private final BlockTypeOperators blockTypeOperators;

    // todo get rid of that and only use revocable memory
    private long emptyHashAggregationBuilderSize;

    private long hashCollisions;
    private double expectedHashCollisions;
    private boolean producingOutput;

    public SpillableHashAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            AggregationNode.Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            DataSize memoryLimitForMerge,
            DataSize memoryLimitForMergeWithMemory,
            SpillerFactory spillerFactory,
            JoinCompiler joinCompiler,
            BlockTypeOperators blockTypeOperators)
    {
        this.accumulatorFactories = accumulatorFactories;
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.groupByTypes = groupByTypes;
        this.groupByChannels = groupByChannels;
        this.hashChannel = hashChannel;
        this.operatorContext = operatorContext;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.localRevocableMemoryContext = operatorContext.localRevocableMemoryContext();
        this.memoryLimitForMerge = memoryLimitForMerge.toBytes();
        this.memoryLimitForMergeWithMemory = memoryLimitForMergeWithMemory.toBytes();
        this.spillerFactory = spillerFactory;
        this.joinCompiler = joinCompiler;
        this.blockTypeOperators = blockTypeOperators;

        rebuildHashAggregationBuilder();
    }

    @Override
    public Work<?> processPage(Page page)
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");
        // hashAggregationBuilder is constructed with non yielding UpdateMemory instance.
        // Therefore the processing of the returned Work should always be true.
        // It is not possible to spill during processing of a page.
        return hashAggregationBuilder.processPage(page);
    }

    @Override
    public void updateMemory()
    {
        checkState(spillInProgress.isDone());

        if (producingOutput) {
            localRevocableMemoryContext.setBytes(0);
            localUserMemoryContext.setBytes(hashAggregationBuilder.getSizeInMemory());
        }
        else {
            localUserMemoryContext.setBytes(emptyHashAggregationBuilderSize);
            localRevocableMemoryContext.setBytes(hashAggregationBuilder.getSizeInMemory() - emptyHashAggregationBuilderSize);
        }
    }

    @Override
    public void recordHashCollisions(HashCollisionsCounter hashCollisionsCounter)
    {
        hashCollisionsCounter.recordHashCollision(hashCollisions, expectedHashCollisions);
        hashCollisions = 0;
        expectedHashCollisions = 0;
    }

    @Override
    public boolean isFull()
    {
        return false;
    }

    private boolean hasPreviousSpillCompletedSuccessfully()
    {
        if (spillInProgress.isDone()) {
            // check for exception from previous spill for early failure
            getFutureValue(spillInProgress);
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public ListenableFuture<Void> startMemoryRevoke()
    {
        if (producingOutput) {
            // all revocable memory has been released in buildResult method
            verify(localRevocableMemoryContext.getBytes() == 0);
            return NOT_BLOCKED;
        }

        return spillToDisk();
    }

    @Override
    public void finishMemoryRevoke()
    {
        updateMemory();
    }

    private boolean shouldMergeWithMemory(long memorySize)
    {
        return memorySize < memoryLimitForMergeWithMemory;
    }

    @Override
    public WorkProcessor<Page> buildResult()
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");
        producingOutput = true;

        // Convert revocable memory to user memory as returned WorkProcessor holds on to memory so we no longer can revoke.
        if (localRevocableMemoryContext.getBytes() > 0) {
            long currentRevocableBytes = localRevocableMemoryContext.getBytes();
            localRevocableMemoryContext.setBytes(0);
            if (!localUserMemoryContext.trySetBytes(localUserMemoryContext.getBytes() + currentRevocableBytes)) {
                // TODO: this might fail (even though we have just released memory), but we don't
                // have a proper way to atomically convert memory reservations
                localRevocableMemoryContext.setBytes(currentRevocableBytes);
                // spill since revocable memory could not be converted to user memory immediately
                // TODO: this should be asynchronous
                getFutureValue(spillToDisk());
                updateMemory();
            }
        }

        if (spiller.isEmpty()) {
            return hashAggregationBuilder.buildResult();
        }

        if (shouldMergeWithMemory(getSizeInMemoryWhenUnspilling())) {
            return mergeFromDiskAndMemory();
        }
        else {
            getFutureValue(spillToDisk());
            return mergeFromDisk();
        }
    }

    /**
     * Estimates future memory usage, during unspilling.
     */
    private long getSizeInMemoryWhenUnspilling()
    {
        // TODO: we could skip memory reservation for hashAggregationBuilder.getGroupIdsSortingSize()
        // if before building result from hashAggregationBuilder we would convert it to "read only" version.
        // Read only version of GroupByHash from hashAggregationBuilder could be compacted by dropping
        // most of it's field, freeing up some memory that could be used for sorting.
        return hashAggregationBuilder.getSizeInMemory() + hashAggregationBuilder.getGroupIdsSortingSize();
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            if (hashAggregationBuilder != null) {
                closer.register(hashAggregationBuilder::close);
            }
            merger.ifPresent(closer::register);
            spiller.ifPresent(closer::register);
            mergeHashSort.ifPresent(closer::register);
            closer.register(() -> localUserMemoryContext.setBytes(0));
            closer.register(() -> localRevocableMemoryContext.setBytes(0));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ListenableFuture<Void> spillToDisk()
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");
        hashAggregationBuilder.setOutputPartial();

        if (spiller.isEmpty()) {
            spiller = Optional.of(spillerFactory.create(
                    hashAggregationBuilder.buildTypes(),
                    operatorContext.getSpillContext(),
                    operatorContext.newAggregateSystemMemoryContext()));
        }

        // start spilling process with current content of the hashAggregationBuilder builder...
        spillInProgress = spiller.get().spill(hashAggregationBuilder.buildHashSortedResult().iterator());
        // ... and immediately create new hashAggregationBuilder so effectively memory ownership
        // over hashAggregationBuilder is transferred from this thread to a spilling thread
        rebuildHashAggregationBuilder();

        return spillInProgress;
    }

    private WorkProcessor<Page> mergeFromDiskAndMemory()
    {
        checkState(spiller.isPresent());

        hashAggregationBuilder.setOutputPartial();
        mergeHashSort = Optional.of(new MergeHashSort(operatorContext.newAggregateSystemMemoryContext(), blockTypeOperators));

        WorkProcessor<Page> mergedSpilledPages = mergeHashSort.get().merge(
                groupByTypes,
                hashAggregationBuilder.buildIntermediateTypes(),
                ImmutableList.<WorkProcessor<Page>>builder()
                        .addAll(spiller.get().getSpills().stream()
                                .map(WorkProcessor::fromIterator)
                                .collect(toImmutableList()))
                        .add(hashAggregationBuilder.buildHashSortedResult())
                        .build(),
                operatorContext.getDriverContext().getYieldSignal());

        return mergeSortedPages(mergedSpilledPages, max(memoryLimitForMerge - memoryLimitForMergeWithMemory, 1L));
    }

    private WorkProcessor<Page> mergeFromDisk()
    {
        checkState(spiller.isPresent());

        mergeHashSort = Optional.of(new MergeHashSort(operatorContext.newAggregateSystemMemoryContext(), blockTypeOperators));

        WorkProcessor<Page> mergedSpilledPages = mergeHashSort.get().merge(
                groupByTypes,
                hashAggregationBuilder.buildIntermediateTypes(),
                spiller.get().getSpills().stream()
                        .map(WorkProcessor::fromIterator)
                        .collect(toImmutableList()),
                operatorContext.getDriverContext().getYieldSignal());

        return mergeSortedPages(mergedSpilledPages, memoryLimitForMerge);
    }

    private WorkProcessor<Page> mergeSortedPages(WorkProcessor<Page> sortedPages, long memoryLimitForMerge)
    {
        merger = Optional.of(new MergingHashAggregationBuilder(
                accumulatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                hashChannel,
                operatorContext,
                sortedPages,
                operatorContext.aggregateSystemMemoryContext(),
                memoryLimitForMerge,
                hashAggregationBuilder.getKeyChannels(),
                joinCompiler,
                blockTypeOperators));

        return merger.get().buildResult();
    }

    private void rebuildHashAggregationBuilder()
    {
        if (hashAggregationBuilder != null) {
            hashCollisions += hashAggregationBuilder.getHashCollisions();
            expectedHashCollisions += hashAggregationBuilder.getExpectedHashCollisions();
            hashAggregationBuilder.close();
        }

        hashAggregationBuilder = new InMemoryHashAggregationBuilder(
                accumulatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                hashChannel,
                operatorContext,
                Optional.of(DataSize.succinctBytes(0)),
                joinCompiler,
                blockTypeOperators,
                () -> {
                    updateMemory();
                    // TODO: Support GroupByHash yielding in spillable hash aggregation (https://github.com/trinodb/trino/issues/460)
                    return true;
                });
        emptyHashAggregationBuilderSize = hashAggregationBuilder.getSizeInMemory();
    }
}
