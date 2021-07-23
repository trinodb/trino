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
package io.trino.operator.join;

import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.trino.operator.WorkProcessor;
import io.trino.operator.join.DefaultPageJoiner.SavedRow;
import io.trino.operator.join.PageJoiner.PageJoinerFactory;
import io.trino.spi.Page;
import io.trino.spiller.PartitioningSpillerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;

import static com.google.common.collect.Iterators.singletonIterator;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getDone;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

public class SpillingJoinProcessor
        implements WorkProcessor.Process<WorkProcessor<Page>>
{
    private final Runnable afterClose;
    private final OptionalInt lookupJoinsCount;
    private final boolean waitForBuild;
    private final LookupSourceFactory lookupSourceFactory;
    private final ListenableFuture<LookupSourceProvider> lookupSourceProvider;
    private final PageJoinerFactory pageJoinerFactory;
    private final PageJoiner sourcePagesJoiner;
    private final WorkProcessor<Page> joinedSourcePages;

    private boolean closed;

    @Nullable
    private ListenableFuture<PartitionedConsumption<Supplier<LookupSource>>> partitionedConsumption;
    @Nullable
    private Iterator<PartitionedConsumption.Partition<Supplier<LookupSource>>> lookupPartitions;
    @Nullable
    private PartitionedConsumption.Partition<Supplier<LookupSource>> previousPartition;
    @Nullable
    private ListenableFuture<Supplier<LookupSource>> previousPartitionLookupSource;

    public SpillingJoinProcessor(
            Runnable afterClose,
            OptionalInt lookupJoinsCount,
            boolean waitForBuild,
            LookupSourceFactory lookupSourceFactory,
            ListenableFuture<LookupSourceProvider> lookupSourceProvider,
            PartitioningSpillerFactory partitioningSpillerFactory,
            PageJoinerFactory pageJoinerFactory,
            WorkProcessor<Page> sourcePages)
    {
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.waitForBuild = waitForBuild;
        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.lookupSourceProvider = requireNonNull(lookupSourceProvider, "lookupSourceProvider is null");
        this.pageJoinerFactory = requireNonNull(pageJoinerFactory, "pageJoinerFactory is null");
        sourcePagesJoiner = pageJoinerFactory.getPageJoiner(
                lookupSourceProvider,
                Optional.of(partitioningSpillerFactory),
                emptyIterator());
        joinedSourcePages = sourcePages.transform(sourcePagesJoiner);
    }

    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try (Closer closer = Closer.create()) {
            // `afterClose` must be run last.
            // Closer is documented to mimic try-with-resource, which implies close will happen in reverse order.
            closer.register(afterClose::run);

            closer.register(sourcePagesJoiner);
            sourcePagesJoiner.getSpiller().ifPresent(closer::register);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public WorkProcessor.ProcessState<WorkProcessor<Page>> process()
    {
        // wait for build side to be completed before fetching any probe data
        // TODO: fix support for probe short-circuit: https://github.com/trinodb/trino/issues/3957
        if (waitForBuild && !lookupSourceProvider.isDone()) {
            return WorkProcessor.ProcessState.blocked(asVoid(lookupSourceProvider));
        }

        if (!joinedSourcePages.isFinished()) {
            return WorkProcessor.ProcessState.ofResult(joinedSourcePages);
        }

        if (partitionedConsumption == null) {
            partitionedConsumption = lookupSourceFactory.finishProbeOperator(lookupJoinsCount);
            return WorkProcessor.ProcessState.blocked(asVoid(partitionedConsumption));
        }

        if (lookupPartitions == null) {
            lookupPartitions = getDone(partitionedConsumption).beginConsumption();
        }

        if (previousPartition != null) {
            // If we had no rows for the previous spill partition, we would finish before it is unspilled.
            // Partition must be loaded before it can be released. // TODO remove this constraint
            if (!previousPartitionLookupSource.isDone()) {
                return WorkProcessor.ProcessState.blocked(asVoid(previousPartitionLookupSource));
            }

            previousPartition.release();
            previousPartition = null;
            previousPartitionLookupSource = null;
        }

        if (!lookupPartitions.hasNext()) {
            close();
            return WorkProcessor.ProcessState.finished();
        }

        PartitionedConsumption.Partition<Supplier<LookupSource>> partition = lookupPartitions.next();
        previousPartition = partition;
        previousPartitionLookupSource = partition.load();

        return WorkProcessor.ProcessState.ofResult(joinUnspilledPages(partition));
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }

    private WorkProcessor<Page> joinUnspilledPages(PartitionedConsumption.Partition<Supplier<LookupSource>> partition)
    {
        int partitionNumber = partition.number();
        WorkProcessor<Page> unspilledInputPages = WorkProcessor.fromIterator(sourcePagesJoiner.getSpiller()
                .map(spiller -> spiller.getSpilledPages(partitionNumber))
                .orElse(emptyIterator()));
        Iterator<SavedRow> savedRow = Optional.ofNullable(sourcePagesJoiner.getSpilledRows().remove(partitionNumber))
                .map(row -> (Iterator<SavedRow>) singletonIterator(row))
                .orElse(emptyIterator());

        ListenableFuture<LookupSourceProvider> unspilledLookupSourceProvider = Futures.transform(
                partition.load(),
                supplier -> new StaticLookupSourceProvider(supplier.get()),
                directExecutor());

        return unspilledInputPages.transform(pageJoinerFactory.getPageJoiner(
                unspilledLookupSourceProvider,
                Optional.empty(),
                savedRow));
    }
}
