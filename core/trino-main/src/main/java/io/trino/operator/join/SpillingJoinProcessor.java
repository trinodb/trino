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
import io.trino.operator.HashGenerator;
import io.trino.operator.ProcessorContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.join.JoinProbe.JoinProbeFactory;
import io.trino.operator.join.LookupJoinOperatorFactory.JoinType;
import io.trino.operator.join.PageJoiner.SavedRow;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spiller.PartitioningSpillerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
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
    private final ProcessorContext processorContext;
    private final Runnable afterClose;
    private final OptionalInt lookupJoinsCount;
    private final List<Type> probeTypes;
    private final List<Type> buildOutputTypes;
    private final JoinType joinType;
    private final boolean outputSingleMatch;
    private final boolean waitForBuild;
    private final HashGenerator hashGenerator;
    private final JoinProbeFactory joinProbeFactory;
    private final LookupSourceFactory lookupSourceFactory;
    private final ListenableFuture<LookupSourceProvider> lookupSourceProvider;
    private final JoinStatisticsCounter statisticsCounter;
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
            ProcessorContext processorContext,
            Runnable afterClose,
            OptionalInt lookupJoinsCount,
            List<Type> probeTypes,
            List<Type> buildOutputTypes,
            JoinType joinType,
            boolean outputSingleMatch,
            boolean waitForBuild,
            HashGenerator hashGenerator,
            JoinProbeFactory joinProbeFactory,
            LookupSourceFactory lookupSourceFactory,
            ListenableFuture<LookupSourceProvider> lookupSourceProvider,
            JoinStatisticsCounter statisticsCounter,
            PartitioningSpillerFactory partitioningSpillerFactory,
            WorkProcessor<Page> sourcePages)
    {
        this.processorContext = requireNonNull(processorContext, "processorContext is null");
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
        this.lookupJoinsCount = requireNonNull(lookupJoinsCount, "lookupJoinsCount is null");
        this.probeTypes = requireNonNull(probeTypes, "probeTypes is null");
        this.buildOutputTypes = requireNonNull(buildOutputTypes, "buildOutputTypes is null");
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.outputSingleMatch = outputSingleMatch;
        this.waitForBuild = waitForBuild;
        this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
        this.lookupSourceProvider = requireNonNull(lookupSourceProvider, "lookupSourceProvider is null");
        this.statisticsCounter = requireNonNull(statisticsCounter, "statisticsCounter is null");
        sourcePagesJoiner = new PageJoiner(
                processorContext,
                probeTypes,
                buildOutputTypes,
                joinType,
                outputSingleMatch,
                hashGenerator,
                joinProbeFactory,
                lookupSourceFactory,
                lookupSourceProvider,
                Optional.of(partitioningSpillerFactory),
                statisticsCounter,
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

            closer.register(sourcePagesJoiner::close);
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
            return WorkProcessor.ProcessState.blocked(lookupSourceProvider);
        }

        if (!joinedSourcePages.isFinished()) {
            return WorkProcessor.ProcessState.ofResult(joinedSourcePages);
        }

        if (partitionedConsumption == null) {
            partitionedConsumption = lookupSourceFactory.finishProbeOperator(lookupJoinsCount);
            return WorkProcessor.ProcessState.blocked(partitionedConsumption);
        }

        if (lookupPartitions == null) {
            lookupPartitions = getDone(partitionedConsumption).beginConsumption();
        }

        if (previousPartition != null) {
            // If we had no rows for the previous spill partition, we would finish before it is unspilled.
            // Partition must be loaded before it can be released. // TODO remove this constraint
            if (!previousPartitionLookupSource.isDone()) {
                return WorkProcessor.ProcessState.blocked(previousPartitionLookupSource);
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

        return unspilledInputPages.transform(new PageJoiner(
                processorContext,
                probeTypes,
                buildOutputTypes,
                joinType,
                outputSingleMatch,
                hashGenerator,
                joinProbeFactory,
                lookupSourceFactory,
                unspilledLookupSourceProvider,
                Optional.empty(),
                statisticsCounter,
                savedRow));
    }
}
