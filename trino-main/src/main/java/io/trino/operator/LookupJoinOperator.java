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
package io.prestosql.operator;

import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.prestosql.memory.context.MemoryTrackingContext;
import io.prestosql.operator.JoinProbe.JoinProbeFactory;
import io.prestosql.operator.LookupJoinOperators.JoinType;
import io.prestosql.operator.LookupSourceProvider.LookupSourceLease;
import io.prestosql.operator.PartitionedConsumption.Partition;
import io.prestosql.operator.WorkProcessor.ProcessState;
import io.prestosql.operator.WorkProcessor.Transformation;
import io.prestosql.operator.WorkProcessor.TransformationState;
import io.prestosql.operator.WorkProcessorOperatorAdapter.AdapterWorkProcessorOperator;
import io.prestosql.operator.exchange.LocalPartitionGenerator;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.PartitioningSpiller;
import io.prestosql.spiller.PartitioningSpiller.PartitioningSpillResult;
import io.prestosql.spiller.PartitioningSpillerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.Iterators.singletonIterator;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static io.airlift.concurrent.MoreFutures.getDone;
import static io.prestosql.operator.LookupJoinOperators.JoinType.FULL_OUTER;
import static io.prestosql.operator.LookupJoinOperators.JoinType.PROBE_OUTER;
import static io.prestosql.operator.Operator.NOT_BLOCKED;
import static io.prestosql.operator.PartitionedLookupSourceFactory.NO_SPILL_EPOCH;
import static io.prestosql.operator.WorkProcessor.TransformationState.blocked;
import static io.prestosql.operator.WorkProcessor.TransformationState.finished;
import static io.prestosql.operator.WorkProcessor.TransformationState.needsMoreData;
import static io.prestosql.operator.WorkProcessor.TransformationState.ofResult;
import static io.prestosql.operator.WorkProcessor.TransformationState.yield;
import static io.prestosql.operator.WorkProcessor.flatten;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

public class LookupJoinOperator
        implements AdapterWorkProcessorOperator
{
    private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;
    private final PageBuffer pageBuffer;
    private final WorkProcessor<Page> pages;
    private final SpillingJoinProcessor joinProcessor;
    private final JoinStatisticsCounter statisticsCounter;

    LookupJoinOperator(List<Type> probeTypes,
            List<Type> buildOutputTypes,
            JoinType joinType,
            LookupSourceFactory lookupSourceFactory,
            JoinProbeFactory joinProbeFactory,
            Runnable afterClose,
            OptionalInt lookupJoinsCount,
            HashGenerator hashGenerator,
            PartitioningSpillerFactory partitioningSpillerFactory,
            ProcessorContext processorContext,
            Optional<WorkProcessor<Page>> sourcePages)
    {
        this.statisticsCounter = new JoinStatisticsCounter(joinType);
        lookupSourceProviderFuture = lookupSourceFactory.createLookupSourceProvider();
        pageBuffer = new PageBuffer(lookupSourceProviderFuture);
        joinProcessor = new SpillingJoinProcessor(
                processorContext,
                afterClose,
                lookupJoinsCount,
                probeTypes,
                buildOutputTypes,
                joinType,
                hashGenerator,
                joinProbeFactory,
                lookupSourceFactory,
                lookupSourceProviderFuture,
                statisticsCounter,
                partitioningSpillerFactory,
                sourcePages.orElse(pageBuffer.pages()));
        pages = flatten(WorkProcessor.create(joinProcessor));
    }

    @Override
    public Optional<OperatorInfo> getOperatorInfo()
    {
        return Optional.of(statisticsCounter.get());
    }

    @Override
    public boolean needsInput()
    {
        return lookupSourceProviderFuture.isDone() && pageBuffer.isEmpty() && !pageBuffer.isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        pageBuffer.add(page);
    }

    @Override
    public void finish()
    {
        pageBuffer.finish();
    }

    @Override
    public WorkProcessor<Page> getOutputPages()
    {
        return pages;
    }

    @Override
    public void close()
    {
        joinProcessor.close();
    }

    private static class PageJoiner
            implements Transformation<Page, Page>
    {
        private final List<Type> probeTypes;
        private final JoinProbeFactory joinProbeFactory;
        private final ListenableFuture<LookupSourceProvider> lookupSourceProviderFuture;
        private final Optional<PartitioningSpillerFactory> partitioningSpillerFactory;
        private final SpillContext spillContext;
        private final MemoryTrackingContext memoryTrackingContext;
        private final JoinStatisticsCounter statisticsCounter;
        private final DriverYieldSignal yieldSignal;
        private final Iterator<SavedRow> savedRows;
        private final Supplier<LocalPartitionGenerator> partitionGenerator;
        private final LookupJoinPageBuilder pageBuilder;
        private final Map<Integer, SavedRow> spilledRows = new HashMap<>();
        private final boolean probeOnOuterSide;

        @Nullable
        private LookupSourceProvider lookupSourceProvider;
        @Nullable
        private JoinProbe probe;
        private long spillEpoch = NO_SPILL_EPOCH;
        private long joinPosition = -1;
        private int joinSourcePositions;
        private boolean currentProbePositionProducedRow;

        private Optional<PartitioningSpiller> spiller = Optional.empty();
        private ListenableFuture<?> spillInProgress = NOT_BLOCKED;

        private PageJoiner(
                ProcessorContext processorContext,
                List<Type> probeTypes,
                List<Type> buildOutputTypes,
                JoinType joinType,
                HashGenerator hashGenerator,
                JoinProbeFactory joinProbeFactory,
                LookupSourceFactory lookupSourceFactory,
                ListenableFuture<LookupSourceProvider> lookupSourceProvider,
                Optional<PartitioningSpillerFactory> partitioningSpillerFactory,
                JoinStatisticsCounter statisticsCounter,
                Iterator<SavedRow> savedRows)
        {
            requireNonNull(processorContext, "processorContext is null");
            this.probeTypes = requireNonNull(probeTypes, "probeTypes is null");
            this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
            this.lookupSourceProviderFuture = requireNonNull(lookupSourceProvider, "lookupSourceProvider is null");
            this.partitioningSpillerFactory = requireNonNull(partitioningSpillerFactory, "partitioningSpillerFactory is null");
            this.spillContext = processorContext.getSpillContext();
            this.memoryTrackingContext = processorContext.getMemoryTrackingContext();
            this.statisticsCounter = requireNonNull(statisticsCounter, "statisticsCounter is null");
            this.yieldSignal = processorContext.getDriverYieldSignal();
            this.savedRows = requireNonNull(savedRows, "savedRows is null");
            this.partitionGenerator = memoize(() -> new LocalPartitionGenerator(hashGenerator, lookupSourceFactory.partitions()));
            this.pageBuilder = new LookupJoinPageBuilder(buildOutputTypes);

            // Cannot use switch case here, because javac will synthesize an inner class and cause IllegalAccessError
            probeOnOuterSide = joinType == PROBE_OUTER || joinType == FULL_OUTER;
        }

        @Override
        public TransformationState<Page> process(@Nullable Page probePage)
        {
            boolean finishing = probePage == null;

            if (probe == null) {
                if (!finishing) {
                    // create new probe for next probe page
                    probe = joinProbeFactory.createJoinProbe(probePage);
                    // force spill state check for new probe
                    spillEpoch = NO_SPILL_EPOCH;
                }
                else if (savedRows.hasNext()) {
                    // create probe from next saved row
                    restoreProbe(savedRows.next());
                }
                else if (!spillInProgress.isDone()) {
                    // block on remaining spill before finishing
                    return blocked(spillInProgress);
                }
                else {
                    checkSuccess(spillInProgress, "spilling failed");
                    close();
                    return finished();
                }
            }
            verify(probe != null, "no probe to work with");

            if (lookupSourceProvider == null) {
                if (!lookupSourceProviderFuture.isDone()) {
                    return blocked(lookupSourceProviderFuture);
                }

                lookupSourceProvider = requireNonNull(getDone(lookupSourceProviderFuture));
                statisticsCounter.updateLookupSourcePositions(lookupSourceProvider.withLease(
                        lookupSourceLease -> lookupSourceLease.getLookupSource().getJoinPositionCount()));
            }

            // Process probe or detect spill state change. Since we update spillEpoch only later, spill
            // state change detection is idempotent.
            Optional<SpillInfoSnapshot> spillInfoSnapshotIfSpillChanged = processProbe();

            if (spillInfoSnapshotIfSpillChanged.isPresent()) {
                if (!spillInProgress.isDone()) {
                    // block on previous spill
                    return blocked(spillInProgress);
                }
                checkSuccess(spillInProgress, "spilling failed");

                // flush any remaining output page for current probe
                if (!pageBuilder.isEmpty()) {
                    return ofResult(buildOutputPage(), false);
                }

                spillJoinProbe(spillInfoSnapshotIfSpillChanged.get());
            }

            if (!probe.isFinished()) {
                // processProbe() returns when pageBuilder is full or yield signal is triggered.

                if (pageBuilder.isFull()) {
                    return ofResult(buildOutputPage(), false);
                }

                return yield();
            }

            if (!pageBuilder.isEmpty() || finishing) {
                // flush the current page (possibly empty one) and reset probe
                Page outputPage = buildOutputPage();
                probe = null;
                return ofResult(outputPage, !finishing);
            }

            probe = null;
            return needsMoreData();
        }

        private Optional<SpillInfoSnapshot> processProbe()
        {
            return lookupSourceProvider.withLease(lookupSourceLease -> {
                if (spillEpoch != lookupSourceLease.spillEpoch()) {
                    // Spill state changed
                    return Optional.of(SpillInfoSnapshot.from(lookupSourceLease));
                }

                // Spill state didn't change, so process as usual.
                processProbe(lookupSourceLease.getLookupSource());
                return Optional.empty();
            });
        }

        private void processProbe(LookupSource lookupSource)
        {
            do {
                if (probe.getPosition() >= 0) {
                    if (!joinCurrentPosition(lookupSource, yieldSignal)) {
                        break;
                    }
                    if (!currentProbePositionProducedRow) {
                        currentProbePositionProducedRow = true;
                        if (!outerJoinCurrentPosition()) {
                            break;
                        }
                    }
                    statisticsCounter.recordProbe(joinSourcePositions);
                }
                if (!advanceProbePosition(lookupSource)) {
                    break;
                }
            }
            while (!yieldSignal.isSet());
        }

        /**
         * Produce rows matching join condition for the current probe position. If this method was called previously
         * for the current probe position, calling this again will produce rows that wasn't been produced in previous
         * invocations.
         *
         * @return true if all eligible rows have been produced; false otherwise
         */
        private boolean joinCurrentPosition(LookupSource lookupSource, DriverYieldSignal yieldSignal)
        {
            // while we have a position on lookup side to join against...
            while (joinPosition >= 0) {
                if (lookupSource.isJoinPositionEligible(joinPosition, probe.getPosition(), probe.getPage())) {
                    currentProbePositionProducedRow = true;

                    pageBuilder.appendRow(probe, lookupSource, joinPosition);
                    joinSourcePositions++;
                }

                // get next position on lookup side for this probe row
                joinPosition = lookupSource.getNextJoinPosition(joinPosition, probe.getPosition(), probe.getPage());

                if (yieldSignal.isSet() || pageBuilder.isFull()) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Produce a row for the current probe position, if it doesn't match any row on lookup side and this is an outer join.
         *
         * @return whether pageBuilder can still not fill
         */
        private boolean outerJoinCurrentPosition()
        {
            if (probeOnOuterSide) {
                pageBuilder.appendNullForBuild(probe);
                return !pageBuilder.isFull();
            }
            return true;
        }

        /**
         * @return whether there are more positions on probe side
         */
        private boolean advanceProbePosition(LookupSource lookupSource)
        {
            if (!probe.advanceNextPosition()) {
                return false;
            }

            // update join position
            joinPosition = probe.getCurrentJoinPosition(lookupSource);
            // reset row join state for next row
            joinSourcePositions = 0;
            currentProbePositionProducedRow = false;
            return true;
        }

        private void spillJoinProbe(SpillInfoSnapshot spillInfoSnapshot)
        {
            verifyNotNull(probe, "probe is null");
            verify(pageBuilder.isEmpty(), "pageBuilder must be flushed before spill");
            checkArgument(spillInfoSnapshot.getSpillEpoch() > NO_SPILL_EPOCH, "invalid spill epoch");

            /*
             * Spill state changed. All probe rows that were not processed yet should be treated as regular input (and be partially spilled).
             * If current row maps to the now-spilled partition, it needs to be saved for later. If it maps to a partition still in memory, it
             * should be added together with not-yet-processed rows. In either case we need to resume processing the row starting at its
             * current position in the lookup source.
             */
            if (probe.getPosition() < 0) {
                // Processing of the page hasn't been started yet.
                probe = joinProbeFactory.createJoinProbe(spillAndMaskSpilledPositions(probe.getPage(), spillInfoSnapshot));
            }
            else {
                int currentRowPartition = partitionGenerator.get().getPartition(probe.getPage(), probe.getPosition());
                boolean currentRowSpilled = spillInfoSnapshot.getSpillMask().test(currentRowPartition);

                if (currentRowSpilled) {
                    spilledRows.merge(
                            currentRowPartition,
                            new SavedRow(probe.getPage(), probe.getPosition(), getJoinPositionWithinPartition(), currentProbePositionProducedRow, joinSourcePositions),
                            (oldValue, newValue) -> {
                                throw new IllegalStateException(format("Partition %s is already spilled", currentRowPartition));
                            });
                    Page remaining = pageTail(probe.getPage(), probe.getPosition() + 1);

                    // create probe starting from next position
                    probe = joinProbeFactory.createJoinProbe(spillAndMaskSpilledPositions(remaining, spillInfoSnapshot));
                    resetProbeRowState();
                }
                else {
                    Page remaining = pageTail(probe.getPage(), probe.getPosition());
                    // create probe starting from current position and keep current row join state
                    probe = joinProbeFactory.createJoinProbe(spillAndMaskSpilledPositions(remaining, spillInfoSnapshot));
                    verify(probe.advanceNextPosition());
                }
            }

            spillEpoch = spillInfoSnapshot.getSpillEpoch();
        }

        private Page spillAndMaskSpilledPositions(Page page, SpillInfoSnapshot spillInfoSnapshot)
        {
            checkSuccess(spillInProgress, "spilling failed");

            if (spiller.isEmpty()) {
                checkState(partitioningSpillerFactory.isPresent(), "Spiller factory is not present");
                spiller = Optional.of(partitioningSpillerFactory.get().create(
                        probeTypes,
                        partitionGenerator.get(),
                        spillContext.newLocalSpillContext(),
                        memoryTrackingContext.newAggregateSystemMemoryContext()));
            }

            PartitioningSpillResult result = spiller.get().partitionAndSpill(page, spillInfoSnapshot.getSpillMask());
            spillInProgress = result.getSpillingFuture();
            return result.getRetained();
        }

        private long getJoinPositionWithinPartition()
        {
            if (joinPosition >= 0) {
                return lookupSourceProvider.withLease(lookupSourceLease -> lookupSourceLease.getLookupSource().joinPositionWithinPartition(joinPosition));
            }
            else {
                return -1;
            }
        }

        private Page buildOutputPage()
        {
            verifyNotNull(probe);
            Page outputPage = pageBuilder.build(probe);
            pageBuilder.reset();
            return outputPage;
        }

        private void resetProbeRowState()
        {
            joinPosition = -1;
            joinSourcePositions = 0;
            currentProbePositionProducedRow = false;
        }

        private void restoreProbe(SavedRow savedRow)
        {
            probe = joinProbeFactory.createJoinProbe(savedRow.row);
            verify(probe.advanceNextPosition());
            joinPosition = savedRow.joinPositionWithinPartition;
            currentProbePositionProducedRow = savedRow.currentProbePositionProducedRow;
            joinSourcePositions = savedRow.joinSourcePositions;
            spillEpoch = NO_SPILL_EPOCH; // irrelevant
        }

        private Page pageTail(Page currentPage, int startAtPosition)
        {
            verify(currentPage.getPositionCount() - startAtPosition >= 0);
            return currentPage.getRegion(startAtPosition, currentPage.getPositionCount() - startAtPosition);
        }

        private Map<Integer, SavedRow> getSpilledRows()
        {
            return spilledRows;
        }

        private Optional<PartitioningSpiller> getSpiller()
        {
            return spiller;
        }

        private void close()
        {
            pageBuilder.reset();
            addSuccessCallback(lookupSourceProviderFuture, LookupSourceProvider::close);
        }
    }

    private static class SpillingJoinProcessor
            implements WorkProcessor.Process<WorkProcessor<Page>>
    {
        private final ProcessorContext processorContext;
        private final Runnable afterClose;
        private final OptionalInt lookupJoinsCount;
        private final List<Type> probeTypes;
        private final List<Type> buildOutputTypes;
        private final JoinType joinType;
        private final HashGenerator hashGenerator;
        private final JoinProbeFactory joinProbeFactory;
        private final LookupSourceFactory lookupSourceFactory;
        private final JoinStatisticsCounter statisticsCounter;
        private final PageJoiner sourcePagesJoiner;
        private final WorkProcessor<Page> joinedSourcePages;

        private boolean closed;

        @Nullable
        private ListenableFuture<PartitionedConsumption<Supplier<LookupSource>>> partitionedConsumption;
        @Nullable
        private Iterator<Partition<Supplier<LookupSource>>> lookupPartitions;
        @Nullable
        private Partition<Supplier<LookupSource>> previousPartition;
        @Nullable
        private ListenableFuture<Supplier<LookupSource>> previousPartitionLookupSource;

        private SpillingJoinProcessor(
                ProcessorContext processorContext,
                Runnable afterClose,
                OptionalInt lookupJoinsCount,
                List<Type> probeTypes,
                List<Type> buildOutputTypes,
                JoinType joinType,
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
            this.hashGenerator = requireNonNull(hashGenerator, "hashGenerator is null");
            this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
            this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
            this.statisticsCounter = requireNonNull(statisticsCounter, "statisticsCounter is null");
            sourcePagesJoiner = new PageJoiner(
                    processorContext,
                    probeTypes,
                    buildOutputTypes,
                    joinType,
                    hashGenerator,
                    joinProbeFactory,
                    lookupSourceFactory,
                    lookupSourceProvider,
                    Optional.of(partitioningSpillerFactory),
                    statisticsCounter,
                    emptyIterator());
            joinedSourcePages = sourcePages.transform(sourcePagesJoiner);
        }

        @Override
        public ProcessState<WorkProcessor<Page>> process()
        {
            if (!joinedSourcePages.isFinished()) {
                return ProcessState.ofResult(joinedSourcePages);
            }

            if (partitionedConsumption == null) {
                partitionedConsumption = lookupSourceFactory.finishProbeOperator(lookupJoinsCount);
                return ProcessState.blocked(partitionedConsumption);
            }

            if (lookupPartitions == null) {
                lookupPartitions = getDone(partitionedConsumption).beginConsumption();
            }

            if (previousPartition != null) {
                // If we had no rows for the previous spill partition, we would finish before it is unspilled.
                // Partition must be loaded before it can be released. // TODO remove this constraint
                if (!previousPartitionLookupSource.isDone()) {
                    return ProcessState.blocked(previousPartitionLookupSource);
                }

                previousPartition.release();
                previousPartition = null;
                previousPartitionLookupSource = null;
            }

            if (!lookupPartitions.hasNext()) {
                close();
                return ProcessState.finished();
            }

            Partition<Supplier<LookupSource>> partition = lookupPartitions.next();
            previousPartition = partition;
            previousPartitionLookupSource = partition.load();

            return ProcessState.ofResult(joinUnspilledPages(partition));
        }

        private WorkProcessor<Page> joinUnspilledPages(Partition<Supplier<LookupSource>> partition)
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
                    hashGenerator,
                    joinProbeFactory,
                    lookupSourceFactory,
                    unspilledLookupSourceProvider,
                    Optional.empty(),
                    statisticsCounter,
                    savedRow));
        }

        private void close()
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
    }

    // This class must be public because LookupJoinOperator is isolated.
    public static class SpillInfoSnapshot
    {
        private final long spillEpoch;
        private final IntPredicate spillMask;

        public SpillInfoSnapshot(long spillEpoch, IntPredicate spillMask)
        {
            this.spillEpoch = spillEpoch;
            this.spillMask = requireNonNull(spillMask, "spillMask is null");
        }

        public static SpillInfoSnapshot from(LookupSourceLease lookupSourceLease)
        {
            return new SpillInfoSnapshot(
                    lookupSourceLease.spillEpoch(),
                    lookupSourceLease.getSpillMask());
        }

        public long getSpillEpoch()
        {
            return spillEpoch;
        }

        public IntPredicate getSpillMask()
        {
            return spillMask;
        }
    }

    // This class must be public because LookupJoinOperator is isolated.
    public static class SavedRow
    {
        /**
         * A page with exactly one {@link Page#getPositionCount}, representing saved row.
         */
        public final Page row;

        /**
         * A snapshot of {@link PageJoiner#joinPosition} "de-partitioned", i.e. {@link PageJoiner#joinPosition} is a join position
         * with respect to (potentially) partitioned lookup source, while this value is a join position with respect to containing partition.
         */
        public final long joinPositionWithinPartition;

        /**
         * A snapshot of {@link PageJoiner#currentProbePositionProducedRow}
         */
        public final boolean currentProbePositionProducedRow;

        /**
         * A snapshot of {@link PageJoiner#joinSourcePositions}
         */
        public final int joinSourcePositions;

        public SavedRow(Page page, int position, long joinPositionWithinPartition, boolean currentProbePositionProducedRow, int joinSourcePositions)
        {
            this.row = page.getSingleValuePage(position);

            this.joinPositionWithinPartition = joinPositionWithinPartition;
            this.currentProbePositionProducedRow = currentProbePositionProducedRow;
            this.joinSourcePositions = joinSourcePositions;
        }
    }
}
