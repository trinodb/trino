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
package io.trino.util;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.PageWithPositionComparator;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public final class MergeSortedPages
{
    private static final PageBreakPredicate WHEN_FULL = (pageBuilder, _, _) -> pageBuilder.isFull();

    private MergeSortedPages() {}

    public static WorkProcessor<Page> mergeSortedPages(
            List<WorkProcessor<Page>> pageProducers,
            PageWithPositionComparator comparator,
            List<Type> outputTypes,
            AggregatedMemoryContext aggregatedMemoryContext,
            DriverYieldSignal yieldSignal)
    {
        return mergeSortedPages(
                pageProducers,
                comparator,
                IntStream.range(0, outputTypes.size()).boxed().collect(toImmutableList()),
                outputTypes,
                aggregatedMemoryContext,
                yieldSignal);
    }

    /**
     * Merges sorted pages into pages that are finished when they are full. Rows selected one after another
     * from the same page are copied together, so an output page can exceed the page size limit by the size
     * of such a run, which is at most one input page.
     */
    public static WorkProcessor<Page> mergeSortedPages(
            List<WorkProcessor<Page>> pageProducers,
            PageWithPositionComparator comparator,
            List<Integer> outputChannels,
            List<Type> outputTypes,
            AggregatedMemoryContext aggregatedMemoryContext,
            DriverYieldSignal yieldSignal)
    {
        return WorkProcessor.create(new MergeProcess(pageProducers, comparator, outputChannels, outputTypes, WHEN_FULL, true, false, aggregatedMemoryContext, yieldSignal));
    }

    /**
     * Merges sorted pages into pages that are finished when {@code pageBreakPredicate} asks for it.
     */
    public static WorkProcessor<Page> mergeSortedPages(
            List<WorkProcessor<Page>> pageProducers,
            PageWithPositionComparator comparator,
            List<Integer> outputChannels,
            List<Type> outputTypes,
            PageBreakPredicate pageBreakPredicate,
            boolean updateMemoryAfterEveryPosition,
            AggregatedMemoryContext aggregatedMemoryContext,
            DriverYieldSignal yieldSignal)
    {
        return WorkProcessor.create(new MergeProcess(pageProducers, comparator, outputChannels, outputTypes, pageBreakPredicate, false, updateMemoryAfterEveryPosition, aggregatedMemoryContext, yieldSignal));
    }

    @FunctionalInterface
    public interface PageBreakPredicate
    {
        /**
         * Tests whether the page that is being built has to be finished before the given row is appended to it.
         * It is called once for every row, in the output order, after all the preceding rows were appended.
         */
        boolean test(PageBuilder pageBuilder, Page page, int position);
    }

    /**
     * Merges sorted sources with a tournament tree of losers. Every internal node of the tree remembers the source
     * that lost the match played in that node, and the source that won all of its matches has the next output row.
     * Once that row is replaced with the next row of the same source, only the matches between the leaf of that source
     * and the root are replayed, which takes a single comparison per tree level. Equal rows are returned in the order
     * of their sources.
     */
    private static final class MergeProcess
            implements WorkProcessor.Process<Page>
    {
        private static final int NO_SOURCE = -1;
        // the yield signal is volatile, so it is not read for every row
        private static final int ROWS_PER_YIELD_CHECK = 1024;
        // makes a finished source lose against every row with a smaller sort key prefix without looking at its page
        private static final long FINISHED_SOURCE_PREFIX = Long.MAX_VALUE;

        private final List<WorkProcessor<Page>> sources;
        private final PageWithPositionComparator comparator;
        private final int[] outputChannels;
        private final PageBreakPredicate pageBreakPredicate;
        // a run of rows can be copied at once only when the page break predicate does not have to see every row
        private final boolean copyRuns;
        private final boolean updateMemoryAfterEveryPosition;
        private final DriverYieldSignal yieldSignal;
        private final PageBuilder pageBuilder;
        private final LocalMemoryContext outputMemoryContext;
        private final LocalMemoryContext[] sourceMemoryContexts;

        // head row of every source: its page, which is null when the source is finished or between pages,
        // position and sort key prefix
        private final Page[] pages;
        private final int[] positions;
        private final long[] sortKeyPrefixes;

        // source that lost the match played in an internal node, the root is at index 1
        // and the leaf of a source is at index sources.size() + source
        private final int[] losers;
        private boolean treeBuilt;
        private int winner;
        // source that has to load its next page before the next winner is known, at first every source in turn
        private int pendingSource;

        // rows selected one after another from the same page that are not copied to the page builder yet
        @Nullable
        private Page runPage;
        private int runStart;
        private int runLength;

        private boolean lastProcessYielded;

        private MergeProcess(
                List<WorkProcessor<Page>> pageProducers,
                PageWithPositionComparator comparator,
                List<Integer> outputChannels,
                List<Type> outputTypes,
                PageBreakPredicate pageBreakPredicate,
                boolean copyRuns,
                boolean updateMemoryAfterEveryPosition,
                AggregatedMemoryContext aggregatedMemoryContext,
                DriverYieldSignal yieldSignal)
        {
            this.sources = ImmutableList.copyOf(requireNonNull(pageProducers, "pageProducers is null"));
            checkArgument(!sources.isEmpty(), "There must be at least one page producer");
            this.comparator = requireNonNull(comparator, "comparator is null");
            this.outputChannels = Ints.toArray(requireNonNull(outputChannels, "outputChannels is null"));
            this.pageBreakPredicate = requireNonNull(pageBreakPredicate, "pageBreakPredicate is null");
            this.copyRuns = copyRuns;
            this.updateMemoryAfterEveryPosition = updateMemoryAfterEveryPosition;
            this.yieldSignal = requireNonNull(yieldSignal, "yieldSignal is null");
            this.pageBuilder = new PageBuilder(requireNonNull(outputTypes, "outputTypes is null"));
            checkArgument(this.outputChannels.length == outputTypes.size(), "outputChannels and outputTypes must be the same size");
            requireNonNull(aggregatedMemoryContext, "aggregatedMemoryContext is null");
            this.outputMemoryContext = aggregatedMemoryContext.newLocalMemoryContext(MergeSortedPages.class.getSimpleName());
            this.sourceMemoryContexts = new LocalMemoryContext[sources.size()];
            for (int source = 0; source < sourceMemoryContexts.length; source++) {
                sourceMemoryContexts[source] = aggregatedMemoryContext.newLocalMemoryContext(MergeSortedPages.class.getSimpleName());
            }
            this.pages = new Page[sources.size()];
            this.positions = new int[sources.size()];
            this.sortKeyPrefixes = new long[sources.size()];
            this.losers = new int[sources.size()];
        }

        @Override
        public ProcessState<Page> process()
        {
            if (!lastProcessYielded && yieldSignal.isSet()) {
                lastProcessYielded = true;
                return ProcessState.yielded();
            }
            // when the previous call yielded, this one makes progress even if the yield signal is still set
            lastProcessYielded = false;

            int rowsUntilYieldCheck = ROWS_PER_YIELD_CHECK;
            while (true) {
                if (pendingSource != NO_SOURCE && !loadPendingSource()) {
                    WorkProcessor<Page> source = sources.get(pendingSource);
                    if (source.isBlocked()) {
                        return ProcessState.blocked(source.getBlockedFuture());
                    }
                    return ProcessState.yielded();
                }

                int source = winner;
                Page page = pages[source];
                if (page == null) {
                    // a finished source wins only when all the other sources are finished too
                    return finish();
                }
                int position = positions[source];

                if (copyRuns && page == runPage) {
                    runLength++;
                    advance(source, page, position);
                }
                else {
                    copyRun();
                    Page outputPage = null;
                    if (pageBreakPredicate.test(pageBuilder, page, position)) {
                        outputPage = buildPage();
                    }
                    runPage = page;
                    runStart = position;
                    runLength = 1;
                    advance(source, page, position);
                    if (outputPage != null) {
                        return ProcessState.ofResult(outputPage);
                    }
                }

                rowsUntilYieldCheck--;
                if (rowsUntilYieldCheck == 0) {
                    if (yieldSignal.isSet()) {
                        lastProcessYielded = true;
                        return ProcessState.yielded();
                    }
                    rowsUntilYieldCheck = ROWS_PER_YIELD_CHECK;
                }
            }
        }

        /**
         * Loads the next page of the pending source or marks the source as finished.
         * Returns false when the source is blocked or yielded.
         */
        private boolean loadPendingSource()
        {
            while (true) {
                WorkProcessor<Page> source = sources.get(pendingSource);
                if (!source.process()) {
                    return false;
                }

                if (source.isFinished()) {
                    sortKeyPrefixes[pendingSource] = FINISHED_SOURCE_PREFIX;
                    sourceMemoryContexts[pendingSource].close();
                }
                else {
                    Page page = source.getResult();
                    if (page.getPositionCount() == 0) {
                        continue;
                    }
                    pages[pendingSource] = page;
                    positions[pendingSource] = 0;
                    sortKeyPrefixes[pendingSource] = comparator.sortKeyPrefix(page, 0);
                    sourceMemoryContexts[pendingSource].setBytes(page.getRetainedSizeInBytes());
                }

                if (treeBuilt) {
                    replayMatches(pendingSource);
                    pendingSource = NO_SOURCE;
                    return true;
                }
                // matches can be played only when the head row of every source is known
                pendingSource++;
                if (pendingSource == sources.size()) {
                    buildTree();
                    pendingSource = NO_SOURCE;
                    return true;
                }
            }
        }

        private void advance(int source, Page page, int position)
        {
            int nextPosition = position + 1;
            if (nextPosition < page.getPositionCount()) {
                positions[source] = nextPosition;
                sortKeyPrefixes[source] = comparator.sortKeyPrefix(page, nextPosition);
                replayMatches(source);
                return;
            }

            // rows selected from the page have to be copied before the page is released
            copyRun();
            pages[source] = null;
            sourceMemoryContexts[source].setBytes(0);
            pendingSource = source;
        }

        private ProcessState<Page> finish()
        {
            copyRun();
            if (pageBuilder.isEmpty()) {
                outputMemoryContext.close();
                return ProcessState.finished();
            }
            return ProcessState.ofResult(buildPage());
        }

        private void copyRun()
        {
            if (runLength == 0) {
                return;
            }

            if (runLength == 1) {
                for (int channel = 0; channel < outputChannels.length; channel++) {
                    Block block = runPage.getBlock(outputChannels[channel]);
                    pageBuilder.getBlockBuilder(channel).append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(runStart));
                }
            }
            else {
                for (int channel = 0; channel < outputChannels.length; channel++) {
                    pageBuilder.getBlockBuilder(channel).appendBlockRange(runPage.getBlock(outputChannels[channel]), runStart, runLength);
                }
            }
            pageBuilder.declarePositions(runLength);
            runPage = null;
            runLength = 0;

            if (updateMemoryAfterEveryPosition) {
                outputMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
            }
        }

        private Page buildPage()
        {
            if (!updateMemoryAfterEveryPosition) {
                // update memory usage just before producing page to cap from top
                outputMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
            }
            Page page = pageBuilder.build();
            pageBuilder.reset();
            if (updateMemoryAfterEveryPosition) {
                outputMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
            }
            return page;
        }

        private void buildTree()
        {
            int sourceCount = sources.size();
            // source that won the match played in a node, leaves are in the second half
            int[] winners = new int[2 * sourceCount];
            for (int source = 0; source < sourceCount; source++) {
                winners[sourceCount + source] = source;
            }
            for (int node = sourceCount - 1; node > 0; node--) {
                int left = winners[2 * node];
                int right = winners[2 * node + 1];
                if (precedes(left, right)) {
                    winners[node] = left;
                    losers[node] = right;
                }
                else {
                    winners[node] = right;
                    losers[node] = left;
                }
            }
            winner = winners[1];
            treeBuilt = true;
        }

        private void replayMatches(int source)
        {
            int candidate = source;
            for (int node = (sources.size() + source) >>> 1; node > 0; node >>>= 1) {
                int loser = losers[node];
                if (precedes(loser, candidate)) {
                    losers[node] = candidate;
                    candidate = loser;
                }
            }
            winner = candidate;
        }

        private boolean precedes(int first, int second)
        {
            long firstPrefix = sortKeyPrefixes[first];
            long secondPrefix = sortKeyPrefixes[second];
            if (firstPrefix != secondPrefix) {
                return firstPrefix < secondPrefix;
            }

            Page firstPage = pages[first];
            if (firstPage == null) {
                return false;
            }
            Page secondPage = pages[second];
            if (secondPage == null) {
                return true;
            }
            int comparison = comparator.compareTo(firstPage, positions[first], secondPage, positions[second]);
            if (comparison != 0) {
                return comparison < 0;
            }
            return first < second;
        }
    }
}
