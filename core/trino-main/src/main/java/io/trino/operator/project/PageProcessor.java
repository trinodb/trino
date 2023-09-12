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
package io.trino.operator.project;

import com.google.common.annotations.VisibleForTesting;
import io.trino.annotation.NotThreadSafe;
import io.trino.array.ReferenceCountMap;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.Work;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.DictionaryId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.sql.gen.ExpressionProfiler;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.PageUtils.recordMaterializedBytes;
import static io.trino.operator.WorkProcessor.ProcessState.finished;
import static io.trino.operator.WorkProcessor.ProcessState.ofResult;
import static io.trino.operator.WorkProcessor.ProcessState.yielded;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class PageProcessor
{
    public static final int MAX_BATCH_SIZE = 8 * 1024;
    static final int MAX_PAGE_SIZE_IN_BYTES = 16 * 1024 * 1024;
    static final int MIN_PAGE_SIZE_IN_BYTES = 4 * 1024 * 1024;

    private final ExpressionProfiler expressionProfiler;
    private final DictionarySourceIdFunction dictionarySourceIdFunction = new DictionarySourceIdFunction();
    private final Optional<PageFilter> filter;
    private final List<PageProjection> projections;

    private int projectBatchSize;

    public PageProcessor(Optional<PageFilter> filter, List<? extends PageProjection> projections, OptionalInt initialBatchSize)
    {
        this(filter, projections, initialBatchSize, new ExpressionProfiler());
    }

    @VisibleForTesting
    public PageProcessor(Optional<PageFilter> filter, List<? extends PageProjection> projections, OptionalInt initialBatchSize, ExpressionProfiler expressionProfiler)
    {
        this.filter = filter.map(pageFilter -> {
            if (pageFilter.getInputChannels().size() == 1 && pageFilter.isDeterministic()) {
                return new DictionaryAwarePageFilter(pageFilter);
            }
            return pageFilter;
        });
        this.projections = projections.stream()
                .map(projection -> {
                    if (projection.getInputChannels().size() == 1 && projection.isDeterministic()) {
                        return new DictionaryAwarePageProjection(projection, dictionarySourceIdFunction, projection instanceof InputPageProjection);
                    }
                    return projection;
                })
                .collect(toImmutableList());
        this.projectBatchSize = initialBatchSize.orElse(1);
        this.expressionProfiler = requireNonNull(expressionProfiler, "expressionProfiler is null");
    }

    public PageProcessor(Optional<PageFilter> filter, List<? extends PageProjection> projections)
    {
        this(filter, projections, OptionalInt.of(1));
    }

    @VisibleForTesting
    public Iterator<Optional<Page>> process(ConnectorSession session, DriverYieldSignal yieldSignal, LocalMemoryContext memoryContext, Page page)
    {
        return process(session, yieldSignal, memoryContext, page, false);
    }

    @VisibleForTesting
    Iterator<Optional<Page>> process(ConnectorSession session, DriverYieldSignal yieldSignal, LocalMemoryContext memoryContext, Page page, boolean avoidPageMaterialization)
    {
        WorkProcessor<Page> processor = createWorkProcessor(session, yieldSignal, memoryContext, new PageProcessorMetrics(), page, avoidPageMaterialization);
        return processor.yieldingIterator();
    }

    public WorkProcessor<Page> createWorkProcessor(
            ConnectorSession session,
            DriverYieldSignal yieldSignal,
            LocalMemoryContext memoryContext,
            PageProcessorMetrics metrics,
            Page page,
            boolean avoidPageMaterialization)
    {
        // limit the scope of the dictionary ids to just one page
        dictionarySourceIdFunction.reset();

        if (page.getPositionCount() == 0) {
            return WorkProcessor.of();
        }

        if (filter.isPresent()) {
            Page inputPage = filter.get().getInputChannels().getInputChannels(page);
            long start = System.nanoTime();
            SelectedPositions selectedPositions = filter.get().filter(session, inputPage);
            metrics.recordFilterTimeSince(start);
            if (selectedPositions.isEmpty()) {
                return WorkProcessor.of();
            }

            if (projections.isEmpty()) {
                // retained memory for empty page is negligible
                return WorkProcessor.of(new Page(selectedPositions.size()));
            }

            if (selectedPositions.size() != page.getPositionCount()) {
                return WorkProcessor.create(new ProjectSelectedPositions(session, yieldSignal, memoryContext, metrics, page, selectedPositions, avoidPageMaterialization));
            }
        }
        else if (projections.isEmpty()) {
            // retained memory for empty page is negligible
            return WorkProcessor.of(new Page(page.getPositionCount()));
        }

        return WorkProcessor.create(new ProjectSelectedPositions(session, yieldSignal, memoryContext, metrics, page, positionsRange(0, page.getPositionCount()), avoidPageMaterialization));
    }

    private class ProjectSelectedPositions
            implements WorkProcessor.Process<Page>
    {
        private final ConnectorSession session;
        private final DriverYieldSignal yieldSignal;
        private final LocalMemoryContext memoryContext;
        private final PageProcessorMetrics metrics;
        private final boolean avoidPageMaterialization;

        private Page page;
        private final Block[] previouslyComputedResults;
        private SelectedPositions selectedPositions;
        private long retainedSizeInBytes;

        // remember if we need to re-use the same batch size if we yield last time
        private boolean lastComputeYielded;
        private int lastComputeBatchSize;
        private Work<Block> pageProjectWork;

        private int outputPagePositions = -1;
        private long outputPageSizeInBytes;

        private ProjectSelectedPositions(
                ConnectorSession session,
                DriverYieldSignal yieldSignal,
                LocalMemoryContext memoryContext,
                PageProcessorMetrics metrics,
                Page page,
                SelectedPositions selectedPositions,
                boolean avoidPageMaterialization)
        {
            checkArgument(!selectedPositions.isEmpty(), "selectedPositions is empty");

            this.session = session;
            this.yieldSignal = yieldSignal;
            this.metrics = metrics;
            this.page = page;
            this.memoryContext = memoryContext;
            this.avoidPageMaterialization = avoidPageMaterialization;
            this.selectedPositions = selectedPositions;
            this.previouslyComputedResults = new Block[projections.size()];
        }

        @Override
        public ProcessState<Page> process()
        {
            if (avoidPageMaterialization && outputPagePositions != -1) {
                updateBatchSize(outputPagePositions, outputPageSizeInBytes);
                outputPagePositions = -1;
            }

            int batchSize;
            while (true) {
                if (selectedPositions.isEmpty()) {
                    verify(!lastComputeYielded);
                    return finished();
                }

                // we always process one chunk
                if (lastComputeYielded) {
                    // re-use the batch size from the last checkpoint
                    verify(lastComputeBatchSize > 0);
                    batchSize = lastComputeBatchSize;
                    lastComputeYielded = false;
                    lastComputeBatchSize = 0;
                }
                else {
                    batchSize = Math.min(selectedPositions.size(), projectBatchSize);
                }
                ProcessBatchResult result = processBatch(batchSize);

                if (result.isYieldFinish()) {
                    // if we are running out of time, save the batch size and continue next time
                    lastComputeYielded = true;
                    lastComputeBatchSize = batchSize;
                    updateRetainedSize();
                    return yielded();
                }

                if (result.isPageTooLarge()) {
                    // if the page buffer filled up, so halve the batch size and retry
                    verify(batchSize > 1);
                    projectBatchSize = projectBatchSize / 2;
                    continue;
                }

                verify(result.isSuccess());
                Page resultPage = result.getPage();

                if (!avoidPageMaterialization) {
                    updateBatchSize(resultPage.getPositionCount(), resultPage.getSizeInBytes());
                }
                else {
                    // This is executed within WorkProcessorOperator context.
                    // Therefore it is guaranteed that:
                    // 1. produced Page is accessed by single thread
                    // 2. lazy Page can be materialized only before fetching next page from PageProcessor
                    outputPagePositions = resultPage.getPositionCount();
                    outputPageSizeInBytes = 0;
                    recordMaterializedBytes(resultPage, sizeInBytes -> outputPageSizeInBytes += sizeInBytes);
                }

                // remove batch from selectedPositions and previouslyComputedResults
                selectedPositions = selectedPositions.subRange(batchSize, selectedPositions.size());
                for (int i = 0; i < previouslyComputedResults.length; i++) {
                    if (previouslyComputedResults[i] != null && previouslyComputedResults[i].getPositionCount() > batchSize) {
                        previouslyComputedResults[i] = previouslyComputedResults[i].getRegion(batchSize, previouslyComputedResults[i].getPositionCount() - batchSize);
                    }
                    else {
                        previouslyComputedResults[i] = null;
                    }
                }

                if (!selectedPositions.isEmpty()) {
                    // there are still some positions to process therefore we need to retain page and account its memory
                    updateRetainedSize();
                }
                else {
                    page = null;
                    for (int i = 0; i < previouslyComputedResults.length; i++) {
                        previouslyComputedResults[i] = null;
                    }
                    memoryContext.setBytes(0);
                }

                return ofResult(resultPage);
            }
        }

        private void updateBatchSize(int positionCount, long pageSize)
        {
            // if we produced a large page or if the expression is expensive, halve the batch size for the next call
            if (positionCount > 1 && (pageSize > MAX_PAGE_SIZE_IN_BYTES || expressionProfiler.isExpressionExpensive())) {
                projectBatchSize = projectBatchSize / 2;
            }

            // if we produced a small page, double the batch size for the next call
            if (pageSize < MIN_PAGE_SIZE_IN_BYTES && projectBatchSize < MAX_BATCH_SIZE && !expressionProfiler.isExpressionExpensive()) {
                projectBatchSize = projectBatchSize * 2;
            }
        }

        private void updateRetainedSize()
        {
            // increment the size only when it is the first reference
            retainedSizeInBytes = Page.getInstanceSizeInBytes(page.getChannelCount());
            ReferenceCountMap referenceCountMap = new ReferenceCountMap();
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Block block = page.getBlock(channel);
                // TODO: block might be partially loaded
                if (block.isLoaded()) {
                    block.retainedBytesForEachPart((object, size) -> {
                        if (referenceCountMap.incrementAndGet(object) == 1) {
                            retainedSizeInBytes += size;
                        }
                    });
                }
            }
            for (Block previouslyComputedResult : previouslyComputedResults) {
                if (previouslyComputedResult != null) {
                    previouslyComputedResult.retainedBytesForEachPart((object, size) -> {
                        if (referenceCountMap.incrementAndGet(object) == 1) {
                            retainedSizeInBytes += size;
                        }
                    });
                }
            }

            memoryContext.setBytes(retainedSizeInBytes);
        }

        private ProcessBatchResult processBatch(int batchSize)
        {
            Block[] blocks = new Block[projections.size()];

            long pageSize = 0;
            SelectedPositions positionsBatch = selectedPositions.subRange(0, batchSize);
            for (int i = 0; i < projections.size(); i++) {
                if (yieldSignal.isSet()) {
                    return ProcessBatchResult.processBatchYield();
                }

                if (positionsBatch.size() > 1 && pageSize > MAX_PAGE_SIZE_IN_BYTES) {
                    return ProcessBatchResult.processBatchTooLarge();
                }

                // if possible, use previouslyComputedResults produced in prior optimistic failure attempt
                PageProjection projection = projections.get(i);
                if (previouslyComputedResults[i] != null && previouslyComputedResults[i].getPositionCount() >= batchSize) {
                    blocks[i] = previouslyComputedResults[i].getRegion(0, batchSize);
                }
                else {
                    if (pageProjectWork == null) {
                        pageProjectWork = projection.project(session, yieldSignal, projection.getInputChannels().getInputChannels(page), positionsBatch);
                    }

                    expressionProfiler.start();
                    boolean finished = pageProjectWork.process();
                    long projectionTimeNanos = expressionProfiler.stop(positionsBatch.size());
                    metrics.recordProjectionTime(projectionTimeNanos);

                    if (!finished) {
                        return ProcessBatchResult.processBatchYield();
                    }
                    previouslyComputedResults[i] = pageProjectWork.getResult();
                    pageProjectWork = null;
                    blocks[i] = previouslyComputedResults[i];
                }

                if (!avoidPageMaterialization) {
                    blocks[i] = blocks[i].getLoadedBlock();
                    pageSize += blocks[i].getSizeInBytes();
                }
            }
            return ProcessBatchResult.processBatchSuccess(new Page(positionsBatch.size(), blocks));
        }
    }

    @VisibleForTesting
    public List<PageProjection> getProjections()
    {
        return projections;
    }

    @NotThreadSafe
    private static class DictionarySourceIdFunction
            implements Function<DictionaryBlock, DictionaryId>
    {
        private final Map<DictionaryId, DictionaryId> dictionarySourceIds = new HashMap<>();

        @Override
        public DictionaryId apply(DictionaryBlock block)
        {
            return dictionarySourceIds.computeIfAbsent(block.getDictionarySourceId(), ignored -> randomDictionaryId());
        }

        public void reset()
        {
            dictionarySourceIds.clear();
        }
    }

    private static class ProcessBatchResult
    {
        private final ProcessBatchState state;
        private final Page page;

        private ProcessBatchResult(ProcessBatchState state, Page page)
        {
            this.state = state;
            this.page = page;
        }

        public static ProcessBatchResult processBatchYield()
        {
            return new ProcessBatchResult(ProcessBatchState.YIELD, null);
        }

        public static ProcessBatchResult processBatchTooLarge()
        {
            return new ProcessBatchResult(ProcessBatchState.PAGE_TOO_LARGE, null);
        }

        public static ProcessBatchResult processBatchSuccess(Page page)
        {
            return new ProcessBatchResult(ProcessBatchState.SUCCESS, requireNonNull(page));
        }

        public boolean isYieldFinish()
        {
            return state == ProcessBatchState.YIELD;
        }

        public boolean isPageTooLarge()
        {
            return state == ProcessBatchState.PAGE_TOO_LARGE;
        }

        public boolean isSuccess()
        {
            return state == ProcessBatchState.SUCCESS;
        }

        public Page getPage()
        {
            verify(state == ProcessBatchState.SUCCESS);
            return verifyNotNull(page);
        }

        private enum ProcessBatchState
        {
            YIELD,
            PAGE_TOO_LARGE,
            SUCCESS
        }
    }
}
