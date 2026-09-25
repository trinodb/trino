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

import io.trino.annotation.NotThreadSafe;
import io.trino.array.ReferenceCountMap;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.ProcessState;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.ObjLongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.WorkProcessor.ProcessState.finished;
import static io.trino.operator.WorkProcessor.ProcessState.ofResult;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.operator.project.PageProcessor.MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.operator.project.PageProcessor.MIN_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;

/**
 * Evaluates a fixed list of projections over the selected positions of a page, emitting one output
 * page per batch. The batch size adapts to the produced output size and a batch whose output
 * crosses {@link PageProcessor#MAX_PAGE_SIZE_IN_BYTES} is halved and retried, bounding transient
 * memory of a single wide page. Batch size and the optimistic reuse of already-computed blocks
 * persist across pages, so the instance is reused for the whole stream of one operator.
 */
@NotThreadSafe
public final class PageProjectionsProcessor
{
    private final List<PageProjection> projections;
    // identity projections return a subset of the input page and cannot expand output size
    private final boolean identityProjections;
    private int projectBatchSize;

    public PageProjectionsProcessor(List<? extends PageProjection> projections, boolean identityProjections, int initialBatchSize)
    {
        this.projections = projections.stream().collect(toImmutableList());
        this.identityProjections = identityProjections;
        this.projectBatchSize = initialBatchSize;
    }

    public List<PageProjection> getProjections()
    {
        return projections;
    }

    public int getProjectionCount()
    {
        return projections.size();
    }

    public WorkProcessor<Page> project(
            ConnectorSession session,
            LocalMemoryContext memoryContext,
            PageProcessorMetrics metrics,
            SourcePage page,
            SelectedPositions selectedPositions)
    {
        return project(session, memoryContext, metrics, page, selectedPositions, Optional.empty());
    }

    /**
     * Projects {@code selectedPositions} of {@code page}. When {@code precomputedChannels} is
     * present it seeds the optimistic reuse cache with already-computed blocks (indexed by
     * projection); missing entries are computed lazily. The array is consumed, not copied.
     */
    public WorkProcessor<Page> project(
            ConnectorSession session,
            LocalMemoryContext memoryContext,
            PageProcessorMetrics metrics,
            SourcePage page,
            SelectedPositions selectedPositions,
            Optional<Block[]> precomputedChannels)
    {
        if (selectedPositions.isEmpty()) {
            return WorkProcessor.of();
        }
        return WorkProcessor.create(new ProjectSelectedPositions(session, memoryContext, metrics, page, selectedPositions, precomputedChannels));
    }

    private class ProjectSelectedPositions
            implements WorkProcessor.Process<Page>
    {
        private static final long INSTANCE_SIZE = instanceSize(ProjectSelectedPositions.class);

        private final ConnectorSession session;
        private final LocalMemoryContext memoryContext;
        private final PageProcessorMetrics metrics;

        private SourcePage page;
        private final Block[] previouslyComputedResults;
        private SelectedPositions selectedPositions;

        private ProjectSelectedPositions(
                ConnectorSession session,
                LocalMemoryContext memoryContext,
                PageProcessorMetrics metrics,
                SourcePage page,
                SelectedPositions selectedPositions,
                Optional<Block[]> precomputedChannels)
        {
            checkArgument(!selectedPositions.isEmpty(), "selectedPositions is empty");
            precomputedChannels.ifPresent(channels -> checkArgument(channels.length == projections.size(), "precomputedChannels size mismatch"));

            this.session = session;
            this.metrics = metrics;
            this.page = page;
            this.memoryContext = memoryContext;
            this.selectedPositions = selectedPositions;
            this.previouslyComputedResults = precomputedChannels.orElseGet(() -> new Block[projections.size()]);
        }

        @Override
        public ProcessState<Page> process()
        {
            while (true) {
                if (selectedPositions.isEmpty()) {
                    return finished();
                }

                int batchSize = Math.min(selectedPositions.size(), projectBatchSize);
                ProcessBatchResult result = processBatch(batchSize);

                if (result.isPageTooLarge()) {
                    // the page buffer filled up, so halve the batch size and retry
                    verify(batchSize > 1);
                    projectBatchSize = projectBatchSize / 2;
                    continue;
                }

                verify(result.isSuccess());
                Page resultPage = result.getPage();
                if (!identityProjections) {
                    // output size is bounded by the input page, so no need to measure it or adapt the batch size
                    updateBatchSize(resultPage.getPositionCount(), resultPage.getSizeInBytes());
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
                    Arrays.fill(previouslyComputedResults, null);
                    memoryContext.setBytes(0);
                }

                return ofResult(resultPage);
            }
        }

        private void updateBatchSize(int positionCount, long pageSize)
        {
            // if we produced a large page, halve the batch size for the next call
            if (positionCount > 1 && pageSize > MAX_PAGE_SIZE_IN_BYTES) {
                projectBatchSize = projectBatchSize / 2;
            }

            // if we produced a small page, double the batch size for the next call
            if (pageSize < MIN_PAGE_SIZE_IN_BYTES && projectBatchSize < MAX_BATCH_SIZE) {
                projectBatchSize = projectBatchSize * 2;
            }
        }

        private void updateRetainedSize()
        {
            RetainedBytesByPartVisitor visitor = new RetainedBytesByPartVisitor();

            page.retainedBytesForEachPart(visitor);

            for (Block previouslyComputedResult : previouslyComputedResults) {
                if (previouslyComputedResult != null) {
                    previouslyComputedResult.retainedBytesForEachPart(visitor);
                }
            }

            long retainedSizeInBytes = INSTANCE_SIZE +
                    selectedPositions.getRetainedSizeInBytes() +
                    sizeOf(previouslyComputedResults) +
                    visitor.getRetainedSizeInBytes();

            memoryContext.setBytes(retainedSizeInBytes);
        }

        private static final class RetainedBytesByPartVisitor
                implements ObjLongConsumer<Object>
        {
            private final ReferenceCountMap referenceCountMap = new ReferenceCountMap();
            private long retainedSizeInBytes;

            public long getRetainedSizeInBytes()
            {
                return retainedSizeInBytes;
            }

            @Override
            public void accept(Object object, long size)
            {
                // increment the size only when it is the first reference
                if (referenceCountMap.incrementAndGetWithExtraIdentity(object, size) == 1) {
                    retainedSizeInBytes += size;
                }
            }
        }

        private ProcessBatchResult processBatch(int batchSize)
        {
            Block[] blocks = new Block[projections.size()];

            long pageSize = 0;
            SelectedPositions positionsBatch = selectedPositions.subRange(0, batchSize);
            for (int i = 0; i < projections.size(); i++) {
                if (!identityProjections && positionsBatch.size() > 1 && pageSize > MAX_PAGE_SIZE_IN_BYTES) {
                    return ProcessBatchResult.processBatchTooLarge();
                }

                // if possible, use previouslyComputedResults produced in prior optimistic failure attempt
                PageProjection projection = projections.get(i);
                if (previouslyComputedResults[i] != null && previouslyComputedResults[i].getPositionCount() >= batchSize) {
                    blocks[i] = previouslyComputedResults[i].getRegion(0, batchSize);
                }
                else {
                    SourcePage inputChannelsSourcePage = projection.getInputChannels().getInputChannels(page);
                    long projectionStartNanos = System.nanoTime();
                    Block result = projection.project(session, inputChannelsSourcePage, positionsBatch);
                    metrics.recordProjectionTime(System.nanoTime() - projectionStartNanos);

                    previouslyComputedResults[i] = result;
                    blocks[i] = previouslyComputedResults[i];
                }

                if (!identityProjections) {
                    pageSize += blocks[i].getSizeInBytes();
                }
            }
            return ProcessBatchResult.processBatchSuccess(new Page(positionsBatch.size(), blocks));
        }
    }

    private record ProcessBatchResult(ProcessBatchState state, Page page)
    {
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

        public static ProcessBatchResult processBatchTooLarge()
        {
            return new ProcessBatchResult(ProcessBatchState.PAGE_TOO_LARGE, null);
        }

        public static ProcessBatchResult processBatchSuccess(Page page)
        {
            return new ProcessBatchResult(ProcessBatchState.SUCCESS, requireNonNull(page));
        }

        private enum ProcessBatchState
        {
            PAGE_TOO_LARGE,
            SUCCESS,
        }
    }
}
