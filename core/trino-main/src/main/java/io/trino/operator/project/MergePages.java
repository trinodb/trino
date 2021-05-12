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

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.WorkProcessor;
import io.trino.operator.WorkProcessor.TransformationState;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.operator.WorkProcessor.TransformationState.finished;
import static io.trino.operator.WorkProcessor.TransformationState.needsMoreData;
import static io.trino.operator.WorkProcessor.TransformationState.ofResult;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;

/**
 * This class is intended to be used right after the PageProcessor to ensure
 * that the size of the pages returned by FilterAndProject and ScanFilterAndProject
 * is big enough so it does not introduce considerable synchronization overhead.
 * <p>
 * As long as the input page contains more than {@link MergePagesTransformation#minRowCount} rows
 * or is bigger than {@link MergePagesTransformation#minPageSizeInBytes} it is returned as is without
 * additional memory copy.
 * <p>
 * The page data that has been buffered so far before receiving a "big" page is being flushed
 * before transferring a "big" page.
 * <p>
 * Although it is still possible that the {@link MergePages} may return a tiny page,
 * this situation is considered to be rare due to the assumption that filter selectivity may not
 * vary a lot based on the particular input page.
 * <p>
 * Considering the CPU time required to process(filter, project) a full (~1MB) page returned by a
 * connector, the CPU cost of memory copying (< 50kb, < 1024 rows) is supposed to be negligible.
 */
public final class MergePages
{
    private static final int MAX_MIN_PAGE_SIZE = 1024 * 1024;
    /**
     * Consider small page to be materialized when most of it's channels
     * are loaded. This prevents outputting a lot of small pages, where there
     * is potentially little benefit from late materialization.
     */
    private static final double LOADED_CHANNELS_RATIO_THRESHOLD = 0.5;

    private MergePages() {}

    public static WorkProcessor<Page> mergePages(
            Iterable<? extends Type> types,
            long minPageSizeInBytes,
            int minRowCount,
            double maxSmallPagesRowRatio,
            WorkProcessor<Page> pages,
            AggregatedMemoryContext memoryContext)
    {
        return mergePages(types, minPageSizeInBytes, minRowCount, DEFAULT_MAX_PAGE_SIZE_IN_BYTES, maxSmallPagesRowRatio, pages, memoryContext);
    }

    public static WorkProcessor<Page> mergePages(
            Iterable<? extends Type> types,
            long minPageSizeInBytes,
            int minRowCount,
            int maxPageSizeInBytes,
            double maxSmallPagesRowRatio,
            WorkProcessor<Page> pages,
            AggregatedMemoryContext memoryContext)
    {
        return pages.transform(new MergePagesTransformation(
                types,
                minPageSizeInBytes,
                minRowCount,
                maxPageSizeInBytes,
                maxSmallPagesRowRatio,
                memoryContext.newLocalMemoryContext(MergePages.class.getSimpleName())));
    }

    private static class MergePagesTransformation
            implements WorkProcessor.Transformation<Page, Page>
    {
        private final List<Type> types;
        private final long minPageSizeInBytes;
        private final int minRowCount;
        private final double maxSmallPagesRowRatio;
        private final LocalMemoryContext memoryContext;
        private final PageBuilder pageBuilder;

        private Page queuedPage;
        private Page smallLazyOutputPage;
        private long smallLazyPagesPositions;
        private long totalPositions;

        private MergePagesTransformation(
                Iterable<? extends Type> types,
                long minPageSizeInBytes,
                int minRowCount,
                int maxPageSizeInBytes,
                double maxSmallPagesRowRatio,
                LocalMemoryContext memoryContext)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            checkArgument(minPageSizeInBytes >= 0, "minPageSizeInBytes must be greater or equal than zero");
            checkArgument(minRowCount >= 0, "minRowCount must be greater or equal than zero");
            checkArgument(maxPageSizeInBytes > 0, "maxPageSizeInBytes must be greater than zero");
            checkArgument(maxPageSizeInBytes >= minPageSizeInBytes, "maxPageSizeInBytes must be greater or equal than minPageSizeInBytes");
            checkArgument(minPageSizeInBytes <= MAX_MIN_PAGE_SIZE, "minPageSizeInBytes must be less or equal than %s", MAX_MIN_PAGE_SIZE);
            checkArgument(maxSmallPagesRowRatio >= 0, "maxSmallPagesRowRatio must be greater than zero");
            this.minPageSizeInBytes = minPageSizeInBytes;
            this.minRowCount = minRowCount;
            this.maxSmallPagesRowRatio = maxSmallPagesRowRatio;
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
            pageBuilder = PageBuilder.withMaxPageSize(maxPageSizeInBytes, this.types);
        }

        @Override
        public TransformationState<Page> process(Page inputPage)
        {
            if (queuedPage != null) {
                checkState(smallLazyOutputPage == null || smallLazyOutputPage == queuedPage);
                Page output = queuedPage;
                queuedPage = null;
                memoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                return ofResult(output);
            }

            if (smallLazyOutputPage != null) {
                if (isPartiallyLoaded(smallLazyOutputPage)) {
                    smallLazyPagesPositions += smallLazyOutputPage.getPositionCount();
                }
                smallLazyOutputPage = null;
            }

            boolean inputFinished = inputPage == null;
            if (inputFinished) {
                if (pageBuilder.isEmpty()) {
                    memoryContext.close();
                    return finished();
                }

                return ofResult(flush(), false);
            }

            totalPositions += inputPage.getPositionCount();
            boolean isPartiallyLoaded = isPartiallyLoaded(inputPage);
            // passthrough small lazy page only when total number of rows from such pages is below threshold
            boolean passthroughSmallLazyPage = !isPartiallyLoaded && (totalPositions == 0 || ((double) smallLazyPagesPositions / totalPositions) <= maxSmallPagesRowRatio);
            if (inputPage.getPositionCount() >= minRowCount || passthroughSmallLazyPage || inputPage.getSizeInBytes() >= minPageSizeInBytes) {
                if (inputPage.getPositionCount() < minRowCount && !isPartiallyLoaded) {
                    // Store small lazy page to check if it was materialized later on.
                    // Ideally, we should account for page memory. However, downstream
                    // operators could also retain such page so this would lead to double
                    // memory accounting.
                    smallLazyOutputPage = inputPage;
                }

                if (pageBuilder.isEmpty()) {
                    return ofResult(inputPage);
                }

                Page output = pageBuilder.build();
                pageBuilder.reset();
                // inputPage is preserved until next process(...) call
                queuedPage = inputPage;
                memoryContext.setBytes(pageBuilder.getRetainedSizeInBytes() + inputPage.getRetainedSizeInBytes());
                return ofResult(output, false);
            }

            appendPage(inputPage);

            if (pageBuilder.isFull()) {
                return ofResult(flush());
            }

            memoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
            return needsMoreData();
        }

        private void appendPage(Page page)
        {
            page = page.getLoadedPage();
            pageBuilder.declarePositions(page.getPositionCount());
            for (int channel = 0; channel < types.size(); channel++) {
                Type type = types.get(channel);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
                }
            }
        }

        private Page flush()
        {
            Page output = pageBuilder.build();
            pageBuilder.reset();
            memoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
            return output;
        }
    }

    private static boolean isPartiallyLoaded(Page page)
    {
        int loadedBlockCount = 0;
        for (int channel = 0; channel < page.getChannelCount(); ++channel) {
            if (page.getBlock(channel).isLoaded()) {
                loadedBlockCount++;
            }
        }

        return (double) loadedBlockCount / page.getChannelCount() > LOADED_CHANNELS_RATIO_THRESHOLD;
    }
}
