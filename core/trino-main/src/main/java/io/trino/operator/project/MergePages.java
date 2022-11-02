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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.WorkProcessor.TransformationState.finished;
import static io.trino.operator.WorkProcessor.TransformationState.needsMoreData;
import static io.trino.operator.WorkProcessor.TransformationState.ofResult;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
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

    private MergePages() {}

    public static WorkProcessor<Page> mergePages(
            Iterable<? extends Type> types,
            long minPageSizeInBytes,
            int minRowCount,
            WorkProcessor<Page> pages,
            AggregatedMemoryContext memoryContext)
    {
        return mergePages(types, minPageSizeInBytes, minRowCount, DEFAULT_MAX_PAGE_SIZE_IN_BYTES, pages, memoryContext);
    }

    public static WorkProcessor<Page> mergePages(
            Iterable<? extends Type> types,
            long minPageSizeInBytes,
            int minRowCount,
            int maxPageSizeInBytes,
            WorkProcessor<Page> pages,
            AggregatedMemoryContext memoryContext)
    {
        List<Type> typeList = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        WorkProcessor.Transformation<Page, Page> mergingTransform;
        if (typeList.isEmpty()) {
            // position count only mode
            mergingTransform = new MergePositionCountPagesTransformation(minRowCount);
        }
        else {
            mergingTransform = new MergePagesTransformation(
                    typeList,
                    minPageSizeInBytes,
                    minRowCount,
                    maxPageSizeInBytes,
                    memoryContext.newLocalMemoryContext(MergePages.class.getSimpleName()));
        }
        return pages.transform(mergingTransform);
    }

    private static class MergePositionCountPagesTransformation
            implements WorkProcessor.Transformation<Page, Page>
    {
        private final int minRowCount;
        private int currentRowCount;

        private MergePositionCountPagesTransformation(int minRowCount)
        {
            checkArgument(minRowCount >= 0, "minRowCount must be greater or equal than zero");
            this.minRowCount = minRowCount;
        }

        @Override
        public TransformationState<Page> process(Page inputPage)
        {
            if (inputPage == null) {
                if (currentRowCount == 0) {
                    return finished();
                }
                Page result = new Page(currentRowCount);
                currentRowCount = 0;
                return ofResult(result, false);
            }
            int inputPositions = inputPage.getPositionCount();
            // Prefer to reuse input page instances directly if they're not too small
            if (inputPositions >= minRowCount) {
                return ofResult(inputPage);
            }
            // Accumulate small pages until we can emit the target output size
            currentRowCount += inputPositions;
            if (currentRowCount >= MAX_BATCH_SIZE) {
                currentRowCount -= MAX_BATCH_SIZE;
                return ofResult(new Page(MAX_BATCH_SIZE));
            }
            return needsMoreData();
        }
    }

    private static class MergePagesTransformation
            implements WorkProcessor.Transformation<Page, Page>
    {
        private final List<Type> types;
        private final long minPageSizeInBytes;
        private final int minRowCount;
        private final LocalMemoryContext memoryContext;
        private final PageBuilder pageBuilder;

        private Page queuedPage;

        private MergePagesTransformation(List<Type> types, long minPageSizeInBytes, int minRowCount, int maxPageSizeInBytes, LocalMemoryContext memoryContext)
        {
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            checkArgument(minPageSizeInBytes >= 0, "minPageSizeInBytes must be greater or equal than zero");
            checkArgument(minRowCount >= 0, "minRowCount must be greater or equal than zero");
            checkArgument(maxPageSizeInBytes > 0, "maxPageSizeInBytes must be greater than zero");
            checkArgument(maxPageSizeInBytes >= minPageSizeInBytes, "maxPageSizeInBytes must be greater or equal than minPageSizeInBytes");
            checkArgument(minPageSizeInBytes <= MAX_MIN_PAGE_SIZE, "minPageSizeInBytes must be less or equal than %s", MAX_MIN_PAGE_SIZE);
            this.minPageSizeInBytes = minPageSizeInBytes;
            this.minRowCount = minRowCount;
            this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
            pageBuilder = PageBuilder.withMaxPageSize(maxPageSizeInBytes, this.types);
        }

        @Override
        public TransformationState<Page> process(Page inputPage)
        {
            if (queuedPage != null) {
                Page output = queuedPage;
                queuedPage = null;
                memoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
                return ofResult(output);
            }

            boolean inputFinished = inputPage == null;
            if (inputFinished) {
                if (pageBuilder.isEmpty()) {
                    memoryContext.close();
                    return finished();
                }

                return ofResult(flush(), false);
            }

            // TODO: merge low cardinality blocks lazily
            if (inputPage.getPositionCount() >= minRowCount || !isLoaded(inputPage) || inputPage.getSizeInBytes() >= minPageSizeInBytes) {
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
            pageBuilder.declarePositions(page.getPositionCount());
            for (int channel = 0; channel < types.size(); channel++) {
                appendBlock(
                        types.get(channel),
                        page.getBlock(channel).getLoadedBlock(),
                        pageBuilder.getBlockBuilder(channel));
            }
        }

        private void appendBlock(Type type, Block block, BlockBuilder blockBuilder)
        {
            for (int position = 0; position < block.getPositionCount(); position++) {
                type.appendTo(block, position, blockBuilder);
            }
        }

        private Page flush()
        {
            Page output = pageBuilder.build();
            pageBuilder.reset();
            memoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
            return output;
        }

        private static boolean isLoaded(Page page)
        {
            // TODO: provide better heuristics there, e.g. check if last produced page was materialized
            for (int channel = 0; channel < page.getChannelCount(); ++channel) {
                Block block = page.getBlock(channel);
                if (!block.isLoaded()) {
                    return false;
                }
            }

            return true;
        }
    }
}
