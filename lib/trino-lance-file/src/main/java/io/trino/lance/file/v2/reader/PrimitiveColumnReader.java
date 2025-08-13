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
package io.trino.lance.file.v2.reader;

import com.google.common.collect.ImmutableList;
import io.trino.lance.file.LanceDataSource;
import io.trino.lance.file.v2.metadata.ColumnMetadata;
import io.trino.lance.file.v2.metadata.Field;
import io.trino.lance.file.v2.metadata.MiniBlockLayout;
import io.trino.lance.file.v2.metadata.PageLayout;
import io.trino.lance.file.v2.metadata.PageMetadata;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PrimitiveColumnReader
        implements ColumnReader
{
    private final LanceDataSource dataSource;
    private final Type type;
    private final List<PageMetadata> pages;
    private final List<Range> ranges;
    private final AggregatedMemoryContext aggregatedMemoryContext;

    private PageReader pageReader;
    private int nextBatchSize;

    private long globalRowOffset;   // global row number
    private int pageIndex;          // current page being processed
    private long pageOffset;
    private int rangeIndex;         // current range being processed
    private long rangeOffset;

    public PrimitiveColumnReader(
            LanceDataSource dataSource,
            Field field,
            ColumnMetadata columnMetadata,
            List<Range> ranges,
            AggregatedMemoryContext memoryContext)
    {
        requireNonNull(field, "field is null");
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.type = field.toTrinoType();
        this.pages = requireNonNull(columnMetadata.pages(), "pages is null");
        this.ranges = requireNonNull(ranges, "ranges is null");
        this.aggregatedMemoryContext = requireNonNull(memoryContext, "memoryContext is null");

        this.globalRowOffset = 0;
        this.pageIndex = 0;
        this.rangeIndex = 0;
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        nextBatchSize = batchSize;
    }

    @Override
    public DecodedPage read()
    {
        if (rangeIndex >= ranges.size()) {
            throw new RuntimeException("no more ranges to read, something went wrong in LanceReader");
        }

        BlockBuilder blockBuilder = type.createBlockBuilder(null, nextBatchSize);
        int rowCount = 0;
        Range currentRange = ranges.get(rangeIndex);
        PageMetadata currentPage = pages.get(pageIndex);
        List<DecodedPage> decodedPages = new ArrayList<>();
        while (rowCount < nextBatchSize) {
            // move to next page
            while (currentPage.numRows() + globalRowOffset <= currentRange.start() + rangeOffset) {
                globalRowOffset += currentPage.numRows();
                advancePage();
                currentPage = pages.get(pageIndex);
            }

            // find all ranges in current page
            ImmutableList.Builder<Range> builder = ImmutableList.builder();
            long remaining = nextBatchSize - rowCount;
            while (remaining > 0 && currentPage.numRows() + globalRowOffset > currentRange.start() + rangeOffset) {
                long start = max(currentRange.start() + rangeOffset, globalRowOffset);
                long startInPage = start - globalRowOffset;
                long endInPage = min(startInPage + min(currentRange.length() - rangeOffset, remaining), currentPage.numRows());
                boolean lastInPage = (endInPage + globalRowOffset) >= currentRange.end();
                builder.add(new Range(startInPage, endInPage));
                remaining -= endInPage - startInPage;
                pageOffset = endInPage - globalRowOffset;
                rangeOffset = endInPage - currentRange.start();

                if (lastInPage) {
                    rangeIndex++;
                    rangeOffset = 0;
                    if (rangeIndex == ranges.size()) {
                        break;
                    }
                    currentRange = ranges.get(rangeIndex);
                }
                else {
                    break;
                }
            }

            // decode the page with ranges for current batch
            if (pageReader == null) {
                pageReader = createPageReader(currentPage);
            }
            DecodedPage decodedPage = pageReader.decodeRanges(builder.build());
            decodedPages.add(decodedPage);
            long numRowsRead = nextBatchSize - rowCount - remaining;
            rowCount += toIntExact(numRowsRead);
            if (pageOffset >= currentPage.numRows()) {
                globalRowOffset += currentPage.numRows();
                advancePage();
            }
        }

        if (decodedPages.size() > 1) {
            // merge all decoded pages
            List<RepetitionDefinitionUnraveler> unravelers = new ArrayList<>();
            for (DecodedPage decodedPage : decodedPages) {
                Block block = decodedPage.block();
                for (int i = 0; i < block.getPositionCount(); i++) {
                    if (block.isNull(i)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition(i));
                    }
                }
                unravelers.add(decodedPage.unraveler());
            }

            return new DecodedPage(blockBuilder.build(), new CompositeUnraveler(unravelers));
        }
        verify(!decodedPages.isEmpty());
        return decodedPages.getFirst();
    }

    private PageReader createPageReader(PageMetadata pageMetadata)
    {
        PageLayout layout = pageMetadata.layout();
        return switch (layout) {
            case MiniBlockLayout miniBlockLayout ->
                    new MiniBlockPageReader(dataSource, type, miniBlockLayout, pageMetadata.bufferOffsets(), pageMetadata.numRows(), aggregatedMemoryContext);
            default -> throw new IllegalArgumentException("Unsupported PageLayout: " + layout);
        };
    }

    private void advancePage()
    {
        pageIndex++;
        pageOffset = 0;
        pageReader = null;
    }
}
