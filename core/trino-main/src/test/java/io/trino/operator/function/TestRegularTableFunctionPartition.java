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
package io.trino.operator.function;

import com.google.common.collect.ImmutableList;
import io.trino.RowPagesBuilder;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.operator.PagesIndex;
import io.trino.operator.WorkProcessor;
import io.trino.operator.function.RegularTableFunctionPartition.PassThroughColumnSpecification;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInputAndProduced;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRegularTableFunctionPartition
{
    @Test
    public void testOutputPageSizeBoundedWhenPassThroughDataIsLarge()
    {
        // When a table function expands one input row into many output rows the pass-through
        // block builder in NonPartitioningColumnProvider is created with a null BlockBuilderStatus,
        // so it grows without bound. With a 4 KB pass-through column and 2000 output rows the
        // single block would be ~8 MB — well above DEFAULT_MAX_PAGE_SIZE_IN_BYTES (1 MB).
        //
        // After the fix, appendPassThroughColumns splits the result into multiple pages so that
        // every page stays within the size limit.

        // PagesIndex: channel 0 = VARCHAR pass-through (4 KB), channel 1 = BIGINT (required by TF)
        PagesIndex pagesIndex = new PagesIndex.TestingFactory(false)
                .newPagesIndex(ImmutableList.of(VARCHAR, BIGINT), 1);
        pagesIndex.addPage(RowPagesBuilder.rowPagesBuilder(VARCHAR, BIGINT)
                .row("x".repeat(4096), 0L)
                .buildPage());

        // A processor that reads the single input row and emits 2000 output rows,
        // each carrying a pass-through index pointing back to input row 0.
        TableFunctionDataProcessor processor = new ExpandingProcessor(2000);

        RegularTableFunctionPartition partition = new RegularTableFunctionPartition(
                pagesIndex,
                0,   // partitionStart
                1,   // partitionEnd
                processor,
                0,   // properChannelsCount  (processor produces no proper columns)
                1,   // passThroughSourcesCount
                ImmutableList.of(ImmutableList.of(1)),   // requiredChannels: channel 1
                Optional.empty(),                         // single source → no marker channels
                ImmutableList.of(new PassThroughColumnSpecification(false, 0, 0)),
                AggregatedMemoryContext.newSimpleAggregatedMemoryContext());
        // pass-through spec: non-partitioning, input channel 0 (VARCHAR), index at output channel 0

        List<Page> outputPages = new ArrayList<>();
        WorkProcessor<Page> output = partition.toOutputPages();
        output.iterator().forEachRemaining(outputPages::add);

        // splitIntoPages must have fired: with avgBytesPerRow=4096 and limit=1MB,
        // maxRowsPerOutputPage = 256, so 2000 rows → ceil(2000/256) = 8 pages.
        assertThat(outputPages.size())
                .as("number of output pages (proves splitIntoPages was called)")
                .isEqualTo(8);

        // Each output page must stay within a reasonable size bound.
        for (Page page : outputPages) {
            assertThat(page.getSizeInBytes())
                    .as("output page size")
                    .isLessThanOrEqualTo((long) DEFAULT_MAX_PAGE_SIZE_IN_BYTES * 2);
        }

        // Total row count across all pages must equal the expansion factor.
        int totalRows = outputPages.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(totalRows).isEqualTo(2000);
    }

    private static class ExpandingProcessor
            implements TableFunctionDataProcessor
    {
        private final int expansionFactor;
        private boolean inputConsumed;

        ExpandingProcessor(int expansionFactor)
        {
            this.expansionFactor = expansionFactor;
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> input)
        {
            if (input == null || inputConsumed) {
                return FINISHED;
            }
            inputConsumed = true;
            BlockBuilder indexBuilder = BIGINT.createFixedSizeBlockBuilder(expansionFactor);
            for (int i = 0; i < expansionFactor; i++) {
                BIGINT.writeLong(indexBuilder, 0L);
            }
            return usedInputAndProduced(new Page(indexBuilder.build()));
        }
    }
}
