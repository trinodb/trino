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
package io.trino.operator;

import com.google.common.collect.ImmutableList;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.project.InputPageProjection;
import io.trino.operator.project.PageProcessorMetrics;
import io.trino.operator.project.PageProjectionsProcessor;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Iterators.getOnlyElement;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.operator.project.SelectedPositions.positionsList;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestMaskedPage
{
    @Test
    public void testGetBlockProducesMaskedChannelOnce()
    {
        LocalMemoryContext memoryContext = newMemoryContext();
        PageProcessorMetrics metrics = new PageProcessorMetrics();
        MaskedPage maskedPage = createMaskedPage(positionsList(new int[] {1, 3, 5, 7}, 0, 4), memoryContext, metrics);
        assertThat(maskedPage.getPositionCount()).isEqualTo(4);
        assertThat(maskedPage.getChannelCount()).isEqualTo(2);
        assertThat(maskedPage.getSizeInBytes()).isZero();

        Block block = maskedPage.getBlock(0);
        assertBlockEquals(BIGINT, block, createLongsBlock(1, 3, 5, 7));
        assertThat(maskedPage.getBlock(0)).isSameAs(block);
        assertThat(maskedPage.getSizeInBytes()).isEqualTo(block.getSizeInBytes());
        assertThat(memoryContext.getBytes()).isEqualTo(block.getRetainedSizeInBytes());
        assertThat(metrics.getMetrics().getMetrics()).containsKey("Projection CPU time");
    }

    @Test
    public void testSelectPositionsNarrowsComputedAndDeferredChannels()
    {
        MaskedPage maskedPage = createMaskedPage(positionsRange(2, 6), newMemoryContext(), new PageProcessorMetrics());
        assertBlockEquals(BIGINT, maskedPage.getBlock(0), createLongsBlock(2, 3, 4, 5, 6, 7));

        maskedPage.selectPositions(new int[] {-1, 0, 2, 3, 5}, 1, 4);
        assertThat(maskedPage.getPositionCount()).isEqualTo(4);
        assertBlockEquals(BIGINT, maskedPage.getBlock(0), createLongsBlock(2, 4, 5, 7));
        assertBlockEquals(BIGINT, maskedPage.getBlock(1), createLongsBlock(102, 104, 105, 107));
    }

    @Test
    public void testSelectContiguousPositionsNarrowsComputedAndDeferredChannels()
    {
        MaskedPage maskedPage = createMaskedPage(positionsList(new int[] {1, 3, 5, 7, 9}, 0, 5), newMemoryContext(), new PageProcessorMetrics());
        assertBlockEquals(BIGINT, maskedPage.getBlock(0), createLongsBlock(1, 3, 5, 7, 9));

        maskedPage.selectPositions(new int[] {2, 3, 4}, 0, 3);
        assertThat(maskedPage.getPositionCount()).isEqualTo(3);
        assertBlockEquals(BIGINT, maskedPage.getBlock(0), createLongsBlock(5, 7, 9));
        assertBlockEquals(BIGINT, maskedPage.getBlock(1), createLongsBlock(105, 107, 109));
    }

    @Test
    public void testMaterializeProducesAllChannels()
    {
        LocalMemoryContext memoryContext = newMemoryContext();
        MaskedPage maskedPage = createMaskedPage(positionsList(new int[] {1, 3, 5, 7}, 0, 4), memoryContext, new PageProcessorMetrics());
        maskedPage.getBlock(0);
        List<Page> validatedPages = new ArrayList<>();
        maskedPage.setOutputValidator(validatedPages::add);

        Page page = getOnlyElement(maskedPage.materialize().iterator());
        assertBlockEquals(BIGINT, page.getBlock(0), createLongsBlock(1, 3, 5, 7));
        assertBlockEquals(BIGINT, page.getBlock(1), createLongsBlock(101, 103, 105, 107));
        assertThat(validatedPages).containsExactly(page);
        assertThat(maskedPage.getSizeInBytes()).isEqualTo(page.getSizeInBytes());
        assertThat(memoryContext.getBytes()).isZero();
    }

    @Test
    public void testMaterializedPageRejectsFurtherReads()
    {
        MaskedPage maskedPage = createMaskedPage(positionsRange(0, 10), newMemoryContext(), new PageProcessorMetrics());
        maskedPage.materialize();

        assertThatThrownBy(() -> maskedPage.getBlock(0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("masked page is already materialized");
        assertThatThrownBy(() -> maskedPage.selectPositions(new int[] {1}, 0, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("masked page is already materialized");
        assertThatThrownBy(maskedPage::materialize)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("masked page is already materialized");
    }

    @Test
    public void testInvalidateReleasesMemoryAndRejectsReads()
    {
        LocalMemoryContext memoryContext = newMemoryContext();
        MaskedPage maskedPage = createMaskedPage(positionsRange(0, 10), memoryContext, new PageProcessorMetrics());
        maskedPage.getBlock(0);
        assertThat(memoryContext.getBytes()).isPositive();

        maskedPage.invalidate();
        assertThat(memoryContext.getBytes()).isZero();
        assertThatThrownBy(() -> maskedPage.getBlock(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("masked page is invalidated");
        assertThatThrownBy(() -> maskedPage.selectPositions(new int[] {1}, 0, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("masked page is invalidated");
        assertThatThrownBy(maskedPage::materialize)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("masked page is invalidated");
    }

    private static MaskedPage createMaskedPage(SelectedPositions selectedPositions, LocalMemoryContext memoryContext, PageProcessorMetrics metrics)
    {
        Page page = new Page(createLongSequenceBlock(0, 10), createLongSequenceBlock(100, 110));
        PageProjectionsProcessor projectionsProcessor = new PageProjectionsProcessor(
                ImmutableList.of(new InputPageProjection(0), new InputPageProjection(1)),
                MAX_BATCH_SIZE);
        return MaskedPage.applyMask(SESSION, SourcePage.create(page), selectedPositions, projectionsProcessor, memoryContext, metrics);
    }

    private static LocalMemoryContext newMemoryContext()
    {
        return newSimpleAggregatedMemoryContext().newLocalMemoryContext("test");
    }
}
