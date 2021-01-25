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
import io.trino.operator.WorkProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.execution.buffer.PageSplitterUtil.splitPage;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.operator.WorkProcessorAssertion.assertFinishes;
import static io.trino.operator.WorkProcessorAssertion.validateResult;
import static io.trino.operator.project.MergePages.mergePages;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMergePages
{
    private static final List<Type> TYPES = ImmutableList.of(BIGINT, REAL, DOUBLE);

    @Test
    public void testMinPageSizeThreshold()
    {
        Page page = createSequencePage(TYPES, 10);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                page.getSizeInBytes(),
                Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                1.0,
                pagesSource(page),
                newSimpleAggregatedMemoryContext());

        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testMinRowCountThreshold()
    {
        Page page = createSequencePage(TYPES, 10);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                1024 * 1024,
                page.getPositionCount(),
                Integer.MAX_VALUE,
                1.0,
                pagesSource(page),
                newSimpleAggregatedMemoryContext());

        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testMinRowCountThresholdWithLazyPages()
    {
        Page firstPage = lazyWrapper(createSequencePage(TYPES, 10));
        Page secondPage = lazyWrapper(createSequencePage(TYPES, 20));
        Page thirdPage = lazyWrapper(createSequencePage(TYPES, 300));
        Page fourthPage = lazyWrapper(createSequencePage(TYPES, 20));

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                1024 * 1024,
                200,
                Integer.MAX_VALUE,
                0.1,
                pagesSource(firstPage, secondPage, thirdPage, fourthPage),
                newSimpleAggregatedMemoryContext());

        assertTrue(mergePages.process());
        assertFalse(mergePages.isFinished());

        // if the lazy page is first, then it should be passed through
        Page result = mergePages.getResult();
        assertFalse(isLoaded(result));
        // load lazy page fully, it's positions should be accounted for as for small page
        assertPageEquals(TYPES, result, firstPage);

        // second page should be loaded because it was accumulated (since maxSmallPagesRowRatio was exceeded)
        assertTrue(mergePages.process());
        result = mergePages.getResult();
        assertTrue(isLoaded(result));
        assertPageEquals(TYPES, result, secondPage);

        // third page is passed through as it's number of rows is equal to minRowCount
        assertTrue(mergePages.process());
        result = mergePages.getResult();
        assertFalse(isLoaded(result));

        // fourth page is passed though as maxSmallPagesRowRatio is not exceeded
        assertTrue(mergePages.process());
        result = mergePages.getResult();
        assertFalse(isLoaded(result));
        assertPageEquals(TYPES, result, fourthPage);
    }

    @Test
    public void testBufferSmallPages()
    {
        int singlePageRowCount = 10;
        Page page = createSequencePage(TYPES, singlePageRowCount * 2);
        List<Page> splits = splitPage(page, page.getSizeInBytes() / 2);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                page.getSizeInBytes() + 1,
                page.getPositionCount() + 1,
                Integer.MAX_VALUE,
                1.0,
                pagesSource(splits.get(0), splits.get(1)),
                newSimpleAggregatedMemoryContext());

        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testFlushOnBigPage()
    {
        Page smallPage = createSequencePage(TYPES, 10);
        Page bigPage = createSequencePage(TYPES, 100);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                bigPage.getSizeInBytes(),
                bigPage.getPositionCount(),
                Integer.MAX_VALUE,
                1.0,
                pagesSource(smallPage, bigPage),
                newSimpleAggregatedMemoryContext());

        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, smallPage));
        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, bigPage));
        assertFinishes(mergePages);
    }

    @Test
    public void testFlushOnFullPage()
    {
        int singlePageRowCount = 10;
        List<Type> types = ImmutableList.of(BIGINT);
        Page page = createSequencePage(types, singlePageRowCount * 2);
        List<Page> splits = splitPage(page, page.getSizeInBytes() / 2);

        WorkProcessor<Page> mergePages = mergePages(
                types,
                page.getSizeInBytes() / 2 + 1,
                page.getPositionCount() / 2 + 1,
                toIntExact(page.getSizeInBytes()),
                1.0,
                pagesSource(splits.get(0), splits.get(1), splits.get(0), splits.get(1)),
                newSimpleAggregatedMemoryContext());

        validateResult(mergePages, actualPage -> assertPageEquals(types, actualPage, page));
        validateResult(mergePages, actualPage -> assertPageEquals(types, actualPage, page));
        assertFinishes(mergePages);
    }

    private static Page lazyWrapper(Page page)
    {
        Block[] lazyBlocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); ++channel) {
            lazyBlocks[channel] = lazyWrapper(page.getBlock(channel));
        }

        return new Page(lazyBlocks);
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), block::getLoadedBlock);
    }

    private static WorkProcessor<Page> pagesSource(Page... pages)
    {
        return WorkProcessor.fromIterable(ImmutableList.copyOf(pages));
    }

    private static boolean isLoaded(Page page)
    {
        for (int channel = 0; channel < page.getChannelCount(); ++channel) {
            if (!page.getBlock(channel).isLoaded()) {
                return false;
            }
        }

        return true;
    }
}
