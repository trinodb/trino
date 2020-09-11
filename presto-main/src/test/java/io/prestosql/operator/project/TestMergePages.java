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
package io.prestosql.operator.project;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.List;

import static io.prestosql.SequencePageBuilder.createSequencePage;
import static io.prestosql.execution.buffer.PageSplitterUtil.splitPage;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.operator.WorkProcessorAssertion.assertFinishes;
import static io.prestosql.operator.WorkProcessorAssertion.validateResult;
import static io.prestosql.operator.project.MergePages.mergePages;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
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
                pagesSource(page),
                newSimpleAggregatedMemoryContext());

        validateResult(mergePages, actualPage -> assertPageEquals(TYPES, actualPage, page));
        assertFinishes(mergePages);
    }

    @Test
    public void testMinRowCountThresholdWithLazyPages()
    {
        Page page = createSequencePage(TYPES, 10);

        LazyBlock channel1 = lazyWrapper(page.getBlock(0));
        LazyBlock channel2 = lazyWrapper(page.getBlock(1));
        LazyBlock channel3 = lazyWrapper(page.getBlock(2));
        page = new Page(channel1, channel2, channel3);

        WorkProcessor<Page> mergePages = mergePages(
                TYPES,
                1024 * 1024,
                page.getPositionCount() * 2,
                Integer.MAX_VALUE,
                pagesSource(page),
                newSimpleAggregatedMemoryContext());

        assertTrue(mergePages.process());
        assertFalse(mergePages.isFinished());

        Page result = mergePages.getResult();
        assertFalse(channel1.isLoaded());
        assertFalse(channel2.isLoaded());
        assertFalse(channel3.isLoaded());
        assertPageEquals(TYPES, result, page);
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
                pagesSource(splits.get(0), splits.get(1), splits.get(0), splits.get(1)),
                newSimpleAggregatedMemoryContext());

        validateResult(mergePages, actualPage -> assertPageEquals(types, actualPage, page));
        validateResult(mergePages, actualPage -> assertPageEquals(types, actualPage, page));
        assertFinishes(mergePages);
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), block::getLoadedBlock);
    }

    private static WorkProcessor<Page> pagesSource(Page... pages)
    {
        return WorkProcessor.fromIterable(ImmutableList.copyOf(pages));
    }
}
