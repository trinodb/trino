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
import io.trino.operator.join.JoinFilterFunction;
import io.trino.operator.join.LookupSource;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinFilterFunctionCompiler;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.HashArraySizeSupplier.defaultHashArraySizeSupplier;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;

public class TestPagesIndex
{
    @Test
    public void testEstimatedSize()
    {
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR);

        PagesIndex pagesIndex = newPagesIndex(types, 30, false);
        long initialEstimatedSize = pagesIndex.getEstimatedSize().toBytes();
        assertThat(initialEstimatedSize > 0)
                .describedAs(format("Initial estimated size must be positive, got %s", initialEstimatedSize))
                .isTrue();

        pagesIndex.addPage(somePage(types));
        long estimatedSizeWithOnePage = pagesIndex.getEstimatedSize().toBytes();
        assertThat(estimatedSizeWithOnePage > initialEstimatedSize)
                .describedAs("Estimated size should grow after adding a page")
                .isTrue();

        pagesIndex.addPage(somePage(types));
        long estimatedSizeWithTwoPages = pagesIndex.getEstimatedSize().toBytes();
        assertThat(estimatedSizeWithTwoPages)
                .describedAs("Estimated size should grow linearly as long as we don't pass expectedPositions")
                .isEqualTo(initialEstimatedSize + (estimatedSizeWithOnePage - initialEstimatedSize) * 2);

        pagesIndex.compact();
        long estimatedSizeAfterCompact = pagesIndex.getEstimatedSize().toBytes();
        // We can expect compact to reduce size because VARCHAR sequence pages are compactable.
        assertThat(estimatedSizeAfterCompact < estimatedSizeWithTwoPages)
                .describedAs(format(
                        "Compact should reduce (or retain) size, but changed from %s to %s",
                        estimatedSizeWithTwoPages,
                        estimatedSizeAfterCompact))
                .isTrue();
    }

    @Test
    public void testEagerCompact()
    {
        List<Type> types = ImmutableList.of(VARCHAR);

        PagesIndex lazyCompactPagesIndex = newPagesIndex(types, 50, false);
        PagesIndex eagerCompactPagesIndex = newPagesIndex(types, 50, true);

        for (int i = 0; i < 5; i++) {
            lazyCompactPagesIndex.addPage(somePage(types));
            eagerCompactPagesIndex.addPage(somePage(types));

            // We can expect eagerCompactPagesIndex retained less data than lazyCompactPagesIndex because
            // the pages used in the test (VARCHAR sequence pages) are compactable.
            assertThat(eagerCompactPagesIndex.getEstimatedSize().toBytes() < lazyCompactPagesIndex.getEstimatedSize().toBytes())
                    .describedAs("Expect eagerCompactPagesIndex retained less data than lazyCompactPagesIndex after adding the page, because the pages used in the test are compactable.")
                    .isTrue();
        }

        lazyCompactPagesIndex.compact();
        assertThat(lazyCompactPagesIndex.getEstimatedSize()).isEqualTo(eagerCompactPagesIndex.getEstimatedSize());
    }

    @Test
    public void testCompactWithNoColumns()
    {
        PagesIndex index = newPagesIndex(ImmutableList.of(), 50, false);
        index.addPage(new Page(10));
        index.addPage(new Page(20));

        index.compact();

        assertThat(index.getPositionCount()).isEqualTo(30);
    }

    @Test
    public void testGetPagesWithNoColumns()
    {
        PagesIndex index = newPagesIndex(ImmutableList.of(), 50, false);
        index.addPage(new Page(10));
        index.addPage(new Page(20));

        Iterator<Page> pages = index.getPages();
        assertThat(pages.next().getPositionCount()).isEqualTo(10);
        assertThat(pages.next().getPositionCount()).isEqualTo(20);
        assertThat(pages.hasNext()).isFalse();
    }

    @Test
    public void testGetEstimatedLookupSourceSizeInBytes()
    {
        for (Optional<Integer> sortChannel : Arrays.asList(Optional.<Integer>empty(), Optional.of(0), Optional.of(1))) {
            for (int joinChannel : Arrays.asList(0, 1)) {
                List<Type> types = ImmutableList.of(BIGINT, VARCHAR);
                PagesIndex pagesIndex = newPagesIndex(types, 50, false);
                int pageCount = 100;
                for (int i = 0; i < pageCount; i++) {
                    pagesIndex.addPage(somePage(types));
                }
                long pageIndexSize = pagesIndex.getEstimatedSize().toBytes();
                long estimatedMemoryRequiredToCreateLookupSource = pagesIndex.getEstimatedMemoryRequiredToCreateLookupSource(
                        defaultHashArraySizeSupplier(),
                        sortChannel,
                        ImmutableList.of(joinChannel));
                assertThat(estimatedMemoryRequiredToCreateLookupSource).isGreaterThan(pageIndexSize);
                long estimatedLookupSourceSize = estimatedMemoryRequiredToCreateLookupSource -
                        // subtract size of page positions
                        sizeOfIntArray(pageCount);
                long estimatedAdditionalSize = estimatedMemoryRequiredToCreateLookupSource - pageIndexSize;

                JoinFilterFunctionCompiler.JoinFilterFunctionFactory filterFunctionFactory = (session, addresses, pages) -> (JoinFilterFunction) (leftPosition, rightPosition, rightPage) -> false;
                LookupSource lookupSource = pagesIndex.createLookupSourceSupplier(
                        TEST_SESSION,
                        ImmutableList.of(joinChannel),
                        sortChannel.map(channel -> filterFunctionFactory),
                        sortChannel,
                        ImmutableList.of(filterFunctionFactory),
                        Optional.of(ImmutableList.of(0, 1)),
                        defaultHashArraySizeSupplier()).get();
                long actualLookupSourceSize = lookupSource.getInMemorySizeInBytes();
                assertThat(estimatedLookupSourceSize).isGreaterThanOrEqualTo(actualLookupSourceSize);
                assertThat(estimatedLookupSourceSize).isCloseTo(actualLookupSourceSize, withPercentage(1));

                long addressesSize = sizeOf(pagesIndex.getValueAddresses().elements());
                long channelsArraySize = sizeOf(pagesIndex.getChannel(0).elements()) * types.size();
                long blocksSize = 0;
                for (int channel = 0; channel < 2; channel++) {
                    blocksSize += pagesIndex.getChannel(channel).stream()
                            .mapToLong(Block::getRetainedSizeInBytes)
                            .sum();
                }
                long actualAdditionalSize = actualLookupSourceSize - (addressesSize + channelsArraySize + blocksSize);
                assertThat(estimatedAdditionalSize).isCloseTo(actualAdditionalSize, withPercentage(1));
            }
        }
    }

    private static PagesIndex newPagesIndex(List<Type> types, int expectedPositions, boolean eagerCompact)
    {
        return new PagesIndex.TestingFactory(eagerCompact).newPagesIndex(types, expectedPositions);
    }

    private static Page somePage(List<Type> types)
    {
        int[] initialValues = new int[types.size()];
        Arrays.setAll(initialValues, i -> 100 * i);
        return createSequencePage(types, 7, initialValues);
    }
}
