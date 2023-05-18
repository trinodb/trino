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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import io.trino.RowPagesBuilder;
import io.trino.operator.PagesHashStrategy;
import io.trino.operator.SimplePagesHashStrategy;
import io.trino.spi.Page;
import io.trino.spi.type.TypeOperators;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.SyntheticAddress.encodeSyntheticAddress;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPositionLinks
{
    private static final Page TEST_PAGE = getOnlyElement(RowPagesBuilder.rowPagesBuilder(BIGINT).addSequencePage(20, 0).build());

    @Test
    public void testArrayPositionLinks()
    {
        PositionLinks.FactoryBuilder factoryBuilder = ArrayPositionLinks.builder(1000);

        assertThat(factoryBuilder.link(1, 0)).isEqualTo(1);
        assertThat(factoryBuilder.link(2, 1)).isEqualTo(2);
        assertThat(factoryBuilder.link(3, 2)).isEqualTo(3);

        assertThat(factoryBuilder.link(11, 10)).isEqualTo(11);
        assertThat(factoryBuilder.link(12, 11)).isEqualTo(12);

        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of());

        assertThat(positionLinks.start(3, 0, TEST_PAGE)).isEqualTo(3);
        assertThat(positionLinks.next(3, 0, TEST_PAGE)).isEqualTo(2);
        assertThat(positionLinks.next(2, 0, TEST_PAGE)).isEqualTo(1);
        assertThat(positionLinks.next(1, 0, TEST_PAGE)).isEqualTo(0);

        assertThat(positionLinks.start(4, 0, TEST_PAGE)).isEqualTo(4);
        assertThat(positionLinks.next(4, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(12, 0, TEST_PAGE)).isEqualTo(12);
        assertThat(positionLinks.next(12, 0, TEST_PAGE)).isEqualTo(11);
        assertThat(positionLinks.next(11, 0, TEST_PAGE)).isEqualTo(10);
    }

    @Test
    public void testSortedPositionLinks()
    {
        JoinFilterFunction filterFunction = (leftAddress, rightPosition, rightPage) ->
                BIGINT.getLong(TEST_PAGE.getBlock(0), leftAddress) > 4;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunction));

        assertThat(positionLinks.start(0, 0, TEST_PAGE)).isEqualTo(5);
        assertThat(positionLinks.next(5, 0, TEST_PAGE)).isEqualTo(6);
        assertThat(positionLinks.next(6, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(7, 0, TEST_PAGE)).isEqualTo(7);
        assertThat(positionLinks.next(7, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(8, 0, TEST_PAGE)).isEqualTo(8);
        assertThat(positionLinks.next(8, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(9, 0, TEST_PAGE)).isEqualTo(9);
        assertThat(positionLinks.next(9, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(10, 0, TEST_PAGE)).isEqualTo(10);
        assertThat(positionLinks.next(10, 0, TEST_PAGE)).isEqualTo(11);
        assertThat(positionLinks.next(11, 0, TEST_PAGE)).isEqualTo(12);
        assertThat(positionLinks.next(12, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(13, 0, TEST_PAGE)).isEqualTo(13);
        assertThat(positionLinks.next(13, 0, TEST_PAGE)).isEqualTo(-1);
    }

    @Test
    public void testSortedPositionLinksAllMatch()
    {
        JoinFilterFunction filterFunction = (leftAddress, rightPosition, rightPage) ->
                BIGINT.getLong(rightPage.getBlock(0), leftAddress) >= 0;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunction));

        assertThat(positionLinks.start(0, 0, TEST_PAGE)).isEqualTo(0);
        assertThat(positionLinks.next(0, 0, TEST_PAGE)).isEqualTo(1);
        assertThat(positionLinks.next(1, 0, TEST_PAGE)).isEqualTo(2);
        assertThat(positionLinks.next(2, 0, TEST_PAGE)).isEqualTo(3);
        assertThat(positionLinks.next(3, 0, TEST_PAGE)).isEqualTo(4);
        assertThat(positionLinks.next(4, 0, TEST_PAGE)).isEqualTo(5);
        assertThat(positionLinks.next(5, 0, TEST_PAGE)).isEqualTo(6);
        assertThat(positionLinks.next(6, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(7, 0, TEST_PAGE)).isEqualTo(7);
        assertThat(positionLinks.next(7, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(8, 0, TEST_PAGE)).isEqualTo(8);
        assertThat(positionLinks.next(8, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(9, 0, TEST_PAGE)).isEqualTo(9);
        assertThat(positionLinks.next(9, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(10, 0, TEST_PAGE)).isEqualTo(10);
        assertThat(positionLinks.next(10, 0, TEST_PAGE)).isEqualTo(11);
        assertThat(positionLinks.next(11, 0, TEST_PAGE)).isEqualTo(12);
        assertThat(positionLinks.next(12, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(13, 0, TEST_PAGE)).isEqualTo(13);
        assertThat(positionLinks.next(13, 0, TEST_PAGE)).isEqualTo(-1);
    }

    @Test
    public void testSortedPositionLinksForRangePredicates()
    {
        JoinFilterFunction filterFunctionOne = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(TEST_PAGE.getBlock(0), leftAddress) > 4;

        JoinFilterFunction filterFunctionTwo = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(TEST_PAGE.getBlock(0), leftAddress) <= 11;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunctionOne, filterFunctionTwo));

        assertThat(positionLinks.start(0, 0, TEST_PAGE)).isEqualTo(5);
        assertThat(positionLinks.next(4, 0, TEST_PAGE)).isEqualTo(5);
        assertThat(positionLinks.next(5, 0, TEST_PAGE)).isEqualTo(6);
        assertThat(positionLinks.next(6, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(7, 0, TEST_PAGE)).isEqualTo(7);
        assertThat(positionLinks.next(7, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(8, 0, TEST_PAGE)).isEqualTo(8);
        assertThat(positionLinks.next(8, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(9, 0, TEST_PAGE)).isEqualTo(9);
        assertThat(positionLinks.next(9, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(10, 0, TEST_PAGE)).isEqualTo(10);
        assertThat(positionLinks.next(10, 0, TEST_PAGE)).isEqualTo(11);
        assertThat(positionLinks.next(11, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(13, 0, TEST_PAGE)).isEqualTo(-1);
    }

    @Test
    public void testSortedPositionLinksForRangePredicatesPrefixMatch()
    {
        JoinFilterFunction filterFunctionOne = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(0), leftAddress) >= 0;

        JoinFilterFunction filterFunctionTwo = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(0), leftAddress) <= 11;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunctionOne, filterFunctionTwo));

        assertThat(positionLinks.start(0, 0, TEST_PAGE)).isEqualTo(0);
        assertThat(positionLinks.next(0, 0, TEST_PAGE)).isEqualTo(1);
        assertThat(positionLinks.next(1, 0, TEST_PAGE)).isEqualTo(2);
        assertThat(positionLinks.next(2, 0, TEST_PAGE)).isEqualTo(3);
        assertThat(positionLinks.next(3, 0, TEST_PAGE)).isEqualTo(4);
        assertThat(positionLinks.next(4, 0, TEST_PAGE)).isEqualTo(5);
        assertThat(positionLinks.next(5, 0, TEST_PAGE)).isEqualTo(6);
        assertThat(positionLinks.next(6, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(7, 0, TEST_PAGE)).isEqualTo(7);
        assertThat(positionLinks.next(7, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(8, 0, TEST_PAGE)).isEqualTo(8);
        assertThat(positionLinks.next(8, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(9, 0, TEST_PAGE)).isEqualTo(9);
        assertThat(positionLinks.next(9, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(10, 0, TEST_PAGE)).isEqualTo(10);
        assertThat(positionLinks.next(10, 0, TEST_PAGE)).isEqualTo(11);
        assertThat(positionLinks.next(11, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(13, 0, TEST_PAGE)).isEqualTo(-1);
    }

    @Test
    public void testSortedPositionLinksForRangePredicatesSuffixMatch()
    {
        JoinFilterFunction filterFunctionOne = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(0), leftAddress) > 4;

        JoinFilterFunction filterFunctionTwo = (leftAddress, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(0), leftAddress) < 100;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunctionOne, filterFunctionTwo));

        assertThat(positionLinks.start(0, 0, TEST_PAGE)).isEqualTo(5);
        assertThat(positionLinks.next(4, 0, TEST_PAGE)).isEqualTo(5);
        assertThat(positionLinks.next(5, 0, TEST_PAGE)).isEqualTo(6);
        assertThat(positionLinks.next(6, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(7, 0, TEST_PAGE)).isEqualTo(7);
        assertThat(positionLinks.next(7, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(8, 0, TEST_PAGE)).isEqualTo(8);
        assertThat(positionLinks.next(8, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(9, 0, TEST_PAGE)).isEqualTo(9);
        assertThat(positionLinks.next(9, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(10, 0, TEST_PAGE)).isEqualTo(10);
        assertThat(positionLinks.next(10, 0, TEST_PAGE)).isEqualTo(11);
        assertThat(positionLinks.next(11, 0, TEST_PAGE)).isEqualTo(12);
        assertThat(positionLinks.next(12, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(13, 0, TEST_PAGE)).isEqualTo(13);
        assertThat(positionLinks.next(13, 0, TEST_PAGE)).isEqualTo(-1);
    }

    @Test
    public void testReverseSortedPositionLinks()
    {
        JoinFilterFunction filterFunction = (leftAddress, rightPosition, rightPage) ->
                BIGINT.getLong(TEST_PAGE.getBlock(0), leftAddress) < 4;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunction));

        assertThat(positionLinks.start(0, 0, TEST_PAGE)).isEqualTo(0);
        assertThat(positionLinks.next(0, 0, TEST_PAGE)).isEqualTo(1);
        assertThat(positionLinks.next(1, 0, TEST_PAGE)).isEqualTo(2);
        assertThat(positionLinks.next(2, 0, TEST_PAGE)).isEqualTo(3);
        assertThat(positionLinks.next(3, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(10, 0, TEST_PAGE)).isEqualTo(-1);
    }

    @Test
    public void testReverseSortedPositionLinksAllMatch()
    {
        JoinFilterFunction filterFunction = (leftAddress, rightPosition, rightPage) ->
                BIGINT.getLong(rightPage.getBlock(0), leftAddress) < 13;

        PositionLinks.FactoryBuilder factoryBuilder = buildSortedPositionLinks();
        PositionLinks positionLinks = factoryBuilder.build().create(ImmutableList.of(filterFunction));

        assertThat(positionLinks.start(0, 0, TEST_PAGE)).isEqualTo(0);
        assertThat(positionLinks.next(0, 0, TEST_PAGE)).isEqualTo(1);
        assertThat(positionLinks.next(1, 0, TEST_PAGE)).isEqualTo(2);
        assertThat(positionLinks.next(2, 0, TEST_PAGE)).isEqualTo(3);
        assertThat(positionLinks.next(3, 0, TEST_PAGE)).isEqualTo(4);
        assertThat(positionLinks.next(4, 0, TEST_PAGE)).isEqualTo(5);
        assertThat(positionLinks.next(5, 0, TEST_PAGE)).isEqualTo(6);
        assertThat(positionLinks.next(6, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(7, 0, TEST_PAGE)).isEqualTo(7);
        assertThat(positionLinks.next(7, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(8, 0, TEST_PAGE)).isEqualTo(8);
        assertThat(positionLinks.next(8, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(9, 0, TEST_PAGE)).isEqualTo(9);
        assertThat(positionLinks.next(9, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(10, 0, TEST_PAGE)).isEqualTo(10);
        assertThat(positionLinks.next(10, 0, TEST_PAGE)).isEqualTo(11);
        assertThat(positionLinks.next(11, 0, TEST_PAGE)).isEqualTo(12);
        assertThat(positionLinks.next(12, 0, TEST_PAGE)).isEqualTo(-1);

        assertThat(positionLinks.start(13, 0, TEST_PAGE)).isEqualTo(-1);
    }

    private static PositionLinks.FactoryBuilder buildSortedPositionLinks()
    {
        SortedPositionLinks.FactoryBuilder builder = SortedPositionLinks.builder(
                1000,
                pagesHashStrategy(),
                addresses());

        /*
         * Built sorted positions links
         *
         * [0] -> [1,2,3,4,5,6]
         * [10] -> [11,12]
         */

        assertThat(builder.link(4, 5)).isEqualTo(4);
        assertThat(builder.link(6, 4)).isEqualTo(4);
        assertThat(builder.link(2, 4)).isEqualTo(2);
        assertThat(builder.link(3, 2)).isEqualTo(2);
        assertThat(builder.link(0, 2)).isEqualTo(0);
        assertThat(builder.link(1, 0)).isEqualTo(0);

        assertThat(builder.link(10, 11)).isEqualTo(10);
        assertThat(builder.link(12, 10)).isEqualTo(10);

        return builder;
    }

    private static PagesHashStrategy pagesHashStrategy()
    {
        return new SimplePagesHashStrategy(
                ImmutableList.of(BIGINT),
                ImmutableList.of(),
                ImmutableList.of(new ObjectArrayList<>(ImmutableList.of(TEST_PAGE.getBlock(0)))),
                ImmutableList.of(),
                OptionalInt.empty(),
                Optional.of(0),
                new BlockTypeOperators(new TypeOperators()));
    }

    private static LongArrayList addresses()
    {
        LongArrayList addresses = new LongArrayList();
        for (int i = 0; i < TEST_PAGE.getPositionCount(); ++i) {
            addresses.add(encodeSyntheticAddress(0, i));
        }
        return addresses;
    }
}
