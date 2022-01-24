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
import com.google.common.primitives.Ints;
import io.trino.spi.Page;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.RowPagesBuilder.rowPagesBuilder;
import static io.trino.operator.GroupByHashFactoryTestUtils.createGroupByHashFactory;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.operator.UpdateMemory.NOOP;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestGroupedTopNRowNumberBuilder
{
    private static final TypeOperators TYPE_OPERATORS_CACHE = new TypeOperators();

    @DataProvider
    public static Object[][] produceRowNumbers()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider
    public static Object[][] pageRowCounts()
    {
        // make either page or row count > 1024 to expand the big arrays
        return new Object[][] {{10000, 20}, {20, 10000}};
    }

    @Test
    public void testEmptyInput()
    {
        GroupedTopNBuilder groupedTopNBuilder = new GroupedTopNRowNumberBuilder(
                ImmutableList.of(BIGINT),
                (left, leftPosition, right, rightPosition) -> {
                    throw new UnsupportedOperationException();
                },
                5,
                false,
                new NoChannelGroupByHash());
        assertFalse(groupedTopNBuilder.buildResult().hasNext());
    }

    @Test(dataProvider = "produceRowNumbers")
    public void testMultiGroupTopN(boolean produceRowNumbers)
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);
        List<Page> input = rowPagesBuilder(types)
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.9)
                .row(3L, 0.1)
                .pageBreak()
                .row(1L, 0.4)
                .pageBreak()
                .row(1L, 0.5)
                .row(1L, 0.6)
                .row(4L, 0.6)
                .row(2L, 0.8)
                .row(2L, 0.7)
                .pageBreak()
                .row(2L, 0.9)
                .build();

        for (Page page : input) {
            page.compact();
        }

        GroupByHash groupByHash = createGroupByHash(ImmutableList.of(types.get(0)), ImmutableList.of(0), NOOP);
        GroupedTopNBuilder groupedTopNBuilder = new GroupedTopNRowNumberBuilder(
                types,
                new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST), TYPE_OPERATORS_CACHE),
                2,
                produceRowNumbers,
                groupByHash);

        // add 4 rows for the first page and created three heaps with 1, 1, 2 rows respectively
        assertTrue(groupedTopNBuilder.processPage(input.get(0)).process());

        // add 1 row for the second page and the three heaps become 2, 1, 2 rows respectively
        assertTrue(groupedTopNBuilder.processPage(input.get(1)).process());

        // add 2 new rows for the third page (which will be compacted into two rows only) and we have four heaps with 2, 2, 2, 1 rows respectively
        assertTrue(groupedTopNBuilder.processPage(input.get(2)).process());

        // the last page will be discarded
        assertTrue(groupedTopNBuilder.processPage(input.get(3)).process());

        List<Page> output = ImmutableList.copyOf(groupedTopNBuilder.buildResult());
        assertEquals(output.size(), 1);

        Page expected = rowPagesBuilder(BIGINT, DOUBLE, BIGINT)
                .row(1L, 0.3, 1)
                .row(1L, 0.4, 2)
                .row(2L, 0.2, 1)
                .row(2L, 0.7, 2)
                .row(3L, 0.1, 1)
                .row(3L, 0.9, 2)
                .row(4L, 0.6, 1)
                .build()
                .get(0);
        if (produceRowNumbers) {
            assertPageEquals(ImmutableList.of(BIGINT, DOUBLE, BIGINT), output.get(0), expected);
        }
        else {
            assertPageEquals(types, output.get(0), new Page(expected.getBlock(0), expected.getBlock(1)));
        }
    }

    @Test(dataProvider = "produceRowNumbers")
    public void testSingleGroupTopN(boolean produceRowNumbers)
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);
        List<Page> input = rowPagesBuilder(types)
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.9)
                .row(3L, 0.1)
                .pageBreak()
                .row(1L, 0.4)
                .pageBreak()
                .row(1L, 0.5)
                .row(1L, 0.6)
                .row(4L, 0.6)
                .row(2L, 0.8)
                .row(2L, 0.7)
                .pageBreak()
                .row(2L, 0.9)
                .build();

        for (Page page : input) {
            page.compact();
        }

        GroupedTopNBuilder groupedTopNBuilder = new GroupedTopNRowNumberBuilder(
                types,
                new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST), TYPE_OPERATORS_CACHE),
                5,
                produceRowNumbers,
                new NoChannelGroupByHash());

        // add 4 rows for the first page and created a single heap with 4 rows
        assertTrue(groupedTopNBuilder.processPage(input.get(0)).process());

        // add 1 row for the second page and the heap is with 5 rows
        assertTrue(groupedTopNBuilder.processPage(input.get(1)).process());

        // update 1 new row from the third page (which will be compacted into a single row only)
        assertTrue(groupedTopNBuilder.processPage(input.get(2)).process());

        // the last page will be discarded
        assertTrue(groupedTopNBuilder.processPage(input.get(3)).process());

        List<Page> output = ImmutableList.copyOf(groupedTopNBuilder.buildResult());
        assertEquals(output.size(), 1);

        Page expected = rowPagesBuilder(BIGINT, DOUBLE, BIGINT)
                .row(3L, 0.1, 1)
                .row(2L, 0.2, 2)
                .row(1L, 0.3, 3)
                .row(1L, 0.4, 4)
                .row(1L, 0.5, 5)
                .build()
                .get(0);
        if (produceRowNumbers) {
            assertPageEquals(ImmutableList.of(BIGINT, DOUBLE, BIGINT), output.get(0), expected);
        }
        else {
            assertPageEquals(types, output.get(0), new Page(expected.getBlock(0), expected.getBlock(1)));
        }
    }

    @Test
    public void testYield()
    {
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);
        Page input = rowPagesBuilder(types)
                .row(1L, 0.3)
                .row(1L, 0.2)
                .row(1L, 0.9)
                .row(1L, 0.1)
                .build()
                .get(0);
        input.compact();

        AtomicBoolean unblock = new AtomicBoolean();
        GroupByHash groupByHash = createGroupByHash(ImmutableList.of(types.get(0)), ImmutableList.of(0), unblock::get);
        GroupedTopNBuilder groupedTopNBuilder = new GroupedTopNRowNumberBuilder(
                types,
                new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST), TYPE_OPERATORS_CACHE),
                5,
                false,
                groupByHash);

        Work<?> work = groupedTopNBuilder.processPage(input);
        assertFalse(work.process());
        assertFalse(work.process());
        unblock.set(true);
        assertTrue(work.process());
        List<Page> output = ImmutableList.copyOf(groupedTopNBuilder.buildResult());
        assertEquals(output.size(), 1);

        Page expected = rowPagesBuilder(types)
                .row(1L, 0.1)
                .row(1L, 0.2)
                .row(1L, 0.3)
                .row(1L, 0.9)
                .build()
                .get(0);
        assertPageEquals(types, output.get(0), expected);
    }

    private static GroupByHash createGroupByHash(List<Type> partitionTypes, List<Integer> partitionChannels, UpdateMemory updateMemory)
    {
        return createGroupByHashFactory()
                .createGroupByHash(
                        partitionTypes,
                        Ints.toArray(partitionChannels),
                        Optional.empty(),
                        1,
                        false,
                        updateMemory);
    }
}
