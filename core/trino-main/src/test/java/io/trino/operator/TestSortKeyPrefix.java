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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.gen.OrderingCompiler;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSortKeyPrefix
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final OrderingCompiler ORDERING_COMPILER = new OrderingCompiler(TYPE_OPERATORS);

    private static final List<Long> LONGS = ImmutableList.of(Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L, 42L, Long.MAX_VALUE - 1, Long.MAX_VALUE);
    private static final List<Long> INTEGERS = ImmutableList.of((long) Integer.MIN_VALUE, -1L, 0L, 1L, 42L, (long) Integer.MAX_VALUE);
    private static final List<Long> SMALLINTS = ImmutableList.of((long) Short.MIN_VALUE, -1L, 0L, 1L, 42L, (long) Short.MAX_VALUE);
    private static final List<Long> TINYINTS = ImmutableList.of((long) Byte.MIN_VALUE, -1L, 0L, 1L, 42L, (long) Byte.MAX_VALUE);

    private static final List<Slice> SLICES = ImmutableList.of(
            utf8Slice(""),
            utf8Slice("a"),
            utf8Slice("ab"),
            // a trailing zero byte has the same prefix as the padding of a shorter value
            Slices.wrappedBuffer(new byte[] {'a', 'b', 0}),
            utf8Slice("abcdefg"),
            utf8Slice("abcdefgh"),
            utf8Slice("abcdefgh1"),
            utf8Slice("abcdefgh2"),
            utf8Slice("b"),
            // bytes that are negative when read as signed
            Slices.wrappedBuffer(new byte[] {0}),
            Slices.wrappedBuffer(new byte[] {0x7F}),
            Slices.wrappedBuffer(new byte[] {(byte) 0x80}),
            Slices.wrappedBuffer(new byte[] {(byte) 0xFF}),
            Slices.wrappedBuffer(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
            Slices.wrappedBuffer(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 1}));

    @Test
    public void testLongValues()
    {
        assertPrefixOrdersLikeComparator(BIGINT, longBlock(BIGINT, LONGS));
        assertPrefixOrdersLikeComparator(createDecimalType(18, 2), longBlock(createDecimalType(18, 2), LONGS));
        assertPrefixOrdersLikeComparator(TIMESTAMP_MICROS, longBlock(TIMESTAMP_MICROS, LONGS));
        assertPrefixOrdersLikeComparator(TIME_MICROS, longBlock(TIME_MICROS, LONGS));
        assertPrefixOrdersLikeComparator(INTEGER, longBlock(INTEGER, INTEGERS));
        assertPrefixOrdersLikeComparator(DATE, longBlock(DATE, INTEGERS));
        assertPrefixOrdersLikeComparator(SMALLINT, longBlock(SMALLINT, SMALLINTS));
        assertPrefixOrdersLikeComparator(TINYINT, longBlock(TINYINT, TINYINTS));
    }

    @Test
    public void testSliceValues()
    {
        assertPrefixOrdersLikeComparator(VARCHAR, sliceBlock(VARCHAR, SLICES));
        assertPrefixOrdersLikeComparator(VARBINARY, sliceBlock(VARBINARY, SLICES));
    }

    @Test
    public void testDictionaryAndRunLengthEncodedBlocks()
    {
        Block longs = longBlock(BIGINT, LONGS);
        assertPrefixOrdersLikeComparator(BIGINT, reversedDictionary(longs));
        Block slices = sliceBlock(VARCHAR, SLICES);
        assertPrefixOrdersLikeComparator(VARCHAR, reversedDictionary(slices));

        for (SortOrder sortOrder : SortOrder.values()) {
            for (PageWithPositionComparator comparator : comparators(ImmutableList.of(BIGINT), ImmutableList.of(sortOrder))) {
                Page values = new Page(longs);
                for (int position = 0; position < longs.getPositionCount(); position++) {
                    Page repeated = new Page(RunLengthEncodedBlock.create(longs.getSingleValueBlock(position), 3));
                    assertThat(comparator.sortKeyPrefix(repeated, 2)).isEqualTo(comparator.sortKeyPrefix(values, position));
                }
            }
        }
    }

    @Test
    public void testPrefixOfFirstSortChannel()
    {
        Block first = longBlock(BIGINT, ImmutableList.of(1L, 1L, 2L, 2L));
        Block second = sliceBlock(VARCHAR, ImmutableList.of(utf8Slice("b"), utf8Slice("a"), utf8Slice("b"), utf8Slice("a")));
        // the second block is the first sort channel
        Page page = new Page(second, first);
        for (PageWithPositionComparator comparator : comparators(ImmutableList.of(BIGINT, VARCHAR), ImmutableList.of(SortOrder.DESC_NULLS_LAST, SortOrder.ASC_NULLS_LAST), ImmutableList.of(1, 0))) {
            assertThat(comparator.sortKeyPrefix(page, 0)).isEqualTo(comparator.sortKeyPrefix(page, 1));
            assertThat(comparator.sortKeyPrefix(page, 2)).isEqualTo(comparator.sortKeyPrefix(page, 3));
            assertThat(comparator.sortKeyPrefix(page, 2)).isLessThan(comparator.sortKeyPrefix(page, 0));
            assertPrefixOrdersLikeComparator(comparator, page);
        }
    }

    @Test
    public void testUnsupportedType()
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 3);
        DOUBLE.writeDouble(blockBuilder, 1.5);
        blockBuilder.appendNull();
        DOUBLE.writeDouble(blockBuilder, -2.5);
        Page page = new Page(blockBuilder.build());
        for (SortOrder sortOrder : SortOrder.values()) {
            for (PageWithPositionComparator comparator : comparators(ImmutableList.of(DOUBLE), ImmutableList.of(sortOrder))) {
                assertThat(comparator.sortKeyPrefix(page, 0)).isEqualTo(comparator.sortKeyPrefix(page, 1));
                assertThat(comparator.sortKeyPrefix(page, 0)).isEqualTo(comparator.sortKeyPrefix(page, 2));
            }
        }
    }

    private static void assertPrefixOrdersLikeComparator(Type type, Block block)
    {
        Page page = new Page(block);
        for (SortOrder sortOrder : SortOrder.values()) {
            List<PageWithPositionComparator> comparators = comparators(ImmutableList.of(type), ImmutableList.of(sortOrder));
            for (PageWithPositionComparator comparator : comparators) {
                assertThat(assertPrefixOrdersLikeComparator(comparator, page))
                        .as("%s %s has prefixes that order rows", type, sortOrder)
                        .isTrue();
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                assertThat(comparators.get(0).sortKeyPrefix(page, position))
                        .as("%s %s position %s", type, sortOrder, position)
                        .isEqualTo(comparators.get(1).sortKeyPrefix(page, position));
            }
        }
    }

    /**
     * Returns whether any two rows had different prefixes
     */
    private static boolean assertPrefixOrdersLikeComparator(PageWithPositionComparator comparator, Page page)
    {
        boolean prefixesDiffer = false;
        for (int left = 0; left < page.getPositionCount(); left++) {
            for (int right = 0; right < page.getPositionCount(); right++) {
                long leftPrefix = comparator.sortKeyPrefix(page, left);
                long rightPrefix = comparator.sortKeyPrefix(page, right);
                if (leftPrefix != rightPrefix) {
                    prefixesDiffer = true;
                    assertThat(Integer.signum(comparator.compareTo(page, left, page, right)))
                            .as("positions %s and %s", left, right)
                            .isEqualTo(Long.compare(leftPrefix, rightPrefix));
                }
            }
        }
        return prefixesDiffer;
    }

    private static List<PageWithPositionComparator> comparators(List<Type> sortTypes, List<SortOrder> sortOrders)
    {
        return comparators(sortTypes, sortOrders, IntStream.range(0, sortTypes.size()).boxed().collect(toImmutableList()));
    }

    private static List<PageWithPositionComparator> comparators(List<Type> sortTypes, List<SortOrder> sortOrders, List<Integer> sortChannels)
    {
        PageWithPositionComparator compiled = ORDERING_COMPILER.compilePageWithPositionComparator(sortTypes, sortChannels, sortOrders);
        // the compiler falls back to the simple comparator when it fails to generate a class
        assertThat(compiled).isNotInstanceOf(SimplePageWithPositionComparator.class);
        return ImmutableList.of(compiled, new SimplePageWithPositionComparator(sortTypes, sortChannels, sortOrders, TYPE_OPERATORS));
    }

    private static Block longBlock(Type type, List<Long> values)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.size() + 1);
        for (long value : values) {
            type.writeLong(blockBuilder, value);
        }
        blockBuilder.appendNull();
        return blockBuilder.build();
    }

    private static Block sliceBlock(Type type, List<Slice> values)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.size() + 1);
        for (Slice value : values) {
            type.writeSlice(blockBuilder, value);
        }
        blockBuilder.appendNull();
        return blockBuilder.build();
    }

    private static Block reversedDictionary(Block block)
    {
        int positionCount = block.getPositionCount();
        int[] ids = IntStream.range(0, positionCount)
                .map(position -> positionCount - 1 - position)
                .toArray();
        return DictionaryBlock.create(positionCount, block, ids);
    }
}
