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
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.IpAddressType.IPADDRESS;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPagesIndexOrdering
{
    private static final TypeOperators TYPE_OPERATORS = new TypeOperators();
    private static final int POSITION_COUNT = 4000;

    @Test
    public void testBigint()
    {
        Random random = new Random(42);
        assertSortsCorrectly(BIGINT, values(
                random,
                ImmutableList.of(Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L, Long.MAX_VALUE - 1, Long.MAX_VALUE, 1L << 32, -(1L << 32), 42L, 43L),
                () -> randomWithTies(random, random::nextLong)));
    }

    @Test
    public void testInteger()
    {
        Random random = new Random(42);
        assertSortsCorrectly(INTEGER, values(
                random,
                ImmutableList.of((long) Integer.MIN_VALUE, (long) Integer.MIN_VALUE + 1, -1L, 0L, 1L, (long) Integer.MAX_VALUE),
                () -> randomWithTies(random, () -> (long) random.nextInt())));
    }

    @Test
    public void testSmallint()
    {
        Random random = new Random(42);
        assertSortsCorrectly(SMALLINT, values(
                random,
                ImmutableList.of((long) Short.MIN_VALUE, -1L, 0L, 1L, (long) Short.MAX_VALUE),
                () -> (long) (short) random.nextInt()));
    }

    @Test
    public void testTinyint()
    {
        Random random = new Random(42);
        assertSortsCorrectly(TINYINT, values(
                random,
                ImmutableList.of((long) Byte.MIN_VALUE, -1L, 0L, 1L, (long) Byte.MAX_VALUE),
                () -> (long) (byte) random.nextInt()));
    }

    @Test
    public void testDate()
    {
        Random random = new Random(42);
        assertSortsCorrectly(DATE, values(
                random,
                ImmutableList.of((long) Integer.MIN_VALUE, -1L, 0L, 1L, (long) Integer.MAX_VALUE),
                () -> randomWithTies(random, () -> (long) random.nextInt())));
    }

    @Test
    public void testTime()
    {
        Random random = new Random(42);
        assertSortsCorrectly(TIME_MICROS, values(
                random,
                ImmutableList.of(0L, 1L, PICOSECONDS_PER_DAY - 1),
                () -> random.nextLong(PICOSECONDS_PER_DAY)));
    }

    @Test
    public void testTimestamp()
    {
        Random random = new Random(42);
        assertSortsCorrectly(TIMESTAMP_MICROS, values(
                random,
                ImmutableList.of(Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE),
                () -> randomWithTies(random, random::nextLong)));
    }

    @Test
    public void testDouble()
    {
        Random random = new Random(42);
        assertSortsCorrectly(DOUBLE, values(
                random,
                ImmutableList.of(
                        Double.NaN,
                        Double.longBitsToDouble(0xFFF8_0000_0000_0001L),
                        Double.NEGATIVE_INFINITY,
                        Double.POSITIVE_INFINITY,
                        -0.0,
                        0.0,
                        Double.MIN_VALUE,
                        -Double.MIN_VALUE,
                        Double.MAX_VALUE,
                        -Double.MAX_VALUE,
                        1.0,
                        Math.nextUp(1.0),
                        -1.0,
                        Math.nextUp(-1.0)),
                () -> randomWithTies(random, () -> (double) random.nextInt(10), random::nextDouble)));
    }

    @Test
    public void testReal()
    {
        Random random = new Random(42);
        assertSortsCorrectly(REAL, values(
                random,
                ImmutableList.of(
                        realValue(Float.NaN),
                        // non-canonical NaN bit pattern, as it could appear in stored data
                        (long) 0xFFC0_0001,
                        realValue(Float.NEGATIVE_INFINITY),
                        realValue(Float.POSITIVE_INFINITY),
                        realValue(-0.0f),
                        realValue(0.0f),
                        realValue(Float.MIN_VALUE),
                        realValue(-Float.MIN_VALUE),
                        realValue(1.0f),
                        realValue(Math.nextUp(1.0f)),
                        realValue(-1.0f)),
                () -> randomWithTies(random, () -> realValue(random.nextInt(10)), () -> realValue(random.nextFloat() * 1000 - 500))));
    }

    @Test
    public void testVarchar()
    {
        Random random = new Random(42);
        assertSortsCorrectly(VARCHAR, values(
                random,
                ImmutableList.of(
                        Slices.utf8Slice(""),
                        Slices.utf8Slice("a"),
                        Slices.utf8Slice("ab"),
                        Slices.utf8Slice("abc\0"),
                        Slices.utf8Slice("abc\0a"),
                        Slices.utf8Slice("abc"),
                        Slices.utf8Slice("abcdefgh"),
                        Slices.utf8Slice("abcdefgha"),
                        Slices.utf8Slice("abcdefghb"),
                        Slices.utf8Slice("https://example.com/category/aaaa"),
                        Slices.utf8Slice("https://example.com/category/bbbb"),
                        Slices.utf8Slice("zażółć gęślą jaźń")),
                () -> Slices.utf8Slice(randomString(random))));
    }

    @Test
    public void testVarbinary()
    {
        Random random = new Random(42);
        assertSortsCorrectly(VARBINARY, values(
                random,
                ImmutableList.of(
                        Slices.wrappedBuffer(),
                        Slices.wrappedBuffer((byte) 0x00),
                        Slices.wrappedBuffer((byte) 0x00, (byte) 0x00),
                        Slices.wrappedBuffer((byte) 0x7F),
                        Slices.wrappedBuffer((byte) 0x80),
                        Slices.wrappedBuffer((byte) 0xFF),
                        Slices.wrappedBuffer((byte) 0xFF, (byte) 0x00)),
                () -> {
                    byte[] bytes = new byte[random.nextInt(12)];
                    random.nextBytes(bytes);
                    return Slices.wrappedBuffer(bytes);
                }));
    }

    @Test
    public void testMultipleSortChannels()
    {
        // tie-heavy leading channel forces the tie breaker onto the secondary channel
        Random random = new Random(42);
        List<Object> leading = values(random, ImmutableList.of(0L, 1L, 2L), () -> (long) random.nextInt(5));
        List<Object> secondary = values(random, ImmutableList.of(Long.MIN_VALUE, Long.MAX_VALUE), random::nextLong);

        for (SortOrder leadingOrder : SortOrder.values()) {
            for (SortOrder secondaryOrder : ImmutableList.of(SortOrder.ASC_NULLS_FIRST, SortOrder.DESC_NULLS_LAST)) {
                assertSortCorrect(
                        ImmutableList.of(BIGINT, BIGINT),
                        ImmutableList.of(0, 1),
                        ImmutableList.of(leadingOrder, secondaryOrder),
                        buildPages(ImmutableList.of(BIGINT, BIGINT), ImmutableList.of(leading, secondary)));
            }
        }
    }

    @Test
    public void testCompositePrefixIntegerInteger()
    {
        // 32 + 1 null bit each: second channel truncated by 2 bits
        Random random = new Random(42);
        List<Object> leading = values(random, ImmutableList.of(0L, 1L), () -> (long) random.nextInt(4));
        List<Object> secondary = values(random, ImmutableList.of((long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE, -1L, 0L), () -> (long) random.nextInt());

        for (SortOrder leadingOrder : SortOrder.values()) {
            for (SortOrder secondaryOrder : SortOrder.values()) {
                assertSortCorrect(
                        ImmutableList.of(INTEGER, INTEGER),
                        ImmutableList.of(0, 1),
                        ImmutableList.of(leadingOrder, secondaryOrder),
                        buildPages(ImmutableList.of(INTEGER, INTEGER), ImmutableList.of(leading, secondary)));
            }
        }
    }

    @Test
    public void testCompositePrefixSmallintChannelsDecideTies()
    {
        // 17 bits per field: three smallint channels pack exactly, so ties are decided by the prefix
        Random random = new Random(42);
        List<Object> first = values(random, ImmutableList.of((long) Short.MIN_VALUE, (long) Short.MAX_VALUE), () -> (long) (short) random.nextInt(8));
        List<Object> second = values(random, ImmutableList.of(0L), () -> (long) (short) random.nextInt(8));
        List<Object> third = values(random, ImmutableList.of(0L), () -> (long) (short) random.nextInt());

        assertSortCorrect(
                ImmutableList.of(SMALLINT, SMALLINT, SMALLINT),
                ImmutableList.of(0, 1, 2),
                ImmutableList.of(SortOrder.ASC_NULLS_FIRST, SortOrder.DESC_NULLS_LAST, SortOrder.ASC_NULLS_LAST),
                buildPages(ImmutableList.of(SMALLINT, SMALLINT, SMALLINT), ImmutableList.of(first, second, third)));
    }

    @Test
    public void testCompositePrefixBooleanBigint()
    {
        Random random = new Random(42);
        List<Object> leading = values(random, ImmutableList.of(true, false), random::nextBoolean);
        List<Object> secondary = values(random, ImmutableList.of(Long.MIN_VALUE, Long.MAX_VALUE, 0L, 1L, -1L), () -> randomWithTies(random, random::nextLong));

        for (SortOrder leadingOrder : SortOrder.values()) {
            assertSortCorrect(
                    ImmutableList.of(BOOLEAN, BIGINT),
                    ImmutableList.of(0, 1),
                    ImmutableList.of(leadingOrder, SortOrder.ASC_NULLS_LAST),
                    buildPages(ImmutableList.of(BOOLEAN, BIGINT), ImmutableList.of(leading, secondary)));
        }
    }

    @Test
    public void testCompositePrefixIntegerVarchar()
    {
        // varchar is inexact, so it must be the last packed channel
        Random random = new Random(42);
        List<Object> leading = values(random, ImmutableList.of(0L, 1L), () -> (long) random.nextInt(4));
        List<Object> secondary = values(random, ImmutableList.of(Slices.utf8Slice(""), Slices.utf8Slice("a")), () -> Slices.utf8Slice(randomString(random)));

        assertSortCorrect(
                ImmutableList.of(INTEGER, VARCHAR),
                ImmutableList.of(0, 1),
                ImmutableList.of(SortOrder.ASC_NULLS_FIRST, SortOrder.DESC_NULLS_LAST),
                buildPages(ImmutableList.of(INTEGER, VARCHAR), ImmutableList.of(leading, secondary)));
    }

    @Test
    public void testVarcharWithCommonPrefixAndTieBreaker()
    {
        // all keys share the first 8 bytes, so every quicksort comparison falls back to the comparator
        Random random = new Random(42);
        List<Object> leading = values(random, ImmutableList.of(), () -> Slices.utf8Slice("https://www.example.com/" + randomString(random)));
        List<Object> secondary = values(random, ImmutableList.of(), random::nextLong);

        assertSortCorrect(
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                ImmutableList.of(SortOrder.ASC_NULLS_FIRST, SortOrder.DESC_NULLS_LAST),
                buildPages(ImmutableList.of(VARCHAR, BIGINT), ImmutableList.of(leading, secondary)));
    }

    @Test
    public void testRangeBelowSortKeyPrefixThreshold()
    {
        Random random = new Random(42);
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            values.add(randomWithTies(random, random::nextLong));
        }
        values.add(null);
        for (SortOrder sortOrder : SortOrder.values()) {
            assertSortCorrect(
                    ImmutableList.of(BIGINT),
                    ImmutableList.of(0),
                    ImmutableList.of(sortOrder),
                    buildPages(ImmutableList.of(BIGINT), ImmutableList.of(values)));
        }
    }

    @Test
    public void testBoolean()
    {
        Random random = new Random(42);
        assertSortsCorrectly(BOOLEAN, values(random, ImmutableList.of(true, false), random::nextBoolean));
    }

    @Test
    public void testChar()
    {
        // char comparison pads the shorter value with spaces, and so must the sort key prefix
        Random random = new Random(42);
        assertSortsCorrectly(createCharType(12), values(
                random,
                ImmutableList.of(
                        Slices.utf8Slice(""),
                        Slices.utf8Slice("a"),
                        Slices.utf8Slice("ab"),
                        Slices.utf8Slice("ab\u0001"),
                        Slices.utf8Slice("ab!"),
                        Slices.utf8Slice("abz"),
                        Slices.utf8Slice("abcdefghijkl")),
                () -> Slices.utf8Slice(randomString(random).stripTrailing())));
    }

    @Test
    public void testShortDecimal()
    {
        Random random = new Random(42);
        assertSortsCorrectly(createDecimalType(15, 2), values(
                random,
                ImmutableList.of(-1L, 0L, 1L, 999_999_999_999_999L, -999_999_999_999_999L),
                () -> randomWithTies(random, random::nextLong)));
    }

    @Test
    public void testLongDecimal()
    {
        Random random = new Random(42);
        assertSortsCorrectly(createDecimalType(38, 2), values(
                random,
                ImmutableList.of(
                        Int128.valueOf(0, 0),
                        Int128.valueOf(0, 1),
                        Int128.valueOf(0, -1),
                        Int128.valueOf(-1, -1),
                        Int128.valueOf(1, 0),
                        Int128.valueOf(-1, 0)),
                () -> Int128.valueOf(random.nextLong() % 1000, random.nextLong())));
    }

    @Test
    public void testUuid()
    {
        Random random = new Random(42);
        assertSortsCorrectly(UUID, values(
                random,
                ImmutableList.of(),
                () -> {
                    byte[] bytes = new byte[16];
                    random.nextBytes(bytes);
                    return Slices.wrappedBuffer(bytes);
                }));
    }

    @Test
    public void testShortTimestampWithTimeZone()
    {
        Random random = new Random(42);
        assertSortsCorrectly(TIMESTAMP_TZ_MILLIS, values(
                random,
                ImmutableList.of(
                        packDateTimeWithZone(0, UTC_KEY),
                        // same instant in a different zone must tie with the UTC one
                        packDateTimeWithZone(0, getTimeZoneKey("Europe/Warsaw")),
                        packDateTimeWithZone(-1, UTC_KEY),
                        packDateTimeWithZone(1, UTC_KEY)),
                () -> packDateTimeWithZone(random.nextLong() % 1_000_000_000_000L, UTC_KEY)));
    }

    @Test
    public void testLongTimestamp()
    {
        Random random = new Random(42);
        assertSortsCorrectly(TIMESTAMP_NANOS, values(
                random,
                ImmutableList.of(
                        new LongTimestamp(0, 0),
                        new LongTimestamp(0, 999_000),
                        new LongTimestamp(-1, 999_000),
                        new LongTimestamp(1, 0)),
                () -> new LongTimestamp(random.nextLong() % 1_000_000_000_000L, random.nextInt(1_000) * 1_000)));
    }

    @Test
    public void testUnsupportedTypeUsesComparatorPath()
    {
        Random random = new Random(42);
        List<Object> values = values(random, ImmutableList.of(), () -> {
            byte[] bytes = new byte[16];
            random.nextBytes(bytes);
            return Slices.wrappedBuffer(bytes);
        });
        for (SortOrder sortOrder : SortOrder.values()) {
            assertSortCorrect(
                    ImmutableList.of(IPADDRESS),
                    ImmutableList.of(0),
                    ImmutableList.of(sortOrder),
                    buildPages(ImmutableList.of(IPADDRESS), ImmutableList.of(values)));
        }
    }

    private static void assertSortsCorrectly(Type type, List<Object> values)
    {
        for (SortOrder sortOrder : SortOrder.values()) {
            assertSortCorrect(
                    ImmutableList.of(type),
                    ImmutableList.of(0),
                    ImmutableList.of(sortOrder),
                    buildPages(ImmutableList.of(type), ImmutableList.of(values)));
        }
    }

    private static void assertSortCorrect(List<Type> types, List<Integer> sortChannels, List<SortOrder> sortOrders, List<Page> pages)
    {
        PagesIndex pagesIndex = new PagesIndex.TestingFactory(false).newPagesIndex(types, POSITION_COUNT);
        pages.forEach(pagesIndex::addPage);

        long[] addressesBefore = pagesIndex.getValueAddresses().toLongArray();
        Arrays.sort(addressesBefore);

        pagesIndex.sort(sortChannels, sortOrders, newSimpleAggregatedMemoryContext().newLocalMemoryContext("test"));

        long[] addressesAfter = pagesIndex.getValueAddresses().toLongArray();
        Arrays.sort(addressesAfter);
        assertThat(addressesAfter)
                .as("sort must permute the positions, not add or drop any")
                .isEqualTo(addressesBefore);

        List<Type> sortTypes = sortChannels.stream()
                .map(types::get)
                .collect(toImmutableList());
        PagesIndexComparator comparator = new SimplePagesIndexComparator(sortTypes, sortChannels, sortOrders, TYPE_OPERATORS);
        for (int position = 1; position < pagesIndex.getPositionCount(); position++) {
            assertThat(comparator.compareTo(pagesIndex, position - 1, position))
                    .as("positions %s and %s are out of order for %s %s", position - 1, position, sortTypes, sortOrders)
                    .isLessThanOrEqualTo(0);
        }
    }

    private static List<Object> values(Random random, List<Object> edgeValues, Supplier<Object> randomValue)
    {
        List<Object> values = new ArrayList<>();
        // include each edge value twice to create ties
        values.addAll(edgeValues);
        values.addAll(edgeValues);
        while (values.size() < POSITION_COUNT) {
            if (random.nextInt(10) == 0) {
                values.add(null);
            }
            else {
                values.add(randomValue.get());
            }
        }
        Collections.shuffle(values, random);
        return values;
    }

    private static Object randomWithTies(Random random, Supplier<Object> randomValue)
    {
        return randomWithTies(random, () -> (long) random.nextInt(10), randomValue);
    }

    private static Object randomWithTies(Random random, Supplier<Object> tieValue, Supplier<Object> randomValue)
    {
        // mix a small domain into the random values to exercise duplicate handling
        if (random.nextInt(4) == 0) {
            return tieValue.get();
        }
        return randomValue.get();
    }

    private static long realValue(float value)
    {
        return floatToRawIntBits(value);
    }

    private static String randomString(Random random)
    {
        int length = random.nextInt(16);
        StringBuilder value = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            value.append((char) ('a' + random.nextInt(26)));
        }
        return value.toString();
    }

    private static List<Page> buildPages(List<Type> types, List<List<Object>> columns)
    {
        int positionCount = columns.get(0).size();
        List<Page> pages = new ArrayList<>();
        PageBuilder pageBuilder = new PageBuilder(types);
        for (int position = 0; position < positionCount; position++) {
            pageBuilder.declarePosition();
            for (int channel = 0; channel < types.size(); channel++) {
                writeNativeValue(types.get(channel), pageBuilder.getBlockBuilder(channel), columns.get(channel).get(position));
            }
            if (pageBuilder.isFull() || (position > 0 && position % 977 == 0)) {
                pages.add(pageBuilder.build());
                pageBuilder.reset();
            }
        }
        if (!pageBuilder.isEmpty()) {
            pages.add(pageBuilder.build());
        }
        return pages;
    }
}
