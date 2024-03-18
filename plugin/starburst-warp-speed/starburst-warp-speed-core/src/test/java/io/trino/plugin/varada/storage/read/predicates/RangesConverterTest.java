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
package io.trino.plugin.varada.storage.read.predicates;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.varada.util.SliceUtils;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.varada.tools.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.BYTE_LOWER_UNBOUNDED;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.BYTE_UPPER_UNBOUNDED;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.DOUBLE_LOWER_UNBOUNDED;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.DOUBLE_UPPER_UNBOUNDED;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.EXCLUSIVE;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.FLOAT_LOWER_UNBOUNDED;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.FLOAT_UPPER_UNBOUNDED;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.INCLUSIVE;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.INT_LOWER_UNBOUNDED;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.INT_UPPER_UNBOUNDED;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.LONG_DECIMAL_LOWER_UNBOUNDED_LSB;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.LONG_DECIMAL_LOWER_UNBOUNDED_MSB;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.LONG_DECIMAL_UPPER_UNBOUNDED_LSB;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.LONG_DECIMAL_UPPER_UNBOUNDED_MSB;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.LONG_LOWER_UNBOUNDED;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.LONG_UPPER_UNBOUNDED;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.SHORT_LOWER_UNBOUNDED;
import static io.trino.plugin.varada.storage.read.predicates.RangesConverter.SHORT_UPPER_UNBOUNDED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class RangesConverterTest
{
    static RangesConverter rangesConverter;
    ByteBuffer low;
    ByteBuffer high;

    @BeforeAll
    public static void beforeAll()
    {
        rangesConverter = new RangesConverter();
    }

    @BeforeEach
    public void initBuffers()
    {
        low = ByteBuffer.allocate(100);
        high = ByteBuffer.allocate(100);
    }

    @AfterEach
    public void afterEach()
    {
        low = null;
        high = null;
    }

    @Test
    public void testLongUnboundedLow()
    {
        Range rangeLess = Range.lessThan(BIGINT, 0L);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, List.of(rangeLess));

        rangesConverter.setLongRanges(low, high, sortedRangeSet);

        List<Pair<Long, Byte>> expectedLowResults = List.of(Pair.of(LONG_LOWER_UNBOUNDED, INCLUSIVE));
        List<Pair<Long, Byte>> expectedHighResults = List.of(Pair.of(0L, EXCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testLongUnboundedLowLessThanOrEqual()
    {
        Range rangeLess = Range.lessThanOrEqual(BIGINT, 0L);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, List.of(rangeLess));
        rangesConverter.setLongRanges(low, high, sortedRangeSet);

        List<Pair<Long, Byte>> expectedLowResults = List.of(Pair.of(LONG_LOWER_UNBOUNDED, INCLUSIVE));
        List<Pair<Long, Byte>> expectedHighResults = List.of(Pair.of(0L, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testLongUnboundedHigh()
    {
        Range greaterThan = Range.greaterThan(BIGINT, 1L);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, List.of(greaterThan));

        rangesConverter.setLongRanges(low, high, sortedRangeSet);

        List<Pair<Long, Byte>> expectedLowResults = List.of(Pair.of(1L, EXCLUSIVE));
        List<Pair<Long, Byte>> expectedHighResults = List.of(Pair.of(LONG_UPPER_UNBOUNDED, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testLongUnboundedGreaterThanOrEqual()
    {
        Range greaterThanOrEqual = Range.greaterThanOrEqual(BIGINT, 1L);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, List.of(greaterThanOrEqual));

        rangesConverter.setLongRanges(low, high, sortedRangeSet);

        List<Pair<Long, Byte>> expectedLowResults = List.of(Pair.of(1L, INCLUSIVE));
        List<Pair<Long, Byte>> expectedHighResults = List.of(Pair.of(LONG_UPPER_UNBOUNDED, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testLongBoundedLowSingleValue()
    {
        Range range = Range.range(BIGINT, 1L, true, 1L, true);
        assertThat(range.isSingleValue()).isTrue();
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, List.of(range));
        rangesConverter.setLongRanges(low, high, sortedRangeSet);

        List<Pair<Long, Byte>> expectedLowResults = List.of(Pair.of(1L, INCLUSIVE));
        List<Pair<Long, Byte>> expectedHighResults = List.of(Pair.of(1L, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testLongBoundedLowSingleValueLowExclusive()
    {
        Range range = Range.range(BIGINT, 0L, false, 2L, true);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, List.of(range));
        rangesConverter.setLongRanges(low, high, sortedRangeSet);

        List<Pair<Long, Byte>> expectedLowResults = List.of(Pair.of(0L, EXCLUSIVE));
        List<Pair<Long, Byte>> expectedHighResults = List.of(Pair.of(2L, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testLongBoundedLowSingleValueLowExclusive2()
    {
        Range range1 = Range.range(BIGINT, 0L, false, 2L, true);
        Range range2 = Range.range(BIGINT, 5L, false, 9L, true);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, List.of(range1, range2));
        rangesConverter.setLongRanges(low, high, sortedRangeSet);

        List<Pair<Long, Byte>> expectedLowResults = List.of(Pair.of(0L, EXCLUSIVE), Pair.of(5L, EXCLUSIVE));
        List<Pair<Long, Byte>> expectedHighResults = List.of(Pair.of(2L, INCLUSIVE), Pair.of(9L, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testLongBoundedLowSingleValueLowInclusive()
    {
        Range range = Range.range(BIGINT, 0L, true, 2L, true);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, List.of(range));
        rangesConverter.setLongRanges(low, high, sortedRangeSet);

        List<Pair<Long, Byte>> expectedLowResults = List.of(Pair.of(0L, INCLUSIVE));
        List<Pair<Long, Byte>> expectedHighResults = List.of(Pair.of(2L, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testManyRanges()
    {
        Range lessRange = Range.lessThanOrEqual(BIGINT, -9L);
        Range middleRange1 = Range.range(BIGINT, 3L, true, 5L, true);
        Range middleRange2 = Range.range(BIGINT, 7L, false, 10L, false);
        Range middleRange3 = Range.range(BIGINT, 20L, true, 25L, false);
        Range middleRange4 = Range.range(BIGINT, 30L, false, 35L, true);
        Range greater = Range.greaterThanOrEqual(BIGINT, 90L);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(BIGINT, List.of(lessRange,
                middleRange1,
                middleRange2,
                middleRange3,
                middleRange4,
                greater));
        rangesConverter.setLongRanges(low, high, sortedRangeSet);

        List<Pair<Long, Byte>> expectedLowResults = List.of(Pair.of(LONG_LOWER_UNBOUNDED, INCLUSIVE),
                Pair.of(3L, INCLUSIVE),
                Pair.of(7L, EXCLUSIVE),
                Pair.of(20L, INCLUSIVE),
                Pair.of(30L, EXCLUSIVE),
                Pair.of(90L, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        List<Pair<Long, Byte>> expectedHighResults = List.of(Pair.of(-9L, INCLUSIVE),
                Pair.of(5L, INCLUSIVE),
                Pair.of(10L, EXCLUSIVE),
                Pair.of(25L, EXCLUSIVE),
                Pair.of(35L, INCLUSIVE),
                Pair.of(LONG_UPPER_UNBOUNDED, INCLUSIVE));
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testManyRangesTinyInt()
    {
        Range lessRange = Range.lessThanOrEqual(TINYINT, -9L);
        Range middleRange1 = Range.range(TINYINT, 3L, true, 5L, true);
        Range middleRange2 = Range.range(TINYINT, 7L, false, 10L, false);
        Range middleRange3 = Range.range(TINYINT, 20L, true, 25L, false);
        Range middleRange4 = Range.range(TINYINT, 30L, false, 35L, true);
        Range greater = Range.greaterThanOrEqual(TINYINT, 90L);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(TINYINT, List.of(lessRange,
                middleRange1,
                middleRange2,
                middleRange3,
                middleRange4,
                greater));
        rangesConverter.setTinyintRanges(low, high, sortedRangeSet);

        var expectedLowResults =
                List.of(Pair.of(BYTE_LOWER_UNBOUNDED, INCLUSIVE),
                        Pair.of((byte) 3, INCLUSIVE),
                        Pair.of((byte) 7, EXCLUSIVE),
                        Pair.of((byte) 20, INCLUSIVE),
                        Pair.of((byte) 30, EXCLUSIVE),
                        Pair.of((byte) 90, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        List<? extends Pair<? extends Number, Byte>> expectedHighResults = List.of(Pair.of((byte) -9, INCLUSIVE),
                Pair.of((byte) 5, INCLUSIVE),
                Pair.of((byte) 10, EXCLUSIVE),
                Pair.of((byte) 25, EXCLUSIVE),
                Pair.of((byte) 35, INCLUSIVE),
                Pair.of(BYTE_UPPER_UNBOUNDED, INCLUSIVE));
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testManyRangesSmallInt()
    {
        Range lessRange = Range.lessThanOrEqual(SMALLINT, -9L);
        Range middleRange1 = Range.range(SMALLINT, 3L, true, 5L, true);
        Range middleRange2 = Range.range(SMALLINT, 7L, false, 10L, false);
        Range middleRange3 = Range.range(SMALLINT, 20L, true, 25L, false);
        Range middleRange4 = Range.range(SMALLINT, 30L, false, 35L, true);
        Range greater = Range.greaterThanOrEqual(SMALLINT, 90L);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(SMALLINT, List.of(lessRange,
                middleRange1,
                middleRange2,
                middleRange3,
                middleRange4,
                greater));
        rangesConverter.setSmallIntRanges(low, high, sortedRangeSet);

        var expectedLowResults =
                List.of(Pair.of(SHORT_LOWER_UNBOUNDED, INCLUSIVE),
                        Pair.of((short) 3, INCLUSIVE),
                        Pair.of((short) 7, EXCLUSIVE),
                        Pair.of((short) 20, INCLUSIVE),
                        Pair.of((short) 30, EXCLUSIVE),
                        Pair.of((short) 90, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        List<? extends Pair<? extends Number, Byte>> expectedHighResults = List.of(Pair.of((short) -9, INCLUSIVE),
                Pair.of((short) 5, INCLUSIVE),
                Pair.of((short) 10, EXCLUSIVE),
                Pair.of((short) 25, EXCLUSIVE),
                Pair.of((short) 35, INCLUSIVE),
                Pair.of(SHORT_UPPER_UNBOUNDED, INCLUSIVE));
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testManyRangesDouble()
    {
        Range lessRange = Range.lessThanOrEqual(DoubleType.DOUBLE, -9D);
        Range middleRange1 = Range.range(DoubleType.DOUBLE, 3D, true, 5D, true);
        Range middleRange2 = Range.range(DoubleType.DOUBLE, 7D, false, 10D, false);
        Range middleRange3 = Range.range(DoubleType.DOUBLE, 20D, true, 25D, false);
        Range middleRange4 = Range.range(DoubleType.DOUBLE, 30D, false, 35D, true);
        Range greater = Range.greaterThanOrEqual(DoubleType.DOUBLE, 90D);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(DoubleType.DOUBLE, List.of(lessRange,
                middleRange1,
                middleRange2,
                middleRange3,
                middleRange4,
                greater));
        rangesConverter.setDoubleRanges(low, high, sortedRangeSet);

        var expectedLowResults =
                List.of(Pair.of(DOUBLE_LOWER_UNBOUNDED, INCLUSIVE),
                        Pair.of((double) 3, INCLUSIVE),
                        Pair.of((double) 7, EXCLUSIVE),
                        Pair.of((double) 20, INCLUSIVE),
                        Pair.of((double) 30, EXCLUSIVE),
                        Pair.of((double) 90, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        List<? extends Pair<? extends Number, Byte>> expectedHighResults = List.of(Pair.of((double) -9, INCLUSIVE),
                Pair.of((double) 5, INCLUSIVE),
                Pair.of((double) 10, EXCLUSIVE),
                Pair.of((double) 25, EXCLUSIVE),
                Pair.of((double) 35, INCLUSIVE),
                Pair.of(DOUBLE_UPPER_UNBOUNDED, INCLUSIVE));
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testManyRangesInteger()
    {
        Range lessRange = Range.lessThanOrEqual(IntegerType.INTEGER, -9L);
        Range middleRange1 = Range.range(IntegerType.INTEGER, 3L, true, 5L, true);
        Range middleRange2 = Range.range(IntegerType.INTEGER, 7L, false, 10L, false);
        Range middleRange3 = Range.range(IntegerType.INTEGER, 20L, true, 25L, false);
        Range middleRange4 = Range.range(IntegerType.INTEGER, 30L, false, 35L, true);
        Range greater = Range.greaterThanOrEqual(IntegerType.INTEGER, 90L);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(IntegerType.INTEGER, List.of(lessRange,
                middleRange1,
                middleRange2,
                middleRange3,
                middleRange4,
                greater));
        rangesConverter.setIntRanges(low, high, sortedRangeSet);

        var expectedLowResults =
                List.of(Pair.of(INT_LOWER_UNBOUNDED, INCLUSIVE),
                        Pair.of(3, INCLUSIVE),
                        Pair.of(7, EXCLUSIVE),
                        Pair.of(20, INCLUSIVE),
                        Pair.of(30, EXCLUSIVE),
                        Pair.of(90, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        List<? extends Pair<? extends Number, Byte>> expectedHighResults = List.of(Pair.of(-9, INCLUSIVE),
                Pair.of(5, INCLUSIVE),
                Pair.of(10, EXCLUSIVE),
                Pair.of(25, EXCLUSIVE),
                Pair.of(35, INCLUSIVE),
                Pair.of(INT_UPPER_UNBOUNDED, INCLUSIVE));
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testManyRangesLongDecimal()
    {
        low = ByteBuffer.allocate(1000);
        high = ByteBuffer.allocate(1000);
        DecimalType longDecimalType = DecimalType.createDecimalType(20, 0);

        Int128 lessRangeSlice = createLongDecimalSlice(50);
        Int128 lowMiddleRange = createLongDecimalSlice(51);
        Int128 highMiddleRange = createLongDecimalSlice(55);
        Int128 greaterSlice = createLongDecimalSlice(190);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(longDecimalType,
                List.of(Range.lessThanOrEqual(longDecimalType, lessRangeSlice),
                        Range.range(longDecimalType, lowMiddleRange, true, highMiddleRange, true),
                        Range.greaterThanOrEqual(longDecimalType, greaterSlice)));
        rangesConverter.setLongDecimalRanges(low, high, sortedRangeSet);

        assertThat(low.getLong(0)).isEqualTo(LONG_DECIMAL_LOWER_UNBOUNDED_MSB);
        assertThat(low.getLong(1)).isEqualTo(LONG_DECIMAL_LOWER_UNBOUNDED_LSB);
        assertThat(low.get(Long.BYTES * 2)).isEqualTo(INCLUSIVE);
        int lowPosition = Long.BYTES * 2 + 1;
        validateResults(low, lowPosition, lowMiddleRange);
        lowPosition += Int128.SIZE;
        assertThat(low.get(lowPosition)).isEqualTo(INCLUSIVE);
        lowPosition += 1;
        validateResults(low, lowPosition, greaterSlice);
        lowPosition += Int128.SIZE;
        assertThat(low.get(lowPosition)).isEqualTo(INCLUSIVE);

        int highPosition = 0;
        validateResults(high, highPosition, lessRangeSlice);
        highPosition += Int128.SIZE;
        assertThat(high.get(highPosition)).isEqualTo(INCLUSIVE);
        highPosition += 1;
        validateResults(high, highPosition, highMiddleRange);
        highPosition += Int128.SIZE;
        assertThat(high.get(highPosition)).isEqualTo(INCLUSIVE);
        highPosition += 1;
        assertThat(high.getLong(highPosition)).isEqualTo(LONG_DECIMAL_UPPER_UNBOUNDED_MSB);
        assertThat(high.getLong(highPosition + Long.BYTES)).isEqualTo(LONG_DECIMAL_UPPER_UNBOUNDED_LSB);
        assertThat(high.get(highPosition + Long.BYTES * 2)).isEqualTo(INCLUSIVE);
    }

    @Disabled
    @Test
    public void testManyRangesReal()
    {
        RangesConverter rangesConverter = new RangesConverter();
        Range lessRange = Range.lessThanOrEqual(RealType.REAL, 9L);
/*        Range middleRange1 = Range.range(RealType.REAL, 3L, true, 5L, true);
        Range middleRange2 = Range.range(RealType.REAL, 7L, false, 10L, false);
        Range middleRange3 = Range.range(RealType.REAL, 20L, true, 25L, false);
        Range middleRange4 = Range.range(RealType.REAL, 30L, false, 35L, true); */
        Range greater = Range.greaterThanOrEqual(RealType.REAL, 90L);
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(RealType.REAL, List.of(lessRange,
                                                                        /*                          middleRange1,
                                                                                                  middleRange2,
                                                                                                  middleRange3,
                                                                                                  middleRange4,*/
                greater));
        rangesConverter.setRealRanges(sortedRangeSet.getRanges(), low, high);

        var expectedLowResults =
                List.of(Pair.of(FLOAT_LOWER_UNBOUNDED, INCLUSIVE),
  /*                      Pair.of((double) 3, INCLUSIVE),
                        Pair.of((double) 7, EXCLUSIVE),
                        Pair.of((double) 20, INCLUSIVE),
                        Pair.of((double) 30, EXCLUSIVE),*/
                        Pair.of((double) 90, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        List<? extends Pair<? extends Number, Byte>> expectedHighResults = List.of(Pair.of((double) 9, INCLUSIVE),
                                                                      /*             Pair.of((double) 5, INCLUSIVE),
                                                                                   Pair.of((double) 10, EXCLUSIVE),
                                                                                   Pair.of((double) 25, EXCLUSIVE),
                                                                                   Pair.of((double) 35, INCLUSIVE),*/
                Pair.of(FLOAT_UPPER_UNBOUNDED, INCLUSIVE));
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
    }

    @Test
    public void testManyStringRanges()
    {
        RangesConverter rangesConverter = new RangesConverter();
        Range graterRange = Range.greaterThan(VarcharType.createVarcharType(10), Slices.utf8Slice("ABC"));
        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(VarcharType.createVarcharType(10), List.of(graterRange));
        Function<Slice, Slice> sliceConverter = SliceUtils.getSliceConverter(sortedRangeSet.getType(), 10, false, false);
        rangesConverter.setStringRanges(sortedRangeSet, low, high, 10, sliceConverter);
        List<? extends Pair<? extends Number, Byte>> expectedLowResults = List.of(Pair.of(SliceUtils.str2int(Slices.utf8Slice("ABC"), true), EXCLUSIVE));
        List<? extends Pair<? extends Number, Byte>> expectedHighResults = List.of(Pair.of(LONG_UPPER_UNBOUNDED, INCLUSIVE));
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
        validateResults(high, expectedHighResults, sortedRangeSet.getType());

        initBuffers();
        Range lessRange = Range.lessThan(VarcharType.createVarcharType(10), Slices.utf8Slice("ZYX"));
        sortedRangeSet = SortedRangeSet.copyOf(VarcharType.createVarcharType(10), List.of(lessRange));
        sliceConverter = SliceUtils.getSliceConverter(sortedRangeSet.getType(), 10, false, false);
        rangesConverter.setStringRanges(sortedRangeSet, low, high, 10, sliceConverter);
        expectedHighResults = List.of(Pair.of(SliceUtils.str2int(Slices.utf8Slice("ZYX"), true), EXCLUSIVE));
        expectedLowResults = List.of(Pair.of(LONG_LOWER_UNBOUNDED, INCLUSIVE));
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
        validateResults(low, expectedLowResults, sortedRangeSet.getType());

        initBuffers();
        Range middleIncExc = Range.range(VarcharType.createVarcharType(10), Slices.utf8Slice("GGG"), true, Slices.utf8Slice("HHH"), false);
        Range middleIncInc = Range.range(VarcharType.createVarcharType(10), Slices.utf8Slice("AAA"), true, Slices.utf8Slice("BBB"), true);
        Range middleExcExc = Range.range(VarcharType.createVarcharType(10), Slices.utf8Slice("III"), false, Slices.utf8Slice("JJJ"), false);
        Range middleExcInc = Range.range(VarcharType.createVarcharType(10), Slices.utf8Slice("CCC"), false, Slices.utf8Slice("DDD"), true);
        Range longSlice = Range.range(VarcharType.createVarcharType(10), Slices.utf8Slice("1234567890"), true, Slices.utf8Slice("1234567899"), true);
        sortedRangeSet = SortedRangeSet.copyOf(VarcharType.createVarcharType(10), List.of(middleIncExc, middleIncInc, middleExcExc, middleExcInc, longSlice));
        sliceConverter = SliceUtils.getSliceConverter(sortedRangeSet.getType(), 10, false, false);
        rangesConverter.setStringRanges(sortedRangeSet, low, high, 10, sliceConverter);
        expectedLowResults = List.of(Pair.of(SliceUtils.str2int(Slices.utf8Slice("12345678"), true), INCLUSIVE),
                Pair.of(SliceUtils.str2int(Slices.utf8Slice("AAA"), true), INCLUSIVE),
                Pair.of(SliceUtils.str2int(Slices.utf8Slice("CCC"), true), EXCLUSIVE),
                Pair.of(SliceUtils.str2int(Slices.utf8Slice("GGG"), true), INCLUSIVE),
                Pair.of(SliceUtils.str2int(Slices.utf8Slice("III"), true), EXCLUSIVE));
        expectedHighResults = List.of(Pair.of(SliceUtils.str2int(Slices.utf8Slice("12345678"), true), INCLUSIVE),
                Pair.of(SliceUtils.str2int(Slices.utf8Slice("BBB"), true), INCLUSIVE),
                Pair.of(SliceUtils.str2int(Slices.utf8Slice("DDD"), true), INCLUSIVE),
                Pair.of(SliceUtils.str2int(Slices.utf8Slice("HHH"), true), EXCLUSIVE),
                Pair.of(SliceUtils.str2int(Slices.utf8Slice("JJJ"), true), EXCLUSIVE));
        validateResults(high, expectedHighResults, sortedRangeSet.getType());
        validateResults(low, expectedLowResults, sortedRangeSet.getType());
    }

    private void validateResults(ByteBuffer byteBuffer, List<? extends Pair<? extends Number, Byte>> expectedResult, Type type)
    {
        if (type == BIGINT) {
            int pos = 0;
            for (var expected : expectedResult) {
                assertThat(byteBuffer.getLong(pos)).isEqualTo(expected.getKey());
                pos += Long.BYTES;
                assertThat(byteBuffer.get(pos)).isEqualTo(expected.getValue());
                pos += 1;
            }
            return;
        }
        else if (type == TINYINT) {
            int pos = 0;
            for (var expected : expectedResult) {
                assertThat(byteBuffer.get(pos)).isEqualTo(expected.getKey());
                pos += Byte.BYTES;
                assertThat(byteBuffer.get(pos)).isEqualTo(expected.getValue());
                pos += 1;
            }
            return;
        }
        else if (type == SMALLINT) {
            int pos = 0;
            for (var expected : expectedResult) {
                assertThat(byteBuffer.getShort(pos)).isEqualTo(expected.getKey());
                pos += Short.BYTES;
                assertThat(byteBuffer.get(pos)).isEqualTo(expected.getValue());
                pos += 1;
            }
            return;
        }
        else if (type == DoubleType.DOUBLE) {
            int pos = 0;
            for (var expected : expectedResult) {
                assertThat(byteBuffer.getDouble(pos)).isEqualTo(expected.getKey());
                pos += Double.BYTES;
                assertThat(byteBuffer.get(pos)).isEqualTo(expected.getValue());
                pos += 1;
            }
            return;
        }
        else if (type == RealType.REAL) {
            int pos = 0;
            for (var expected : expectedResult) {
                assertThat(byteBuffer.getLong(pos)).isEqualTo(expected.getKey());
                pos += Float.BYTES;
                assertThat(byteBuffer.get(pos)).isEqualTo(expected.getValue());
                pos += 1;
            }
            return;
        }
        else if (type == IntegerType.INTEGER) {
            int pos = 0;
            for (var expected : expectedResult) {
                assertThat(byteBuffer.getInt(pos)).isEqualTo(expected.getKey());
                pos += Integer.BYTES;
                assertThat(byteBuffer.get(pos)).isEqualTo(expected.getValue());
                pos += 1;
            }
            return;
        }
        else if (type instanceof VarcharType) {
            int pos = 0;
            for (var expected : expectedResult) {
                assertThat(byteBuffer.getLong(pos)).isEqualTo(expected.getKey());
                pos += Long.BYTES;
                assertThat(byteBuffer.get(pos)).isEqualTo(expected.getValue());
                pos += 1;
            }
            return;
        }
        fail("not supported type");
    }

    private Int128 createLongDecimalSlice(int val)
    {
        return Decimals.encodeScaledValue(new BigDecimal(val), 0);
    }

    private void validateResults(ByteBuffer buffer, int startPosition, Int128 int128)
    {
        long[] actual = new long[2];

        actual[0] = buffer.getLong(startPosition);
        actual[1] = buffer.getLong(startPosition + Long.BYTES);
        assertThat(Int128.valueOf(actual[0], actual[1])).isEqualTo(int128);
    }
}
