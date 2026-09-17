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
package io.trino.type;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MINUTE;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MINUTE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimeWithTimeZoneType
        extends AbstractTestType
{
    public TestTimeWithTimeZoneType()
    {
        super(TIME_TZ_MILLIS, SqlTimeWithTimeZone.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = TIME_TZ_MILLIS.createFixedSizeBlockBuilder(15);
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(1_111_000_000L, 0));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(1_111_000_000L, 1));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(1_111_000_000L, 2));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(2_222_000_000L, 3));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(2_222_000_000L, 4));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(2_222_000_000L, 5));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(2_222_000_000L, 6));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(2_222_000_000L, 7));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(3_333_000_000L, 8));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(3_333_000_000L, 9));
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(4_444_000_000L, 10));
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return packTimeWithTimeZone(unpackTimeNanos((Long) value) + 10, unpackOffsetMinutes((Long) value));
    }

    @Test
    public void testRange()
    {
        assertThat(type.getRange())
                .isEmpty();
    }

    @Test
    public void testPreviousValue()
    {
        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(packTimeWithTimeZone(1_110_000_000L, 0)));

        // there is no value before midnight at the +00:00 offset
        assertThat(type.getPreviousValue(packTimeWithTimeZone(0, 0)))
                .isEqualTo(Optional.empty());
        // midnight at a positive offset is a time late in the day once normalized, so it does have a previous value
        assertThat(type.getPreviousValue(packTimeWithTimeZone(0, 60)))
                .isEqualTo(Optional.of(packTimeWithTimeZone(NANOSECONDS_PER_DAY - 60 * NANOSECONDS_PER_MINUTE - 1_000_000L, 0)));
    }

    @Test
    public void testNextValue()
    {
        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(packTimeWithTimeZone(1_112_000_000L, 0)));

        // there is no value after the last millisecond of the day at the +00:00 offset
        assertThat(type.getNextValue(packTimeWithTimeZone(NANOSECONDS_PER_DAY - 1_000_000L, 0)))
                .isEqualTo(Optional.empty());
        // the same time at a positive offset is normalized to an earlier time of the day, so it does have a next value
        assertThat(type.getNextValue(packTimeWithTimeZone(NANOSECONDS_PER_DAY - 1_000_000L, 60)))
                .isEqualTo(Optional.of(packTimeWithTimeZone(NANOSECONDS_PER_DAY - 60 * NANOSECONDS_PER_MINUTE, 0)));
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueShortPrecisionDataProvider")
    public void testPreviousValueShortPrecision(int precision, long step)
    {
        Type type = createTimeWithTimeZoneType(precision);

        // there is no value before the minimum
        assertThat(type.getPreviousValue(packTimeWithTimeZone(0, 0)))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(packTimeWithTimeZone(step, 0)))
                .isEqualTo(Optional.of(packTimeWithTimeZone(0, 0)));

        // the offset doesn't matter for ordering
        assertThat(type.getPreviousValue(packTimeWithTimeZone(5 * step + NANOSECONDS_PER_MINUTE, 1)))
                .isEqualTo(Optional.of(packTimeWithTimeZone(4 * step, 0)));
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueShortPrecisionDataProvider")
    public void testNextValueShortPrecision(int precision, long step)
    {
        Type type = createTimeWithTimeZoneType(precision);

        // there is no value after the maximum
        assertThat(type.getNextValue(packTimeWithTimeZone(NANOSECONDS_PER_DAY - step, 0)))
                .isEqualTo(Optional.empty());
        assertThat(type.getNextValue(packTimeWithTimeZone(NANOSECONDS_PER_DAY - 2 * step, 0)))
                .isEqualTo(Optional.of(packTimeWithTimeZone(NANOSECONDS_PER_DAY - step, 0)));

        // the offset doesn't matter for ordering
        assertThat(type.getNextValue(packTimeWithTimeZone(4 * step + NANOSECONDS_PER_MINUTE, 1)))
                .isEqualTo(Optional.of(packTimeWithTimeZone(5 * step, 0)));
    }

    public static Stream<Arguments> testPreviousNextValueShortPrecisionDataProvider()
    {
        // ShortTimeWithTimeZoneType covers precisions 0..9; the step is the number of nanoseconds per unit at that precision
        return Stream.of(
                Arguments.of(0, 1_000_000_000L),
                Arguments.of(1, 100_000_000L),
                Arguments.of(2, 10_000_000L),
                Arguments.of(3, 1_000_000L),
                Arguments.of(4, 100_000L),
                Arguments.of(5, 10_000L),
                Arguments.of(6, 1_000L),
                Arguments.of(7, 100L),
                Arguments.of(8, 10L),
                Arguments.of(9, 1L));
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueLongPrecisionDataProvider")
    public void testPreviousValueLongPrecision(int precision, long step)
    {
        Type type = createTimeWithTimeZoneType(precision);

        // there is no value before the minimum
        assertThat(type.getPreviousValue(new LongTimeWithTimeZone(0, 0)))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(new LongTimeWithTimeZone(step, 0)))
                .isEqualTo(Optional.of(new LongTimeWithTimeZone(0, 0)));

        // the offset doesn't matter for ordering
        assertThat(type.getPreviousValue(new LongTimeWithTimeZone(5 * step + PICOSECONDS_PER_MINUTE, 1)))
                .isEqualTo(Optional.of(new LongTimeWithTimeZone(4 * step, 0)));
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueLongPrecisionDataProvider")
    public void testNextValueLongPrecision(int precision, long step)
    {
        Type type = createTimeWithTimeZoneType(precision);

        // there is no value after the maximum
        assertThat(type.getNextValue(new LongTimeWithTimeZone(PICOSECONDS_PER_DAY - step, 0)))
                .isEqualTo(Optional.empty());
        assertThat(type.getNextValue(new LongTimeWithTimeZone(PICOSECONDS_PER_DAY - 2 * step, 0)))
                .isEqualTo(Optional.of(new LongTimeWithTimeZone(PICOSECONDS_PER_DAY - step, 0)));

        // the offset doesn't matter for ordering
        assertThat(type.getNextValue(new LongTimeWithTimeZone(4 * step + PICOSECONDS_PER_MINUTE, 1)))
                .isEqualTo(Optional.of(new LongTimeWithTimeZone(5 * step, 0)));
    }

    public static Stream<Arguments> testPreviousNextValueLongPrecisionDataProvider()
    {
        // LongTimeWithTimeZoneType covers precisions 10..12; the step is the number of picoseconds per unit at that precision
        return Stream.of(
                Arguments.of(10, 100L),
                Arguments.of(11, 10L),
                Arguments.of(12, 1L));
    }
}
