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
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLongTimestampWithTimeZoneType
        extends AbstractTestType
{
    public TestLongTimestampWithTimeZoneType()
    {
        super(TIMESTAMP_TZ_MICROS, SqlTimestampWithTimeZone.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP_TZ_MICROS.createFixedSizeBlockBuilder(15);
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1111, 0, getTimeZoneKeyForOffset(0)));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1111, 0, getTimeZoneKeyForOffset(1)));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(1111, 0, getTimeZoneKeyForOffset(2)));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(2222, 0, getTimeZoneKeyForOffset(3)));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(2222, 0, getTimeZoneKeyForOffset(4)));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(2222, 0, getTimeZoneKeyForOffset(5)));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(2222, 0, getTimeZoneKeyForOffset(6)));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(2222, 0, getTimeZoneKeyForOffset(7)));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(3333, 0, getTimeZoneKeyForOffset(8)));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(3333, 0, getTimeZoneKeyForOffset(9)));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(4444, 0, getTimeZoneKeyForOffset(10)));
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        // time zone doesn't matter for ordering
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(((LongTimestampWithTimeZone) value).getEpochMillis() + 1, 0, getTimeZoneKeyForOffset(33));
    }

    @Test
    public void testPreviousValue()
    {
        LongTimestampWithTimeZone minValue = LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY);
        LongTimestampWithTimeZone nextToMinValue = LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 1_000_000, UTC_KEY);
        LongTimestampWithTimeZone previousToMaxValue = LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, 998_000_000, UTC_KEY);
        LongTimestampWithTimeZone maxValue = LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, 999_000_000, UTC_KEY);

        assertThat(type.getPreviousValue(minValue))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(nextToMinValue))
                .isEqualTo(Optional.of(minValue));

        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1110, 999_000_000, getTimeZoneKeyForOffset(0))));
        assertThat(type.getPreviousValue(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1483228800000L, 000_000_000, getTimeZoneKeyForOffset(0))))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1483228799999L, 999_000_000, getTimeZoneKeyForOffset(0))));

        assertThat(type.getPreviousValue(previousToMaxValue))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, 997_000_000, UTC_KEY)));
        assertThat(type.getPreviousValue(maxValue))
                .isEqualTo(Optional.of(previousToMaxValue));
    }

    @Test
    public void testNextValue()
    {
        LongTimestampWithTimeZone minValue = LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY);
        LongTimestampWithTimeZone nextToMinValue = LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 1_000_000, UTC_KEY);
        LongTimestampWithTimeZone previousToMaxValue = LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, 998_000_000, UTC_KEY);
        LongTimestampWithTimeZone maxValue = LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, 999_000_000, UTC_KEY);

        assertThat(type.getNextValue(minValue))
                .isEqualTo(Optional.of(nextToMinValue));
        assertThat(type.getNextValue(nextToMinValue))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 2_000_000, UTC_KEY)));

        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1111, 1_000_000, getTimeZoneKeyForOffset(0))));
        assertThat(type.getNextValue(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1483228799999L, 999_000_000, getTimeZoneKeyForOffset(0))))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1483228800000L, 000_000_000, getTimeZoneKeyForOffset(0))));

        assertThat(type.getNextValue(previousToMaxValue))
                .isEqualTo(Optional.of(maxValue));
        assertThat(type.getNextValue(maxValue))
                .isEqualTo(Optional.empty());
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueEveryPrecisionDataProvider")
    public void testPreviousValueEveryPrecision(int precision, int step)
    {
        Type type = createTimestampWithTimeZoneType(precision);

        // there is no value before the minimum
        assertThat(type.getPreviousValue(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY)))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, step, UTC_KEY)))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY)));

        // stepping down stays within the same millisecond (time zone doesn't matter for ordering)
        assertThat(type.getPreviousValue(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1111, step * 5, getTimeZoneKeyForOffset(2))))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1111, step * 4, UTC_KEY)));
        // stepping down crosses a millisecond boundary
        assertThat(type.getPreviousValue(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1111, 0, getTimeZoneKeyForOffset(2))))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1110, PICOSECONDS_PER_MILLISECOND - step, UTC_KEY)));

        // near the maximum
        assertThat(type.getPreviousValue(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOSECONDS_PER_MILLISECOND - step, UTC_KEY)))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOSECONDS_PER_MILLISECOND - 2 * step, UTC_KEY)));
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueEveryPrecisionDataProvider")
    public void testNextValueEveryPrecision(int precision, int step)
    {
        Type type = createTimestampWithTimeZoneType(precision);

        // there is no value after the maximum
        assertThat(type.getNextValue(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOSECONDS_PER_MILLISECOND - step, UTC_KEY)))
                .isEqualTo(Optional.empty());
        assertThat(type.getNextValue(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOSECONDS_PER_MILLISECOND - 2 * step, UTC_KEY)))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOSECONDS_PER_MILLISECOND - step, UTC_KEY)));

        // stepping up stays within the same millisecond (time zone doesn't matter for ordering)
        assertThat(type.getNextValue(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1111, step * 4, getTimeZoneKeyForOffset(2))))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1111, step * 5, UTC_KEY)));
        // stepping up crosses a millisecond boundary
        assertThat(type.getNextValue(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1110, PICOSECONDS_PER_MILLISECOND - step, getTimeZoneKeyForOffset(2))))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(1111, 0, UTC_KEY)));
    }

    public static Stream<Arguments> testPreviousNextValueEveryPrecisionDataProvider()
    {
        // LongTimestampWithTimeZoneType covers precisions 4..12; the step is the number of picoseconds per unit at that precision
        return Stream.of(
                Arguments.of(4, 100_000_000),
                Arguments.of(5, 10_000_000),
                Arguments.of(6, 1_000_000),
                Arguments.of(7, 100_000),
                Arguments.of(8, 10_000),
                Arguments.of(9, 1_000),
                Arguments.of(10, 100),
                Arguments.of(11, 10),
                Arguments.of(12, 1));
    }

    @Test
    public void testRange()
    {
        assertThat(type.getRange())
                .isEmpty();
    }
}
