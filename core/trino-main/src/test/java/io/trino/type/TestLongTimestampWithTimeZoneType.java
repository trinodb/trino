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

    private static final int PICOS_OF_MILLI = 1_000_000_000;

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
    public void testPreviousValueEveryPrecision(int precision, LongTimestampWithTimeZone minValue, LongTimestampWithTimeZone maxValue, int step)
    {
        long middleRangeEpochMillis = 123_456_789_000_000L;
        Type type = createTimestampWithTimeZoneType(precision);

        LongTimestampWithTimeZone zeroValueType = LongTimestampWithTimeZone.fromEpochMillisAndFraction(0, 0, UTC_KEY);
        LongTimestampWithTimeZone sequenceValueType = LongTimestampWithTimeZone.fromEpochMillisAndFraction(middleRangeEpochMillis, 0, UTC_KEY);
        LongTimestampWithTimeZone nextToMinValueType = LongTimestampWithTimeZone.fromEpochMillisAndFraction(minValue.getEpochMillis(), step, UTC_KEY);
        LongTimestampWithTimeZone previousToMaxValueType = LongTimestampWithTimeZone.fromEpochMillisAndFraction(maxValue.getEpochMillis(), PICOS_OF_MILLI - (2 * step), UTC_KEY);

        assertThat(type.getPreviousValue(minValue))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(nextToMinValueType))
                .isEqualTo(Optional.of(minValue));

        assertThat(type.getPreviousValue(zeroValueType))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(-1, PICOS_OF_MILLI - step, UTC_KEY)));
        assertThat(type.getPreviousValue(sequenceValueType))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(middleRangeEpochMillis - 1, PICOS_OF_MILLI - step, UTC_KEY)));

        assertThat(type.getPreviousValue(maxValue))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(maxValue.getEpochMillis(), PICOS_OF_MILLI - (2 * step), UTC_KEY)));
        assertThat(type.getPreviousValue(previousToMaxValueType))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(maxValue.getEpochMillis(), PICOS_OF_MILLI - (3 * step), UTC_KEY)));
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueEveryPrecisionDataProvider")
    public void testNextValueEveryPrecision(int precision, LongTimestampWithTimeZone minValue, LongTimestampWithTimeZone maxValue, int step)
    {
        long middleRangeEpochMillis = 123_456_789_000_000L;

        Type type = createTimestampWithTimeZoneType(precision);
        LongTimestampWithTimeZone zeroValueType = LongTimestampWithTimeZone.fromEpochMillisAndFraction(0, 0, UTC_KEY);
        LongTimestampWithTimeZone sequenceValueType = LongTimestampWithTimeZone.fromEpochMillisAndFraction(middleRangeEpochMillis, 0, UTC_KEY);
        LongTimestampWithTimeZone nextToMinValueType = LongTimestampWithTimeZone.fromEpochMillisAndFraction(minValue.getEpochMillis(), step, UTC_KEY);
        LongTimestampWithTimeZone previousToMaxValueType = LongTimestampWithTimeZone.fromEpochMillisAndFraction(maxValue.getEpochMillis(), PICOS_OF_MILLI - (2 * step), UTC_KEY);

        assertThat(type.getNextValue(minValue))
                .isEqualTo(Optional.of(nextToMinValueType));
        assertThat(type.getNextValue(nextToMinValueType))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(minValue.getEpochMillis(), 2 * step, UTC_KEY)));

        assertThat(type.getNextValue(zeroValueType))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(0, step, UTC_KEY)));
        assertThat(type.getNextValue(sequenceValueType))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(middleRangeEpochMillis, step, UTC_KEY)));

        assertThat(type.getNextValue(previousToMaxValueType))
                .isEqualTo(Optional.of(maxValue));
        assertThat(type.getNextValue(maxValue))
                .isEqualTo(Optional.empty());
    }

    public static Stream<Arguments> testPreviousNextValueEveryPrecisionDataProvider()
    {
        return Stream.of(
                Arguments.of(
                        4,
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOS_OF_MILLI - 100_000_000, UTC_KEY),
                        100_000_000),
                Arguments.of(
                        5,
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOS_OF_MILLI - 10_000_000, UTC_KEY),
                        10_000_000),
                Arguments.of(
                        6,
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOS_OF_MILLI - 1_000_000, UTC_KEY),
                        1_000_000),
                Arguments.of(
                        7,
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOS_OF_MILLI - 100_000, UTC_KEY),
                        100_000),
                Arguments.of(
                        8,
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOS_OF_MILLI - 10_000, UTC_KEY),
                        10_000),
                Arguments.of(
                        9,
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOS_OF_MILLI - 1_000, UTC_KEY),
                        1_000),
                Arguments.of(
                        10,
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOS_OF_MILLI - 100, UTC_KEY),
                        100),
                Arguments.of(
                        11,
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOS_OF_MILLI - 10, UTC_KEY),
                        10),
                Arguments.of(
                        12,
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MIN_VALUE, 0, UTC_KEY),
                        LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, PICOS_OF_MILLI - 1, UTC_KEY),
                        1));
    }

    @Test
    public void testRange()
    {
        assertThat(type.getRange())
                .isEmpty();
    }
}
