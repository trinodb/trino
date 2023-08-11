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

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
import io.trino.spi.type.Type.Range;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Stream;

import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestShortTimestampType
        extends AbstractTestType
{
    public TestShortTimestampType()
    {
        super(TIMESTAMP_MILLIS, SqlTimestamp.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP_MILLIS.createBlockBuilder(null, 15);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 1111_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 1111_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 1111_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 2222_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 2222_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 2222_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 2222_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 2222_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 3333_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 3333_000);
        TIMESTAMP_MILLIS.writeLong(blockBuilder, 4444_000);
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((Long) value) + 1_000;
    }

    @Test
    public void testRange()
    {
        Range range = type.getRange().orElseThrow();
        assertEquals(range.getMin(), Long.MIN_VALUE + 808);
        assertEquals(range.getMax(), Long.MAX_VALUE - 807);
    }

    @ParameterizedTest
    @MethodSource("testRangeEveryPrecisionDataProvider")
    public void testRangeEveryPrecision(int precision, long expectedMin, long expectedMax)
    {
        Range range = createTimestampType(precision).getRange().orElseThrow();
        assertEquals(range.getMin(), expectedMin);
        assertEquals(range.getMax(), expectedMax);
    }

    public static Stream<Arguments> testRangeEveryPrecisionDataProvider()
    {
        return Stream.of(
                Arguments.of(0, Long.MIN_VALUE + 775808, Long.MAX_VALUE - 775807),
                Arguments.of(1, Long.MIN_VALUE + 75808, Long.MAX_VALUE - 75807),
                Arguments.of(2, Long.MIN_VALUE + 5808, Long.MAX_VALUE - 5807),
                Arguments.of(3, Long.MIN_VALUE + 808, Long.MAX_VALUE - 807),
                Arguments.of(4, Long.MIN_VALUE + 8, Long.MAX_VALUE - 7),
                Arguments.of(5, Long.MIN_VALUE + 8, Long.MAX_VALUE - 7),
                Arguments.of(6, Long.MIN_VALUE, Long.MAX_VALUE));
    }

    @Test
    public void testPreviousValue()
    {
        long minValue = Long.MIN_VALUE + 808;
        long maxValue = Long.MAX_VALUE - 807;

        assertThat(type.getPreviousValue(minValue))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(minValue + 1_000))
                .isEqualTo(Optional.of(minValue));

        assertThat(type.getPreviousValue(getSampleValue()))
                .isEqualTo(Optional.of(1110_000L));

        assertThat(type.getPreviousValue(maxValue - 1_000))
                .isEqualTo(Optional.of(maxValue - 2_000));
        assertThat(type.getPreviousValue(maxValue))
                .isEqualTo(Optional.of(maxValue - 1_000));
    }

    @Test
    public void testNextValue()
    {
        long minValue = Long.MIN_VALUE + 808;
        long maxValue = Long.MAX_VALUE - 807;

        assertThat(type.getNextValue(minValue))
                .isEqualTo(Optional.of(minValue + 1_000));
        assertThat(type.getNextValue(minValue + 1_000))
                .isEqualTo(Optional.of(minValue + 2_000));

        assertThat(type.getNextValue(getSampleValue()))
                .isEqualTo(Optional.of(1112_000L));

        assertThat(type.getNextValue(maxValue - 1_000))
                .isEqualTo(Optional.of(maxValue));
        assertThat(type.getNextValue(maxValue))
                .isEqualTo(Optional.empty());
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueEveryPrecisionDataProvider")
    public void testPreviousValueEveryPrecision(int precision, long minValue, long maxValue, long step)
    {
        Type type = createTimestampType(precision);

        assertThat(type.getPreviousValue(minValue))
                .isEqualTo(Optional.empty());
        assertThat(type.getPreviousValue(minValue + step))
                .isEqualTo(Optional.of(minValue));

        assertThat(type.getPreviousValue(0L))
                .isEqualTo(Optional.of(-step));
        assertThat(type.getPreviousValue(123_456_789_000_000L))
                .isEqualTo(Optional.of(123_456_789_000_000L - step));

        assertThat(type.getPreviousValue(maxValue - step))
                .isEqualTo(Optional.of(maxValue - 2 * step));
        assertThat(type.getPreviousValue(maxValue))
                .isEqualTo(Optional.of(maxValue - step));
    }

    @ParameterizedTest
    @MethodSource("testPreviousNextValueEveryPrecisionDataProvider")
    public void testNextValueEveryPrecision(int precision, long minValue, long maxValue, long step)
    {
        Type type = createTimestampType(precision);

        assertThat(type.getNextValue(minValue))
                .isEqualTo(Optional.of(minValue + step));
        assertThat(type.getNextValue(minValue + step))
                .isEqualTo(Optional.of(minValue + 2 * step));

        assertThat(type.getNextValue(0L))
                .isEqualTo(Optional.of(step));
        assertThat(type.getNextValue(123_456_789_000_000L))
                .isEqualTo(Optional.of(123_456_789_000_000L + step));

        assertThat(type.getNextValue(maxValue - step))
                .isEqualTo(Optional.of(maxValue));
        assertThat(type.getNextValue(maxValue))
                .isEqualTo(Optional.empty());
    }

    public static Stream<Arguments> testPreviousNextValueEveryPrecisionDataProvider()
    {
        return Stream.of(
                Arguments.of(0, Long.MIN_VALUE + 775808, Long.MAX_VALUE - 775807, 1_000_000L),
                Arguments.of(1, Long.MIN_VALUE + 75808, Long.MAX_VALUE - 75807, 100_000L),
                Arguments.of(2, Long.MIN_VALUE + 5808, Long.MAX_VALUE - 5807, 10_000L),
                Arguments.of(3, Long.MIN_VALUE + 808, Long.MAX_VALUE - 807, 1_000L),
                Arguments.of(4, Long.MIN_VALUE + 8, Long.MAX_VALUE - 7, 100L),
                Arguments.of(5, Long.MIN_VALUE + 8, Long.MAX_VALUE - 7, 10L),
                Arguments.of(6, Long.MIN_VALUE, Long.MAX_VALUE, 1L));
    }
}
