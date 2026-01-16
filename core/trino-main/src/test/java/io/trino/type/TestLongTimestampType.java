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

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type.Range;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLongTimestampType
        extends AbstractTestType
{
    public TestLongTimestampType()
    {
        super(TIMESTAMP_NANOS, SqlTimestamp.class, createTestBlock());
    }

    public static ValueBlock createTestBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP_NANOS.createFixedSizeBlockBuilder(15);
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(1111_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(1111_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(1111_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(2222_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(2222_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(2222_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(2222_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(2222_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(3333_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(3333_123, 123_000));
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(4444_123, 123_000));
        return blockBuilder.buildValueBlock();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        LongTimestamp timestamp = (LongTimestamp) value;
        return new LongTimestamp(timestamp.getEpochMicros() + 1, 0);
    }

    @Test
    public void testRange()
    {
        Range range = type.getRange().orElseThrow();
        assertThat(range.getMin()).isEqualTo(new LongTimestamp(Long.MIN_VALUE, 0));
        assertThat(range.getMax()).isEqualTo(new LongTimestamp(Long.MAX_VALUE, 999_000));
    }

    @Test
    public void testRangeEveryPrecision()
    {
        for (MaxPrecision entry : maxPrecisions()) {
            Range range = createTimestampType(entry.precision()).getRange().orElseThrow();
            assertThat(range.getMin()).isEqualTo(new LongTimestamp(Long.MIN_VALUE, 0));
            assertThat(range.getMax()).isEqualTo(entry.expectedMax());
        }
    }

    public static List<MaxPrecision> maxPrecisions()
    {
        return ImmutableList.of(
                new MaxPrecision(7, new LongTimestamp(Long.MAX_VALUE, 900_000)),
                new MaxPrecision(8, new LongTimestamp(Long.MAX_VALUE, 990_000)),
                new MaxPrecision(9, new LongTimestamp(Long.MAX_VALUE, 999_000)),
                new MaxPrecision(10, new LongTimestamp(Long.MAX_VALUE, 999_900)),
                new MaxPrecision(11, new LongTimestamp(Long.MAX_VALUE, 999_990)),
                new MaxPrecision(12, new LongTimestamp(Long.MAX_VALUE, 999_999)));
    }

    @Test
    public void testPreviousValue()
    {
        // Basic previous value within same microsecond
        assertThat(type.getPreviousValue(new LongTimestamp(1000, 500_000)))
                .isEqualTo(Optional.of(new LongTimestamp(1000, 499_000)));

        // Previous value wrapping to previous microsecond
        assertThat(type.getPreviousValue(new LongTimestamp(1000, 0)))
                .isEqualTo(Optional.of(new LongTimestamp(999, 999_000)));

        // Previous value at minimum microsecond boundary returns empty
        assertThat(type.getPreviousValue(new LongTimestamp(Long.MIN_VALUE, 0)))
                .isEmpty();

        // Previous value at minimum microsecond but non-zero picos
        assertThat(type.getPreviousValue(new LongTimestamp(Long.MIN_VALUE, 1_000)))
                .isEqualTo(Optional.of(new LongTimestamp(Long.MIN_VALUE, 0)));
    }

    @Test
    public void testNextValue()
    {
        // Basic next value within same microsecond
        assertThat(type.getNextValue(new LongTimestamp(1000, 500_000)))
                .isEqualTo(Optional.of(new LongTimestamp(1000, 501_000)));

        // Next value wrapping to next microsecond
        assertThat(type.getNextValue(new LongTimestamp(1000, 999_000)))
                .isEqualTo(Optional.of(new LongTimestamp(1001, 0)));

        // Next value at maximum returns empty
        assertThat(type.getNextValue(new LongTimestamp(Long.MAX_VALUE, 999_000)))
                .isEmpty();

        // Next value at maximum microsecond but not max picos
        assertThat(type.getNextValue(new LongTimestamp(Long.MAX_VALUE, 998_000)))
                .isEqualTo(Optional.of(new LongTimestamp(Long.MAX_VALUE, 999_000)));
    }

    @Test
    public void testPreviousValueEveryPrecision()
    {
        // Test that previous value decrements by correct step size for each precision
        for (MaxPrecision entry : maxPrecisions()) {
            var timestampType = createTimestampType(entry.precision());
            int stepSize = getStepSize(entry.precision());

            // Basic previous value
            assertThat(timestampType.getPreviousValue(new LongTimestamp(1000, stepSize * 5)))
                    .isEqualTo(Optional.of(new LongTimestamp(1000, stepSize * 4)));

            // Previous value wrapping to previous microsecond
            assertThat(timestampType.getPreviousValue(new LongTimestamp(1000, 0)))
                    .isEqualTo(Optional.of(new LongTimestamp(999, 1_000_000 - stepSize)));
        }
    }

    @Test
    public void testNextValueEveryPrecision()
    {
        // Test that next value increments by correct step size for each precision
        for (MaxPrecision entry : maxPrecisions()) {
            var timestampType = createTimestampType(entry.precision());
            int stepSize = getStepSize(entry.precision());

            // Basic next value
            assertThat(timestampType.getNextValue(new LongTimestamp(1000, stepSize * 5)))
                    .isEqualTo(Optional.of(new LongTimestamp(1000, stepSize * 6)));

            // Next value wrapping to next microsecond
            assertThat(timestampType.getNextValue(new LongTimestamp(1000, 1_000_000 - stepSize)))
                    .isEqualTo(Optional.of(new LongTimestamp(1001, 0)));
        }
    }

    private static int getStepSize(int precision)
    {
        // Step size in picoseconds for each precision
        // precision 7 = 100_000 picos (100 nanos)
        // precision 8 = 10_000 picos (10 nanos)
        // precision 9 = 1_000 picos (1 nano)
        // precision 10 = 100 picos
        // precision 11 = 10 picos
        // precision 12 = 1 pico
        return switch (precision) {
            case 7 -> 100_000;
            case 8 -> 10_000;
            case 9 -> 1_000;
            case 10 -> 100;
            case 11 -> 10;
            case 12 -> 1;
            default -> throw new IllegalArgumentException("Unsupported precision: " + precision);
        };
    }

    record MaxPrecision(int precision, LongTimestamp expectedMax) {}
}
