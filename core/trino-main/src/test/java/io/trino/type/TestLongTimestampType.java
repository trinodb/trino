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
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type.Range;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestLongTimestampType
        extends AbstractTestType
{
    public TestLongTimestampType()
    {
        super(TIMESTAMP_NANOS, SqlTimestamp.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP_NANOS.createBlockBuilder(null, 15);
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
        return blockBuilder.build();
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
        assertEquals(range.getMin(), new LongTimestamp(Long.MIN_VALUE, 0));
        assertEquals(range.getMax(), new LongTimestamp(Long.MAX_VALUE, 999_000));
    }

    @Test
    public void testRangeEveryPrecision()
    {
        for (MaxPrecision entry : maxPrecisions()) {
            Range range = createTimestampType(entry.precision()).getRange().orElseThrow();
            assertEquals(range.getMin(), new LongTimestamp(Long.MIN_VALUE, 0));
            assertEquals(range.getMax(), entry.expectedMax());
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
        assertThat(type.getPreviousValue(getSampleValue()))
                .isEmpty();
    }

    @Test
    public void testNextValue()
    {
        assertThat(type.getNextValue(getSampleValue()))
                .isEmpty();
    }

    record MaxPrecision(int precision, LongTimestamp expectedMax)
    {
    }
}
