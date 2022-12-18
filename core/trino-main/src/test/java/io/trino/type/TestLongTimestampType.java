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
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type.Range;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.createTimestampType;
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

    @Override
    public void testRange()
    {
        Range range = type.getRange().orElseThrow();
        assertEquals(range.getMin(), new LongTimestamp(Long.MIN_VALUE, 0));
        assertEquals(range.getMax(), new LongTimestamp(Long.MAX_VALUE, 999_000));
    }

    @Test(dataProvider = "testRangeEveryPrecisionDataProvider")
    public void testRangeEveryPrecision(int precision, LongTimestamp expectedMax)
    {
        Range range = createTimestampType(precision).getRange().orElseThrow();
        assertEquals(range.getMin(), new LongTimestamp(Long.MIN_VALUE, 0));
        assertEquals(range.getMax(), expectedMax);
    }

    @DataProvider
    public static Object[][] testRangeEveryPrecisionDataProvider()
    {
        return new Object[][] {
                {7, new LongTimestamp(Long.MAX_VALUE, 900_000)},
                {8, new LongTimestamp(Long.MAX_VALUE, 990_000)},
                {9, new LongTimestamp(Long.MAX_VALUE, 999_000)},
                {10, new LongTimestamp(Long.MAX_VALUE, 999_900)},
                {11, new LongTimestamp(Long.MAX_VALUE, 999_990)},
                {12, new LongTimestamp(Long.MAX_VALUE, 999_999)},
        };
    }
}
