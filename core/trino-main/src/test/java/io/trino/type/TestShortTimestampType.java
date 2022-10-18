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
import io.trino.spi.type.Type.Range;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
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

    @Override
    public void testRange()
    {
        Range range = type.getRange().orElseThrow();
        assertEquals(range.getMin(), Long.MIN_VALUE + 808);
        assertEquals(range.getMax(), Long.MAX_VALUE - 807);
    }

    @Test(dataProvider = "testRangeEveryPrecisionDataProvider")
    public void testRangeEveryPrecision(int precision, long expectedMin, long expectedMax)
    {
        Range range = createTimestampType(precision).getRange().orElseThrow();
        assertEquals(range.getMin(), expectedMin);
        assertEquals(range.getMax(), expectedMax);
    }

    @DataProvider
    public static Object[][] testRangeEveryPrecisionDataProvider()
    {
        return new Object[][] {
                {0, Long.MIN_VALUE + 775808, Long.MAX_VALUE - 775807},
                {1, Long.MIN_VALUE + 75808, Long.MAX_VALUE - 75807},
                {2, Long.MIN_VALUE + 5808, Long.MAX_VALUE - 5807},
                {3, Long.MIN_VALUE + 808, Long.MAX_VALUE - 807},
                {4, Long.MIN_VALUE + 8, Long.MAX_VALUE - 7},
                {5, Long.MIN_VALUE + 8, Long.MAX_VALUE - 7},
                {6, Long.MIN_VALUE, Long.MAX_VALUE},
        };
    }
}
