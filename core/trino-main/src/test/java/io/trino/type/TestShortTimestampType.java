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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(range.getMin()).isEqualTo(Long.MIN_VALUE + 808);
        assertThat(range.getMax()).isEqualTo(Long.MAX_VALUE - 807);
    }

    @Test(dataProvider = "testRangeEveryPrecisionDataProvider")
    public void testRangeEveryPrecision(int precision, long expectedMin, long expectedMax)
    {
        Range range = createTimestampType(precision).getRange().orElseThrow();
        assertThat(range.getMin()).isEqualTo(expectedMin);
        assertThat(range.getMax()).isEqualTo(expectedMax);
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

    @Override
    public void testPreviousValue()
    {
        long minValue = Long.MIN_VALUE + 808;
        long maxValue = Long.MAX_VALUE - 807;

        assertThat(type.getPreviousValue(minValue)).isEmpty();
        assertThat(type.getPreviousValue(minValue + 1_000)).hasValue(minValue);

        assertThat(type.getPreviousValue(getSampleValue())).hasValue(1110_000L);

        assertThat(type.getPreviousValue(maxValue - 1_000)).hasValue(maxValue - 2_000);
        assertThat(type.getPreviousValue(maxValue)).hasValue(maxValue - 1_000);
    }

    @Override
    public void testNextValue()
    {
        long minValue = Long.MIN_VALUE + 808;
        long maxValue = Long.MAX_VALUE - 807;

        assertThat(type.getNextValue(minValue)).hasValue(minValue + 1_000);
        assertThat(type.getNextValue(minValue + 1_000)).hasValue(minValue + 2_000);

        assertThat(type.getNextValue(getSampleValue())).hasValue(1112_000L);

        assertThat(type.getNextValue(maxValue - 1_000)).hasValue(maxValue);
        assertThat(type.getNextValue(maxValue)).isEmpty();
    }

    @Test(dataProvider = "testPreviousNextValueEveryPrecisionDatProvider")
    public void testPreviousValueEveryPrecision(int precision, long minValue, long maxValue, long step)
    {
        Type type = createTimestampType(precision);

        assertThat(type.getPreviousValue(minValue)).isEmpty();
        assertThat(type.getPreviousValue(minValue + step)).hasValue(minValue);

        assertThat(type.getPreviousValue(0L)).hasValue(-step);
        assertThat(type.getPreviousValue(123_456_789_000_000L)).hasValue(123_456_789_000_000L - step);

        assertThat(type.getPreviousValue(maxValue - step)).hasValue(maxValue - 2 * step);
        assertThat(type.getPreviousValue(maxValue)).hasValue(maxValue - step);
    }

    @Test(dataProvider = "testPreviousNextValueEveryPrecisionDatProvider")
    public void testNextValueEveryPrecision(int precision, long minValue, long maxValue, long step)
    {
        Type type = createTimestampType(precision);

        assertThat(type.getNextValue(minValue)).hasValue(minValue + step);
        assertThat(type.getNextValue(minValue + step)).hasValue(minValue + 2 * step);

        assertThat(type.getNextValue(0L)).hasValue(step);
        assertThat(type.getNextValue(123_456_789_000_000L)).hasValue(123_456_789_000_000L + step);

        assertThat(type.getNextValue(maxValue - step)).hasValue(maxValue);
        assertThat(type.getNextValue(maxValue)).isEmpty();
    }

    @DataProvider
    public Object[][] testPreviousNextValueEveryPrecisionDatProvider()
    {
        return new Object[][] {
                {0, Long.MIN_VALUE + 775808, Long.MAX_VALUE - 775807, 1_000_000L},
                {1, Long.MIN_VALUE + 75808, Long.MAX_VALUE - 75807, 100_000L},
                {2, Long.MIN_VALUE + 5808, Long.MAX_VALUE - 5807, 10_000L},
                {3, Long.MIN_VALUE + 808, Long.MAX_VALUE - 807, 1_000L},
                {4, Long.MIN_VALUE + 8, Long.MAX_VALUE - 7, 100L},
                {5, Long.MIN_VALUE + 8, Long.MAX_VALUE - 7, 10L},
                {6, Long.MIN_VALUE, Long.MAX_VALUE, 1L},
        };
    }
}
