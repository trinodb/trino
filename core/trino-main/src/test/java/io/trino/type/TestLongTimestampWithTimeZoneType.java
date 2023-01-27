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
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLongTimestampWithTimeZoneType
        extends AbstractTestType
{
    public TestLongTimestampWithTimeZoneType()
    {
        super(TIMESTAMP_TZ_MICROS, SqlTimestampWithTimeZone.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = TIMESTAMP_TZ_MICROS.createBlockBuilder(null, 15);
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
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        // time zone doesn't matter for ordering
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(((LongTimestampWithTimeZone) value).getEpochMillis() + 1, 0, getTimeZoneKeyForOffset(33));
    }

    @Override
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

        assertThat(type.getPreviousValue(previousToMaxValue))
                .isEqualTo(Optional.of(LongTimestampWithTimeZone.fromEpochMillisAndFraction(Long.MAX_VALUE, 997_000_000, UTC_KEY)));
        assertThat(type.getPreviousValue(maxValue))
                .isEqualTo(Optional.of(previousToMaxValue));
    }

    @Override
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

        assertThat(type.getNextValue(previousToMaxValue))
                .isEqualTo(Optional.of(maxValue));
        assertThat(type.getNextValue(maxValue))
                .isEqualTo(Optional.empty());
    }

    @Test(dataProvider = "testPreviousNextValueEveryPrecisionDatProvider")
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

    @Test(dataProvider = "testPreviousNextValueEveryPrecisionDatProvider")
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
