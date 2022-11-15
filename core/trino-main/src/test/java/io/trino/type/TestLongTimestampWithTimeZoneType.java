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

import static io.trino.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;

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
}
