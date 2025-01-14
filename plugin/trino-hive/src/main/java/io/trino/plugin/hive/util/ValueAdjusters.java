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
package io.trino.plugin.hive.util;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.plugin.hive.util.CalendarUtils.convertTimestampToProlepticGregorian;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;

public final class ValueAdjusters
{
    private ValueAdjusters() {}

    public static Optional<ValueAdjuster<? extends Type>> createValueAdjuster(Type primitiveType)
    {
        if (DATE.equals(primitiveType)) {
            return Optional.of(new DateValueAdjuster());
        }
        else if (primitiveType instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                return Optional.of(new ShortTimestampValueAdjuster(timestampType));
            }
            return Optional.of(new LongTimestampValueAdjuster(timestampType));
        }
        throw new IllegalArgumentException("Unsupported type: " + primitiveType);
    }

    private static class DateValueAdjuster
            extends ValueAdjuster<DateType>
    {
        private DateValueAdjuster()
        {
            super(DATE);
        }

        @Override
        protected void adjustValue(BlockBuilder blockBuilder, Block block, int i)
        {
            int intValue = DATE.getInt(block, i);
            intValue = CalendarUtils.convertDaysToProlepticGregorian(intValue);
            DATE.writeInt(blockBuilder, intValue);
        }
    }

    private static class ShortTimestampValueAdjuster
            extends ValueAdjuster<TimestampType>
    {
        protected ShortTimestampValueAdjuster(TimestampType timestampType)
        {
            super(timestampType);
        }

        @Override
        protected void adjustValue(BlockBuilder blockBuilder, Block block, int i)
        {
            long hybridTimestamp = forType.getLong(block, i);
            long millis = floorDiv(hybridTimestamp, MICROSECONDS_PER_MILLISECOND);
            long remainderMicros = floorMod(hybridTimestamp, MICROSECONDS_PER_MILLISECOND);
            millis = convertTimestampToProlepticGregorian(millis);
            forType.writeLong(blockBuilder, millis * MICROSECONDS_PER_MILLISECOND + remainderMicros);
        }
    }

    private static class LongTimestampValueAdjuster
            extends ValueAdjuster<TimestampType>
    {
        protected LongTimestampValueAdjuster(TimestampType timestampType)
        {
            super(timestampType);
        }

        @Override
        protected void adjustValue(BlockBuilder blockBuilder, Block block, int i)
        {
            LongTimestamp timestamp = (LongTimestamp) this.forType.getObject(block, i);
            long millis = floorDiv(timestamp.getEpochMicros(), MICROSECONDS_PER_MILLISECOND);
            long remainderMicros = floorMod(timestamp.getEpochMicros(), MICROSECONDS_PER_MILLISECOND);
            millis = convertTimestampToProlepticGregorian(millis);
            forType.writeObject(blockBuilder, new LongTimestamp(millis * MICROSECONDS_PER_MILLISECOND + remainderMicros, timestamp.getPicosOfMicro()));
        }
    }
}
