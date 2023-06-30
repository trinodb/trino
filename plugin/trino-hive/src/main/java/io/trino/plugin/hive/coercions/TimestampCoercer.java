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
package io.trino.plugin.hive.coercions;

import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;

import java.time.LocalDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

public final class TimestampCoercer
{
    private static final DateTimeFormatter LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME)
            .toFormatter()
            .withChronology(IsoChronology.INSTANCE);

    private TimestampCoercer() {}

    public static class ShortTimestampToVarcharCoercer
            extends TypeCoercer<TimestampType, VarcharType>
    {
        public ShortTimestampToVarcharCoercer(TimestampType fromType, VarcharType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            long epochMicros = fromType.getLong(block, position);
            long epochSecond = floorDiv(epochMicros, MICROSECONDS_PER_SECOND);
            int nanoFraction = floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            toType.writeSlice(
                    blockBuilder,
                    truncateToLength(
                            Slices.utf8Slice(
                                    LOCAL_DATE_TIME.format(LocalDateTime.ofEpochSecond(epochSecond, nanoFraction, UTC))),
                            toType));
        }
    }

    public static class LongTimestampToVarcharCoercer
            extends TypeCoercer<TimestampType, VarcharType>
    {
        public LongTimestampToVarcharCoercer(TimestampType fromType, VarcharType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            LongTimestamp timestamp = (LongTimestamp) fromType.getObject(block, position);

            long epochSecond = floorDiv(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND);
            long microsFraction = floorMod(timestamp.getEpochMicros(), MICROSECONDS_PER_SECOND);
            // Hive timestamp has nanoseconds precision, so no truncation here
            long nanosFraction = (microsFraction * NANOSECONDS_PER_MICROSECOND) + (timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND);

            toType.writeSlice(
                    blockBuilder,
                    truncateToLength(
                            Slices.utf8Slice(
                                    LOCAL_DATE_TIME.format(LocalDateTime.ofEpochSecond(epochSecond, toIntExact(nanosFraction), UTC))),
                            toType));
        }
    }
}
