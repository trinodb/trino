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
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;

import java.time.LocalDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_TIMESTAMP_COERCION;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.SECONDS_PER_DAY;
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

    // Before 1900, Java Time and Joda Time are not consistent with java.sql.Date and java.util.Calendar
    private static final long START_OF_MODERN_ERA_SECONDS = java.time.LocalDate.of(1900, 1, 1).toEpochDay() * SECONDS_PER_DAY;

    private TimestampCoercer() {}

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
            if (epochSecond < START_OF_MODERN_ERA_SECONDS) {
                throw new TrinoException(HIVE_INVALID_TIMESTAMP_COERCION, "Coercion on historical dates is not supported");
            }

            toType.writeSlice(
                    blockBuilder,
                    truncateToLength(
                            Slices.utf8Slice(
                                    LOCAL_DATE_TIME.format(LocalDateTime.ofEpochSecond(epochSecond, toIntExact(nanosFraction), UTC))),
                            toType));
        }
    }
}
