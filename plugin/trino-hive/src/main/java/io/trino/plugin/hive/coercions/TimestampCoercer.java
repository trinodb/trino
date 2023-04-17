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

import io.airlift.slice.Slice;
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
import java.time.format.DateTimeParseException;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_TIMESTAMP_COERCION;
import static io.trino.spi.type.TimestampType.MAX_PRECISION;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.SECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.Timestamps.roundDiv;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.time.format.ResolverStyle.STRICT;

public final class TimestampCoercer
{
    private static final DateTimeFormatter LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME)
            .toFormatter()
            .withResolverStyle(STRICT)
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

    public static class VarcharToShortTimestampCoercer
            extends TypeCoercer<VarcharType, TimestampType>
    {
        public VarcharToShortTimestampCoercer(VarcharType fromType, TimestampType toType)
        {
            super(fromType, toType);
            checkArgument(toType.isShort(), format("TIMESTAMP precision must be in range [0, %s]: %s", MAX_PRECISION, toType.getPrecision()));
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            try {
                Slice value = fromType.getSlice(block, position);
                LocalDateTime dateTime = LOCAL_DATE_TIME.parse(value.toStringUtf8(), LocalDateTime::from);
                long epochSecond = dateTime.toEpochSecond(UTC);
                if (epochSecond < START_OF_MODERN_ERA_SECONDS) {
                    throw new TrinoException(HIVE_INVALID_TIMESTAMP_COERCION, "Coercion on historical dates is not supported");
                }
                long roundedNanos = round(dateTime.getNano(), 9 - toType.getPrecision());
                long epochMicros = epochSecond * MICROSECONDS_PER_SECOND + roundDiv(roundedNanos, NANOSECONDS_PER_MICROSECOND);
                toType.writeLong(blockBuilder, epochMicros);
            }
            catch (DateTimeParseException ignored) {
                // Hive treats invalid String as null instead of propagating exception
                // In case of bigger tables with all values being invalid, log output will be huge so avoiding log here.
                blockBuilder.appendNull();
            }
        }
    }

    public static class VarcharToLongTimestampCoercer
            extends TypeCoercer<VarcharType, TimestampType>
    {
        public VarcharToLongTimestampCoercer(VarcharType fromType, TimestampType toType)
        {
            super(fromType, toType);
            checkArgument(!toType.isShort(), format("Precision must be in the range [%s, %s]", MAX_SHORT_PRECISION + 1, MAX_PRECISION));
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            try {
                Slice value = fromType.getSlice(block, position);
                LocalDateTime dateTime = LOCAL_DATE_TIME.parse(value.toStringUtf8(), LocalDateTime::from);
                long epochSecond = dateTime.toEpochSecond(UTC);
                if (epochSecond < START_OF_MODERN_ERA_SECONDS) {
                    throw new TrinoException(HIVE_INVALID_TIMESTAMP_COERCION, "Coercion on historical dates is not supported");
                }
                long epochMicros = epochSecond * MICROSECONDS_PER_SECOND + dateTime.getNano() / NANOSECONDS_PER_MICROSECOND;
                int picosOfMicro = (dateTime.getNano() % NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND;
                toType.writeObject(blockBuilder, new LongTimestamp(epochMicros, picosOfMicro));
            }
            catch (DateTimeParseException ignored) {
                // Hive treats invalid String as null instead of propagating exception
                // In case of bigger tables with all values being invalid, log output will be huge so avoiding log here.
                blockBuilder.appendNull();
            }
        }
    }
}
