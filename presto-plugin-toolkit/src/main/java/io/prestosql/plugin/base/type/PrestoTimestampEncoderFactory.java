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
package io.prestosql.plugin.base.type;

import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.TimestampType;
import org.joda.time.DateTimeZone;

import java.time.Instant;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.prestosql.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.spi.type.Timestamps.round;
import static java.lang.Math.multiplyExact;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;
import static java.util.Objects.requireNonNull;

public final class PrestoTimestampEncoderFactory
{
    private PrestoTimestampEncoderFactory()
    {
    }

    public static PrestoTimestampEncoder<? extends Comparable<?>> createTimestampEncoder(TimestampType type)
    {
        return createTimestampEncoder(type, DateTimeZone.UTC);
    }

    public static PrestoTimestampEncoder<? extends Comparable<?>> createTimestampEncoder(TimestampType type, DateTimeZone timeZone)
    {
        requireNonNull(type, "type is null");
        requireNonNull(timeZone, "timeZoneKey is null");

        if (type.isShort()) {
            return new ShortTimestampEncoder(type, timeZone);
        }
        return new LongTimestampEncoder(type, timeZone);
    }

    public static long scaleEpochMillisToMicros(long epochMillis)
    {
        return multiplyExact(epochMillis, MICROSECONDS_PER_MILLISECOND);
    }

    public static LongTimestamp longTimestamp(long precision, Instant start)
    {
        checkArgument(precision > MAX_SHORT_PRECISION && precision <= TimestampType.MAX_PRECISION, "Precision is out of range");
        return new LongTimestamp(
                start.getEpochSecond() * MICROSECONDS_PER_SECOND + start.getLong(MICRO_OF_SECOND),
                (int) round((start.getNano() % PICOSECONDS_PER_NANOSECOND) * PICOSECONDS_PER_NANOSECOND, (int) (TimestampType.MAX_PRECISION - precision)));
    }

    public static LongTimestamp longTimestamp(long epochSecond, long fractionInPicos)
    {
        return new LongTimestamp(
                multiplyExact(epochSecond, MICROSECONDS_PER_SECOND) + fractionInPicos / PICOSECONDS_PER_MICROSECOND,
                (int) (fractionInPicos % PICOSECONDS_PER_MICROSECOND));
    }
}
