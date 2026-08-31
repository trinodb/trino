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
package io.trino.plugin.paimon;

import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import org.apache.paimon.data.LocalZoneTimestamp;
import org.apache.paimon.data.Timestamp;

import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.toIntExact;

final class PaimonTrinoTypeConversions
{
    private PaimonTrinoTypeConversions() {}

    static long paimonTimeMillisToTrinoPicos(int millisOfDay)
    {
        return millisOfDay * (long) PICOSECONDS_PER_MILLISECOND;
    }

    static int trinoTimePicosToPaimonMillis(long picosOfDay)
    {
        if (picosOfDay % PICOSECONDS_PER_MILLISECOND != 0) {
            throw new IllegalArgumentException("Paimon stores TIME values with millisecond precision");
        }
        return toIntExact(picosOfDay / PICOSECONDS_PER_MILLISECOND);
    }

    static Object paimonTimestampToTrino(Type type, Timestamp timestamp)
    {
        TimestampType timestampType = (TimestampType) type;
        long epochMicros = timestamp.toMicros();
        if (timestampType.isShort()) {
            return epochMicros;
        }
        return new LongTimestamp(epochMicros, timestamp.getNanoOfMillisecond() % 1_000 * PICOSECONDS_PER_NANOSECOND);
    }

    static Timestamp trinoTimestampToPaimon(Object trinoNativeValue)
    {
        if (trinoNativeValue instanceof Long value) {
            return Timestamp.fromMicros(value);
        }
        LongTimestamp value = (LongTimestamp) trinoNativeValue;
        long epochMicros = value.getEpochMicros();
        long epochMillis = Math.floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND);
        int nanoOfMillisecond = toIntExact((epochMicros - epochMillis * MICROSECONDS_PER_MILLISECOND) * 1_000
                + value.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND);
        return Timestamp.fromEpochMillis(epochMillis, nanoOfMillisecond);
    }

    static Object paimonTimestampToTrinoTimestampWithTimeZone(Type type, Object value)
    {
        Timestamp timestamp = toPaimonTimestamp(value);
        TimestampWithTimeZoneType timestampWithTimeZoneType = (TimestampWithTimeZoneType) type;
        if (timestampWithTimeZoneType.isShort()) {
            return DateTimeEncoding.packDateTimeWithZone(timestamp.getMillisecond(), UTC_KEY);
        }
        return fromEpochMillisAndFraction(
                timestamp.getMillisecond(),
                timestamp.getNanoOfMillisecond() * PICOSECONDS_PER_NANOSECOND,
                UTC_KEY);
    }

    private static Timestamp toPaimonTimestamp(Object value)
    {
        if (value instanceof Timestamp timestamp) {
            return timestamp;
        }
        LocalZoneTimestamp timestamp = (LocalZoneTimestamp) value;
        return Timestamp.fromEpochMillis(timestamp.getMillisecond(), timestamp.getNanoOfMillisecond());
    }

    static Timestamp trinoTimestampWithTimeZoneToPaimon(Object trinoNativeValue)
    {
        if (trinoNativeValue instanceof Long value) {
            return Timestamp.fromEpochMillis(unpackMillisUtc(value));
        }
        LongTimestampWithTimeZone value = (LongTimestampWithTimeZone) trinoNativeValue;
        return Timestamp.fromEpochMillis(value.getEpochMillis(), value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND);
    }
}
