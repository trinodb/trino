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
package io.trino.plugin.kafka.encoder.json.format;

import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.NANOSECONDS_PER_MICROSECOND;
import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.NANOSECONDS_PER_MILLISECOND;
import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.getMicrosOfSecond;
import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.getMillisOfSecond;
import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.getNanosOfDay;
import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.scaleEpochMicrosToSeconds;
import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.scaleEpochMillisToSeconds;
import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.scalePicosToMillis;
import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.scalePicosToNanos;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_TIME;

public class ISO8601DateTimeFormatter
        implements JsonDateTimeFormatter
{
    public static boolean isSupportedType(Type type)
    {
        return type.equals(DATE) ||
                type.equals(TIME_MILLIS) ||
                type.equals(TIME_TZ_MILLIS) ||
                type.equals(TIMESTAMP_MILLIS) ||
                type.equals(TIMESTAMP_TZ_MILLIS);
    }

    @Override
    public String formatDate(SqlDate value)
    {
        return LocalDate.ofEpochDay(value.getDays()).toString();
    }

    @Override
    public String formatTime(SqlTime value, int precision)
    {
        return LocalTime.ofNanoOfDay(getNanosOfDay(scalePicosToNanos(value.getPicos()))).toString();
    }

    @Override
    public String formatTimeWithZone(SqlTimeWithTimeZone value)
    {
        int offsetMinutes = value.getOffsetMinutes();
        return ISO_OFFSET_TIME.format(LocalTime.ofNanoOfDay(scalePicosToNanos(value.getPicos())).atOffset(ZoneOffset.ofHoursMinutes(offsetMinutes / 60, offsetMinutes % 60)));
    }

    @Override
    public String formatTimestamp(SqlTimestamp value)
    {
        long epochMicros = value.getEpochMicros();
        long picosOfMicros = value.getPicosOfMicros();

        long epochSecond = scaleEpochMicrosToSeconds(epochMicros);
        int nanoFraction = getMicrosOfSecond(epochMicros) * NANOSECONDS_PER_MICROSECOND + (int) scalePicosToNanos(picosOfMicros);

        Instant instant = Instant.ofEpochSecond(epochSecond, nanoFraction);
        return LocalDateTime.ofInstant(instant, UTC).toString();
    }

    @Override
    public String formatTimestampWithZone(SqlTimestampWithTimeZone value)
    {
        long epochMillis = value.getEpochMillis();
        int picosOfMilli = value.getPicosOfMilli();

        long epochSecond = scaleEpochMillisToSeconds(epochMillis);
        int nanoFraction = getMillisOfSecond(epochMillis) * NANOSECONDS_PER_MILLISECOND + (int) scalePicosToMillis(picosOfMilli);

        return ISO_OFFSET_DATE_TIME.format(Instant.ofEpochSecond(epochSecond, nanoFraction).atZone(value.getTimeZoneKey().getZoneId()));
    }
}
