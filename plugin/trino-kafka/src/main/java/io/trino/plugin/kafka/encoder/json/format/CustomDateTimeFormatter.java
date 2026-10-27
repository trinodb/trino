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

import io.trino.plugin.base.time.DateTimeFormat;
import io.trino.spi.type.SqlDate;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.SqlTimeWithTimeZone;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Optional;

import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.getMillisOfDay;
import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.scaleEpochMicrosToMillis;
import static io.trino.plugin.kafka.encoder.json.format.util.TimeConversions.scalePicosToMillis;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.DAYS;

public class CustomDateTimeFormatter
        implements JsonDateTimeFormatter
{
    private final DateTimeFormat formatter;

    public static boolean isSupportedType(Type type)
    {
        return type.equals(DATE) ||
                type.equals(TIME_MILLIS) ||
                type.equals(TIME_TZ_MILLIS) ||
                type.equals(TIMESTAMP_MILLIS) ||
                type.equals(TIMESTAMP_TZ_MILLIS);
    }

    public CustomDateTimeFormatter(Optional<String> pattern)
    {
        this.formatter = DateTimeFormat.forPattern(getPattern(pattern));
    }

    private static String getPattern(Optional<String> pattern)
    {
        return pattern.orElseThrow(() -> new IllegalArgumentException("No pattern defined for custom date time format"));
    }

    @Override
    public String formatDate(SqlDate value)
    {
        return formatter.format(DAYS.toMillis(value.getDays()), UTC, Locale.ENGLISH);
    }

    @Override
    public String formatTime(SqlTime value, int precision)
    {
        return formatter.format(LocalTime.ofNanoOfDay(getMillisOfDay(scalePicosToMillis(value.getPicos())) * 1_000_000), Locale.ENGLISH);
    }

    @Override
    public String formatTimeWithZone(SqlTimeWithTimeZone value)
    {
        int offsetMinutes = value.getOffsetMinutes();
        ZoneOffset zone = ZoneOffset.ofTotalSeconds(offsetMinutes * 60);
        long picos = value.getPicos() - (offsetMinutes * 60L * PICOSECONDS_PER_SECOND);
        return formatter.format(scalePicosToMillis(picos), zone, Locale.ENGLISH);
    }

    @Override
    public String formatTimestamp(SqlTimestamp value)
    {
        return formatter.format(scaleEpochMicrosToMillis(value.getEpochMicros()), UTC, Locale.ENGLISH);
    }

    @Override
    public String formatTimestampWithZone(SqlTimestampWithTimeZone value)
    {
        ZoneId zone = value.getTimeZoneKey().getZoneId();
        return formatter.format(value.getEpochMillis(), zone, Locale.ENGLISH);
    }
}
