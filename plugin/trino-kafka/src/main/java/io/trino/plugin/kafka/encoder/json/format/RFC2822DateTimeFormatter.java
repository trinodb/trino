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
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.SqlTimestampWithTimeZone;
import io.trino.spi.type.Type;

import java.util.Locale;

import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.time.ZoneOffset.UTC;

public class RFC2822DateTimeFormatter
        implements JsonDateTimeFormatter
{
    private static final DateTimeFormat RFC_FORMATTER = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy");

    public static boolean isSupportedType(Type type)
    {
        return type.equals(TIMESTAMP_MILLIS) ||
                type.equals(TIMESTAMP_TZ_MILLIS);
    }

    @Override
    public String formatTimestamp(SqlTimestamp value)
    {
        return RFC_FORMATTER.format(value.getMillis(), UTC, Locale.ENGLISH);
    }

    @Override
    public String formatTimestampWithZone(SqlTimestampWithTimeZone value)
    {
        return RFC_FORMATTER.format(value.getEpochMillis(), value.getTimeZoneKey().getZoneId(), Locale.ENGLISH);
    }
}
