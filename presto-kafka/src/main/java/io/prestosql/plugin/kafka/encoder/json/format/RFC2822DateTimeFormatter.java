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
package io.prestosql.plugin.kafka.encoder.json.format;

import io.prestosql.spi.type.SqlTimestamp;
import io.prestosql.spi.type.SqlTimestampWithTimeZone;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.TimeZone;

import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static org.joda.time.DateTimeZone.UTC;

public class RFC2822DateTimeFormatter
        implements JsonDateTimeFormatter
{
    private static final DateTimeFormatter RFC_FORMATTER = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy")
            .withLocale(Locale.ENGLISH)
            .withChronology(ISOChronology.getInstanceUTC());

    public static boolean isSupportedType(Type type)
    {
        return type.equals(TIMESTAMP_MILLIS) ||
                type.equals(TIMESTAMP_TZ_MILLIS);
    }

    @Override
    public String formatTimestamp(SqlTimestamp value)
    {
        return RFC_FORMATTER.withZoneUTC().print(new DateTime(value.getMillis(), UTC));
    }

    @Override
    public String formatTimestampWithZone(SqlTimestampWithTimeZone value)
    {
        DateTimeZone dateTimeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone(value.getTimeZoneKey().getZoneId()));
        return RFC_FORMATTER.withZone(dateTimeZone).print(new DateTime(value.getEpochMillis(), dateTimeZone));
    }
}
