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
package io.trino.plugin.redis.decoder.hash;

import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.spi.type.Type;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;

import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ISO8601HashRedisFieldDecoder
        extends HashRedisFieldDecoder
{
    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.dateTimeParser()
            .withLocale(Locale.ENGLISH)
            .withChronology(ISOChronology.getInstanceUTC())
            .withOffsetParsed();

    @Override
    public FieldValueProvider decode(String value, DecoderColumnHandle columnHandle)
    {
        return new ISO8601HashRedisValueProvider(columnHandle, value);
    }

    private static class ISO8601HashRedisValueProvider
            extends HashRedisValueProvider
    {
        public ISO8601HashRedisValueProvider(DecoderColumnHandle columnHandle, String value)
        {
            super(columnHandle, value);
        }

        @Override
        public long getLong()
        {
            DateTime dateTime = FORMATTER.parseDateTime(getSlice().toStringAscii());
            long millis = dateTime.getMillis();

            Type type = columnHandle.getType();
            if (type.equals(DATE)) {
                return MILLISECONDS.toDays(millis);
            }
            if (type.equals(TIMESTAMP_MILLIS)) {
                return millis * MICROSECONDS_PER_MILLISECOND;
            }
            if (type.equals(TIME_MILLIS)) {
                return millis * PICOSECONDS_PER_MILLISECOND;
            }
            if (type.equals(TIMESTAMP_TZ_MILLIS) || type.equals(TIME_TZ_MILLIS)) {
                return packDateTimeWithZone(millis, getTimeZoneKey(dateTime.getZone().getID()));
            }

            throw new IllegalStateException("Unsupported type: " + type);
        }
    }
}
