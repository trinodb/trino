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
package io.prestosql.plugin.redis.decoder.hash;

import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Locale;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class ISO8601HashRedisFieldDecoder
        extends HashRedisFieldDecoder
{
    private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.dateTimeParser().withLocale(Locale.ENGLISH).withOffsetParsed();

    private final ConnectorSession session;

    public ISO8601HashRedisFieldDecoder(ConnectorSession session)
    {
        this.session = requireNonNull(session, "session is null");
        checkArgument(this.session.isLegacyTimestamp(), "The ISO8601HashRedisFieldDecoder does not support non-legacy timestamp semantics");
    }

    @Override
    public FieldValueProvider decode(String value, DecoderColumnHandle columnHandle)
    {
        return new ISO8601HashRedisValueProvider(session, columnHandle, value);
    }

    private static class ISO8601HashRedisValueProvider
            extends HashRedisValueProvider
    {
        private final ConnectorSession session;

        public ISO8601HashRedisValueProvider(ConnectorSession session, DecoderColumnHandle columnHandle, String value)
        {
            super(columnHandle, value);
            this.session = session;
        }

        @Override
        public long getLong()
        {
            long millisUtc = FORMATTER.withZoneUTC().parseMillis(getSlice().toStringAscii());
            TimeZoneKey timeZoneKey = getTimeZoneKey(FORMATTER.parseDateTime(getSlice().toStringAscii()).getZone().getID());

            Type type = columnHandle.getType();
            if (type.equals(DATE)) {
                return MILLISECONDS.toDays(millisUtc);
            }
            if (type.equals(TIME)) {
                return millisUtc;
            }
            if (type instanceof TimestampType) {
                packDateTimeWithZone(millisUtc, session.getTimeZoneKey());
            }
            if (type instanceof TimestampWithTimeZoneType || type.equals(TIME_WITH_TIME_ZONE)) {
                return packDateTimeWithZone(millisUtc, timeZoneKey);
            }

            return millisUtc;
        }
    }
}
