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
package io.trino.decoder.json;

import com.fasterxml.jackson.databind.JsonNode;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static io.trino.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static java.lang.String.format;

public abstract class AbstractDateTimeJsonValueProvider
        extends FieldValueProvider
{
    protected final JsonNode value;
    protected final DecoderColumnHandle columnHandle;

    protected AbstractDateTimeJsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle)
    {
        this.value = value;
        this.columnHandle = columnHandle;
    }

    @Override
    public final boolean isNull()
    {
        return value.isMissingNode() || value.isNull();
    }

    @Override
    public final long getLong()
    {
        long millis = getMillis();

        Type type = columnHandle.getType();

        if (type.equals(TIME_MILLIS) || type.equals(TIME_TZ_MILLIS)) {
            if (millis < 0 || millis >= TimeUnit.DAYS.toMillis(1)) {
                throw new TrinoException(
                        DECODER_CONVERSION_NOT_SUPPORTED,
                        format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
            }
        }

        if (type.equals(DATE)) {
            return TimeUnit.MILLISECONDS.toDays(millis);
        }
        if (type.equals(TIME_MILLIS)) {
            return millis * PICOSECONDS_PER_MILLISECOND;
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return millis * MICROSECONDS_PER_MILLISECOND;
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            return packDateTimeWithZone(millis, getTimeZone());
        }
        if (type.equals(TIME_TZ_MILLIS)) {
            int offsetMinutes = getTimeZone().getZoneId().getRules().getOffset(Instant.ofEpochMilli(millis)).getTotalSeconds() / 60;
            return packTimeWithTimeZone((millis + (offsetMinutes * 60L * MILLISECONDS_PER_SECOND)) * NANOSECONDS_PER_MILLISECOND, offsetMinutes);
        }

        throw new IllegalStateException("Unsupported type: " + type);
    }

    /**
     * @return epoch milliseconds in UTC
     */
    protected abstract long getMillis();

    /**
     * @return TimeZoneKey for value
     */
    protected abstract TimeZoneKey getTimeZone();
}
