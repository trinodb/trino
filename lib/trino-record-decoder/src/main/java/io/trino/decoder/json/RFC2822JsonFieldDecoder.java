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
import com.google.common.collect.ImmutableSet;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.FieldValueProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.Set;

import static io.trino.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.trino.decoder.json.JsonRowDecoderFactory.throwUnsupportedColumnType;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * RFC 2822 date format decoder.
 * <p/>
 * Uses hardcoded UTC timezone and english locale.
 */
public class RFC2822JsonFieldDecoder
        implements JsonFieldDecoder
{
    private static final Set<Type> SUPPORTED_TYPES = ImmutableSet.of(TIMESTAMP_MILLIS, TIMESTAMP_TZ_MILLIS);

    /**
     * Todo - configurable time zones and locales.
     */
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy")
            .withLocale(Locale.ENGLISH)
            .withChronology(ISOChronology.getInstanceUTC())
            .withOffsetParsed();

    private final DecoderColumnHandle columnHandle;

    public RFC2822JsonFieldDecoder(DecoderColumnHandle columnHandle)
    {
        this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        if (!SUPPORTED_TYPES.contains(columnHandle.getType())) {
            throwUnsupportedColumnType(columnHandle);
        }
    }

    @Override
    public FieldValueProvider decode(JsonNode value)
    {
        return new RFC2822JsonValueProvider(value, columnHandle);
    }

    public static class RFC2822JsonValueProvider
            extends AbstractDateTimeJsonValueProvider
    {
        public RFC2822JsonValueProvider(JsonNode value, DecoderColumnHandle columnHandle)
        {
            super(value, columnHandle);
        }

        @Override
        protected long getMillis()
        {
            if (value.isValueNode()) {
                try {
                    return FORMATTER.parseMillis(value.asText());
                }
                catch (IllegalArgumentException e) {
                    throw new TrinoException(
                            DECODER_CONVERSION_NOT_SUPPORTED,
                            format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
                }
            }
            throw new TrinoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse non-value node as '%s' for column '%s'", columnHandle.getType(), columnHandle.getName()));
        }

        @Override
        protected TimeZoneKey getTimeZone()
        {
            if (value.isValueNode()) {
                try {
                    return getTimeZoneKey(FORMATTER.parseDateTime(value.asText()).getZone().getID());
                }
                catch (IllegalArgumentException e) {
                    throw new TrinoException(
                            DECODER_CONVERSION_NOT_SUPPORTED,
                            format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
                }
            }
            throw new TrinoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse non-value node as '%s' for column '%s'", columnHandle.getType(), columnHandle.getName()));
        }
    }
}
