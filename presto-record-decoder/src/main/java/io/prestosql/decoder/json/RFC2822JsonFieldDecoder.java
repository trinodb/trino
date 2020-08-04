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
package io.prestosql.decoder.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Locale;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static io.prestosql.decoder.json.JsonRowDecoderFactory.throwUnsupportedColumnType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
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
    private static final Set<Type> NON_PARAMETRIC_SUPPORTED_TYPES = ImmutableSet.of(DATE, TIME, TIME_WITH_TIME_ZONE);

    /**
     * Todo - configurable time zones and locales.
     */
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.ENGLISH).withOffsetParsed();

    private final ConnectorSession session;
    private final DecoderColumnHandle columnHandle;

    public RFC2822JsonFieldDecoder(ConnectorSession session, DecoderColumnHandle columnHandle)
    {
        this.session = requireNonNull(session, "session is null");
        checkArgument(this.session.isLegacyTimestamp(), "The RFC2822 JSON field decoder only supports legacy timestamp semantics");
        this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        if (!isSupportedType(columnHandle.getType())) {
            throwUnsupportedColumnType(columnHandle);
        }
    }

    private boolean isSupportedType(Type type)
    {
        return NON_PARAMETRIC_SUPPORTED_TYPES.contains(type) ||
                type instanceof TimestampType ||
                type instanceof TimestampWithTimeZoneType;
    }

    @Override
    public FieldValueProvider decode(JsonNode value)
    {
        return new RFC2822JsonValueProvider(session, value, columnHandle);
    }

    public static class RFC2822JsonValueProvider
            extends AbstractDateTimeJsonValueProvider
    {
        public RFC2822JsonValueProvider(ConnectorSession session, JsonNode value, DecoderColumnHandle columnHandle)
        {
            super(session, value, columnHandle);
        }

        @Override
        protected long getMillisUtc()
        {
            if (!value.isValueNode()) {
                throw new PrestoException(
                        DECODER_CONVERSION_NOT_SUPPORTED,
                        format("could not parse non-value node as '%s' for column '%s'", columnHandle.getType(), columnHandle.getName()));
            }
            try {
                return FORMATTER.withZoneUTC().parseMillis(value.asText());
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(
                        DECODER_CONVERSION_NOT_SUPPORTED,
                        format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
            }
        }

        @Override
        protected TimeZoneKey getTimeZone()
        {
            if (value.isValueNode()) {
                try {
                    return getTimeZoneKey(FORMATTER.parseDateTime(value.asText()).getZone().getID());
                }
                catch (IllegalArgumentException e) {
                    throw new PrestoException(
                            DECODER_CONVERSION_NOT_SUPPORTED,
                            format("could not parse value '%s' as '%s' for column '%s'", value.asText(), columnHandle.getType(), columnHandle.getName()));
                }
            }
            throw new PrestoException(
                    DECODER_CONVERSION_NOT_SUPPORTED,
                    format("could not parse non-value node as '%s' for column '%s'", columnHandle.getType(), columnHandle.getName()));
        }
    }
}
