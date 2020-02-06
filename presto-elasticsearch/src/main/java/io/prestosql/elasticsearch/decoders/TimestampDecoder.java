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
package io.prestosql.elasticsearch.decoders;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchHit;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

public class TimestampDecoder
        implements Decoder
{
    private static final ZoneId ZULU = ZoneId.of("Z");
    private static final DateTimeFormatter STRICT_DATE_OPTIONAL_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISO_LOCAL_DATE)
            .optionalStart()
            .appendLiteral('T')
            .optionalStart()
            .appendValue(HOUR_OF_DAY, 2, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(MINUTE_OF_HOUR, 2, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(SECOND_OF_MINUTE, 2, 2, SignStyle.NOT_NEGATIVE)
            .optionalStart()
            .appendFraction(NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .optionalStart()
            .appendLiteral(',')
            .appendFraction(NANO_OF_SECOND, 1, 9, false)
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .optionalStart()
            .appendZoneOrOffsetId()
            .optionalEnd()
            .optionalStart()
            .appendOffsetId()
            .optionalEnd()
            .optionalEnd()
            .optionalEnd()
            .toFormatter(Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);
    private final String path;
    private final ZoneId zoneId;

    public TimestampDecoder(ConnectorSession session, String path)
    {
        this.path = path;
        this.zoneId = ZoneId.of(session.getTimeZoneKey().getId());
    }

    private static LocalDateTime query(TemporalAccessor parsedDateTime)
    {
        try {
            return LocalDateTime.from(parsedDateTime);
        }
        catch (DateTimeException ex) {
        }

        try {
            LocalDate date = LocalDate.from(parsedDateTime);
            return date.atStartOfDay();
        }
        catch (DateTimeException ex) {
            throw new PrestoException(NOT_SUPPORTED, "Unable to obtain LocalDateTime from TemporalAccessor: " +
                    parsedDateTime + " of type " + parsedDateTime.getClass().getName(), ex);
        }
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        if (false) {

            DocumentField documentField = hit.getFields().get(path);
            if (documentField == null) {
                output.appendNull();
            }
            else if (documentField.getValues().size() > 1) {
                throw new PrestoException(TYPE_MISMATCH, "Expected single value for column: " + path);
            }
            else {
                Object value = documentField.getValue();

                LocalDateTime timestamp;
                if (value instanceof String) {
                    timestamp = ISO_DATE_TIME.parse((String) value, LocalDateTime::from);
                }
                else if (value instanceof Number) {
                    timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) value).longValue()), ZULU);
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format(
                            "Unsupported representation for timestamp type: %s [%s]",
                            value.getClass().getSimpleName(),
                            value));
                }

                long epochMillis = timestamp.atZone(zoneId)
                        .toInstant()
                        .toEpochMilli();

                TIMESTAMP.writeLong(output, epochMillis);
            }
        }
        else {
            Map<String, Object> documentMap = hit.getSourceAsMap();
            Object documentField = null;
            for (String subPath : path.split("\\.")) {
                Object val = documentMap.get(subPath);
                if (val instanceof Map) {
                    documentMap = ((Map<String, Object>) val);
                }
                else {
                    documentField = val;
                }
            }

            if (documentField == null) {
                output.appendNull();
            }
            else {
                Object value = documentField;

                LocalDateTime timestamp;
                if (value instanceof String) {
                    timestamp = STRICT_DATE_OPTIONAL_TIME_FORMATTER.parse((String) value, TimestampDecoder::query);
                }
                else if (value instanceof Number) {
                    timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) value).longValue()), ZULU);
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format(
                            "Unsupported representation for timestamp type: %s [%s]",
                            value.getClass().getSimpleName(),
                            value));
                }

                long epochMillis = timestamp.atZone(zoneId)
                        .toInstant()
                        .toEpochMilli();

                TIMESTAMP.writeLong(output, epochMillis);
            }
        }
    }
}
