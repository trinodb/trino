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
package io.trino.plugin.elasticsearch.decoders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.elasticsearch.DecoderDescriptor;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.LongTimestamp;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.search.SearchHit;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TimestampDecoder
        implements Decoder
{
    private final String path;
    private final DateFormatter formatter;

    private final int precision;

    public TimestampDecoder(String path, List<String> formats, int precision)
    {
        this.path = requireNonNull(path, "path is null");
        this.precision = precision;
        if (formats.isEmpty()) {
            this.formatter = DateFormatter.forPattern("strict_date_optional_time||epoch_millis");
        }
        else {
            this.formatter = DateFormatter.forPattern(String.join("||", formats));
        }
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        DocumentField documentField = hit.getFields().get(path);
        Object value;

        if (documentField != null) {
            if (documentField.getValues().size() > 1) {
                throw new TrinoException(TYPE_MISMATCH, format("Expected single value for column '%s', found: %s", path, documentField.getValues().size()));
            }
            value = documentField.getValue();
        }
        else {
            value = getter.get();
        }

        if (value == null) {
            output.appendNull();
        }
        else {
            if (value instanceof Number) {
                value = String.valueOf(value);
            }
            if (value instanceof String valueString) {
                if (precision <= 3) {
                    long epochMicros = formatter.parseMillis(valueString) * MICROSECONDS_PER_MILLISECOND;
                    TIMESTAMP_MILLIS.writeLong(output, epochMicros);
                }
                else if (precision <= MAX_SHORT_PRECISION) {
                    // Elasticsearch currently only support type 'date' and 'date_nanos' for date field.
                }
                else {
                    Instant instant;
                    TemporalAccessor temporalAccessor = formatter.parse(valueString);
                    if (temporalAccessor.isSupported(ChronoField.INSTANT_SECONDS)) {
                        instant = Instant.from(temporalAccessor);
                    }
                    else {
                        // The case that Elasticsearch date format does not include time zone information
                        LocalDateTime localDateTime = LocalDateTime.of(
                                temporalAccessor.get(ChronoField.YEAR_OF_ERA),
                                temporalAccessor.get(ChronoField.MONTH_OF_YEAR),
                                temporalAccessor.get(ChronoField.DAY_OF_MONTH),
                                temporalAccessor.get(ChronoField.HOUR_OF_DAY),
                                temporalAccessor.get(ChronoField.MINUTE_OF_HOUR),
                                temporalAccessor.get(ChronoField.SECOND_OF_MINUTE),
                                temporalAccessor.get(ChronoField.NANO_OF_SECOND));
                        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
                        instant = Instant.from(zonedDateTime);
                    }
                    long epochMicros = instant.getLong(ChronoField.INSTANT_SECONDS) * MICROSECONDS_PER_SECOND
                            + instant.getLong(ChronoField.MICRO_OF_SECOND);
                    int picoOfMicros = instant.get(ChronoField.NANO_OF_SECOND) * PICOSECONDS_PER_NANOSECOND
                            - instant.get(ChronoField.MICRO_OF_SECOND) * PICOSECONDS_PER_MICROSECOND;
                    TIMESTAMP_NANOS.writeObject(output, new LongTimestamp(epochMicros, picoOfMicros));
                }
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format(
                        "Unsupported representation for field '%s' of type TIMESTAMP: %s [%s]",
                        path,
                        value,
                        value.getClass().getSimpleName()));
            }
        }
    }

    public static class Descriptor
            implements DecoderDescriptor
    {
        private final String path;
        private final List<String> formats;
        private final int precision;

        @JsonCreator
        public Descriptor(String path, List<String> formats, int precision)
        {
            this.path = path;
            this.formats = formats;
            this.precision = precision;
        }

        @JsonProperty
        public String getPath()
        {
            return path;
        }

        @JsonProperty
        public List<String> getFormats()
        {
            return formats;
        }

        @JsonProperty
        public int getPrecision()
        {
            return precision;
        }

        @Override
        public Decoder createDecoder()
        {
            return new TimestampDecoder(path, formats, precision);
        }
    }
}
