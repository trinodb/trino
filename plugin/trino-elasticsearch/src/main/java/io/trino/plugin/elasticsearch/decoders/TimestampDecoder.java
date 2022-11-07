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
import com.google.common.collect.ImmutableList;
import io.trino.plugin.elasticsearch.DecoderDescriptor;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.search.SearchHit;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Objects.requireNonNull;

public class TimestampDecoder
        implements Decoder
{
    private final String path;
    private final DateFormatter formatter;

    public TimestampDecoder(String path, List<String> formats)
    {
        this.path = requireNonNull(path, "path is null");
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
            LocalDateTime timestamp;
            if (value instanceof String valueString) {
                try {
                    timestamp = LocalDateTime.from(formatter.parse(valueString));
                }
                catch (DateTimeParseException | IllegalArgumentException e) {
                    // Compatible for Elasticsearch6
                    // Docvalue_fields query for Elasticsearch6 will always return ISO_DATE_TIME format
                    timestamp = ISO_DATE_TIME.parse(valueString, LocalDateTime::from);
                }
            }
            else if (value instanceof Number) {
                String valueString = String.valueOf(value);
                timestamp = LocalDateTime.from(formatter.parse(valueString));
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format(
                        "Unsupported representation for field '%s' of type TIMESTAMP: %s [%s]",
                        path,
                        value,
                        value.getClass().getSimpleName()));
            }

            long epochMicros = timestamp.atOffset(UTC).toInstant().toEpochMilli() * MICROSECONDS_PER_MILLISECOND;

            TIMESTAMP_MILLIS.writeLong(output, epochMicros);
        }
    }

    public static class Descriptor
            implements DecoderDescriptor
    {
        private final String path;
        private final List<String> formats;

        @JsonCreator
        public Descriptor(String path, List<String> formats)
        {
            this.path = path;
            this.formats = ImmutableList.copyOf(formats);
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

        @Override
        public Decoder createDecoder()
        {
            return new TimestampDecoder(path, formats);
        }
    }
}
