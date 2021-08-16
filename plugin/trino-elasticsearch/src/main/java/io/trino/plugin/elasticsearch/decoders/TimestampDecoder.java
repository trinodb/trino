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

import com.google.common.primitives.Longs;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DateTimeEncoding;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateFormatters;
import org.elasticsearch.search.SearchHit;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class TimestampDecoder
        implements Decoder
{
    private final String path;
    private final String format;

    public TimestampDecoder(String path, String format)
    {
        this.path = requireNonNull(path, "path is null");
        this.format = requireNonNull(format, "format is null");
    }

    @Override
    public void decode(SearchHit hit, Supplier<Object> getter, BlockBuilder output)
    {
        DocumentField documentField = hit.getFields().get(path);
        Object value = null;

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
            Long epochMillis;
            ZoneOffset zoneOffset;
            if (value instanceof String) {
                String valueString = (String) value;
                epochMillis = Longs.tryParse(valueString);
                if (epochMillis == null) {
                    ZonedDateTime dateTime = DateFormatters.from(DateFormatter.forPattern(format).parse(valueString));
                    zoneOffset = dateTime.getOffset();
                    epochMillis = dateTime.toInstant().toEpochMilli();
                }
                else {
                    zoneOffset = UTC;
                }
            }
            else if (value instanceof Number) {
                epochMillis = ((Number) value).longValue();
                zoneOffset = UTC;
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format(
                        "Unsupported representation for field '%s' of type TIMESTAMP: %s [%s]",
                        path,
                        value,
                        value.getClass().getSimpleName()));
            }

            TIMESTAMP_TZ_MILLIS.writeLong(output, DateTimeEncoding.packDateTimeWithZone(epochMillis, zoneOffset.getId()));
        }
    }
}
