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

import java.time.Instant;
import java.time.LocalDateTime;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

public class TimestampDecoder
        extends AbstractDecoder<Long>
{
    public TimestampDecoder()
    {
        super(TIMESTAMP_MILLIS);
    }

    @Override
    public Long convert(String path, Object value)
    {
        LocalDateTime timestamp;
        if (value instanceof String) {
            String valueString = (String) value;
            Long epochMillis = Longs.tryParse(valueString);
            if (epochMillis != null) {
                timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), UTC);
            }
            else {
                timestamp = ISO_DATE_TIME.parse(valueString, LocalDateTime::from);
            }
        }
        else if (value instanceof Number) {
            timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) value).longValue()), UTC);
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, format(
                    "Unsupported representation for field '%s' of type TIMESTAMP: %s [%s]",
                    path,
                    value,
                    value.getClass().getSimpleName()));
        }

        return timestamp.atOffset(UTC).toInstant().toEpochMilli() * MICROSECONDS_PER_MILLISECOND;
    }

    @Override
    public void write(BlockBuilder output, Long value)
    {
        TIMESTAMP_MILLIS.writeLong(output, value);
    }
}
