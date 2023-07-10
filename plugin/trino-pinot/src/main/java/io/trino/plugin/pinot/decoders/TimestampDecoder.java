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
package io.trino.plugin.pinot.decoders;

import io.trino.plugin.pinot.conversion.PinotTimestamps;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.function.Supplier;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

public class TimestampDecoder
        implements Decoder
{
    @Override
    public void decode(Supplier<Object> getter, BlockBuilder output)
    {
        Object value = getter.get();
        if (value == null) {
            output.appendNull();
        }
        else {
            LocalDateTime timestamp;
            if (value instanceof String stringValue) {
                timestamp = PinotTimestamps.tryParse(stringValue);
                if (timestamp == null) {
                    throw new TrinoException(NOT_SUPPORTED, format(
                            "Unable to parse string representation of type TIMESTAMP: %s [%s]",
                            value,
                            value.getClass().getSimpleName()));
                }
            }
            else if (value instanceof Double || value instanceof Long) {
                timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) value).longValue()), UTC);
            }
            else {
                throw new TrinoException(NOT_SUPPORTED, format(
                        "Unsupported representation of type TIMESTAMP: %s [%s]",
                        value,
                        value.getClass().getSimpleName()));
            }
            long epochMicros = PinotTimestamps.toMicros(timestamp.atOffset(UTC).toInstant());
            TIMESTAMP_MILLIS.writeLong(output, epochMicros);
        }
    }
}
