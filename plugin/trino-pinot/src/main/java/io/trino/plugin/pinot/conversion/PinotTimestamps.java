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
package io.trino.plugin.pinot.conversion;

import com.google.common.primitives.Longs;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_INSTANT;

public final class PinotTimestamps
{
    private static final DateTimeFormatter PINOT_TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS][.SS][.S]");

    private PinotTimestamps() {}

    public static long toMicros(long millis)
    {
        return millis * MICROSECONDS_PER_MILLISECOND;
    }

    public static long toMicros(Instant instant)
    {
        return toMicros(instant.toEpochMilli());
    }

    public static LocalDateTime tryParse(String value)
    {
        Long epochMillis = Longs.tryParse(value);
        if (epochMillis != null) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), UTC);
        }
        // Try parsing using standard formats
        LocalDateTime timestamp = tryParse(PINOT_TIMESTAMP_FORMATTER, value);
        if (timestamp == null) {
            timestamp = tryParse(ISO_INSTANT, value);
        }
        return timestamp;
    }

    private static LocalDateTime tryParse(DateTimeFormatter formatter, String value)
    {
        try {
            return formatter.parse(value, LocalDateTime::from);
        }
        catch (DateTimeParseException e) {
            // Ignore
        }
        return null;
    }
}
