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
package io.trino.plugin.memsql;

import com.google.common.annotations.VisibleForTesting;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.spi.type.TimeType;

import java.time.LocalTime;
import java.time.format.DateTimeParseException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTime;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.util.Objects.requireNonNull;

/**
 * {@link MemSqlTimeColumnMapping} is a utility class for parsing times returned by {@code SingleStore} driver.
 *
 * {@see com.singlestore.jdbc.codec.list.LocalTimeCodec}
 */
class MemSqlTimeColumnMapping
{
    static ColumnMapping memSqlTimeColumnMapping(TimeType timeType)
    {
        return ColumnMapping.longMapping(
                timeType,
                memsqlTimeReadFunction(timeType),
                memsqlTimeWriteFunction(timeType.getPrecision()));
    }

    static LongReadFunction memsqlTimeReadFunction(TimeType timeType)
    {
        requireNonNull(timeType, "timeType is null");
        checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
        int precision = timeType.getPrecision();
        return (resultSet, columnIndex) -> {
            // SingleStore driver wraps values <00:00:00 and >24:00:00, but we must disallow them
            // LocalTime time = resultSet.getObject(columnIndex, LocalTime.class);
            String timeString = resultSet.getString(columnIndex);
            return parseMemSqlLocalTime(timeString, precision);
        };
    }

    @VisibleForTesting
    static long parseMemSqlLocalTime(String timeString, int precision)
    {
        try {
            LocalTime time = LocalTime.from(ISO_LOCAL_TIME.parse(timeString));
            long nanosOfDay = time.toNanoOfDay();
            verify(nanosOfDay < NANOSECONDS_PER_DAY, "Invalid value of nanosOfDay: %s", nanosOfDay);
            long picosOfDay = nanosOfDay * PICOSECONDS_PER_NANOSECOND;
            long rounded = round(picosOfDay, 12 - precision);
            if (rounded == PICOSECONDS_PER_DAY) {
                rounded = 0;
            }
            return rounded;
        }
        catch (DateTimeParseException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static LongWriteFunction memsqlTimeWriteFunction(int precision)
    {
        checkArgument(precision <= 9, "Unsupported precision: %s", precision);
        return (statement, index, picosOfDay) -> {
            picosOfDay = round(picosOfDay, 12 - precision);
            if (picosOfDay == PICOSECONDS_PER_DAY) {
                picosOfDay = 0;
            }
            statement.setObject(index, fromTrinoTime(picosOfDay));
        };
    }

    private MemSqlTimeColumnMapping()
    {
        // prevent instantiation of utility class
    }
}
