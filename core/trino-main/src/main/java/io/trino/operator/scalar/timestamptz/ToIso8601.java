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
package io.trino.operator.scalar.timestamptz;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.type.Constraint;
import io.trino.type.DateTimes;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackZoneKey;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.type.DateTimes.PICOSECONDS_PER_MILLISECOND;
import static io.trino.type.DateTimes.getMillisOfSecond;

@ScalarFunction("to_iso8601")
public final class ToIso8601
{
    private static final DateTimeFormatter ISO8601_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss");

    // 1 digit for year sign
    // 6 digits for year -- TODO: we should constrain this further. A 6-digit year seems useless
    // 15 digits for -MM-DDTHH:MM:SS
    // min(p, 1) for the fractional second period (i.e., no period if p == 0)
    // p for the fractional digits
    // 6 for timezone offset
    private static final String RESULT_LENGTH = "1 + 6 + 15 + min(p, 1) + p + 6";

    private ToIso8601() {}

    @LiteralParameters({"p", "n"})
    @SqlType("varchar(n)")
    @Constraint(variable = "n", expression = RESULT_LENGTH)
    public static Slice toIso8601(@LiteralParameter("p") long precision, @SqlType("timestamp(p) with time zone") long packedEpochMillis)
    {
        long epochMillis = unpackMillisUtc(packedEpochMillis);
        ZoneId zoneId = unpackZoneKey(packedEpochMillis).getZoneId();

        return utf8Slice(format((int) precision, epochMillis, 0, zoneId));
    }

    @LiteralParameters({"p", "n"})
    @SqlType("varchar(n)")
    @Constraint(variable = "n", expression = RESULT_LENGTH)
    public static Slice toIso8601(@LiteralParameter("p") long precision, @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone timestamp)
    {
        return utf8Slice(format((int) precision, timestamp.getEpochMillis(), timestamp.getPicosOfMilli(), getTimeZoneKey(timestamp.getTimeZoneKey()).getZoneId()));
    }

    private static String format(int precision, long epochMillis, int picosOfMilli, ZoneId zoneId)
    {
        long picoFraction = ((long) getMillisOfSecond(epochMillis)) * PICOSECONDS_PER_MILLISECOND + picosOfMilli;

        ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), zoneId);
        ZoneOffset offset = dateTime.getOffset();
        if (offset.getTotalSeconds() % 60 != 0) {
            throw new TrinoException(INVALID_ARGUMENTS, "Timezone with non-zero seconds offset cannot be rendered as ISO8601: " + offset.getId());
        }

        return DateTimes.formatTimestamp(precision, dateTime.toLocalDateTime(), picoFraction, ISO8601_FORMATTER, builder -> builder.append(offset));
    }
}
