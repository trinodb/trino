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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.type.DateTimes;

import java.time.DateTimeException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.function.Function;
import java.util.regex.Matcher;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.scalar.StringFunctions.trim;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.TimestampWithTimeZoneType.MAX_PRECISION;
import static io.trino.spi.type.TimestampWithTimeZoneType.MAX_SHORT_PRECISION;
import static io.trino.type.DateTimes.MILLISECONDS_PER_SECOND;
import static io.trino.type.DateTimes.longTimestampWithTimeZone;
import static io.trino.type.DateTimes.rescale;
import static io.trino.type.DateTimes.round;

@ScalarOperator(CAST)
public final class VarcharToTimestampWithTimeZoneCast
{
    private VarcharToTimestampWithTimeZoneCast() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static long castToShort(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("varchar(x)") Slice value)
    {
        try {
            return toShort((int) precision, trim(value).toStringUtf8(), timezone -> {
                if (timezone == null) {
                    return session.getTimeZoneKey().getZoneId();
                }
                return ZoneId.of(timezone);
            });
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone castToLong(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("varchar(x)") Slice value)
    {
        try {
            return toLong((int) precision, trim(value).toStringUtf8(), timezone -> {
                if (timezone == null) {
                    return session.getTimeZoneKey().getZoneId();
                }
                return ZoneId.of(timezone);
            });
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    private static long toShort(int precision, String value, Function<String, ZoneId> zoneId)
    {
        checkArgument(precision <= MAX_SHORT_PRECISION, "precision must be less than max short timestamp precision");

        Matcher matcher = DateTimes.DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        ZoneId zone;
        long epochSecond;

        try {
            zone = zoneId.apply(timezone);
            epochSecond = ZonedDateTime.of(
                            Integer.parseInt(year),
                            Integer.parseInt(month),
                            Integer.parseInt(day),
                            hour == null ? 0 : Integer.parseInt(hour),
                            minute == null ? 0 : Integer.parseInt(minute),
                            second == null ? 0 : Integer.parseInt(second),
                            0,
                            zone)
                    .toEpochSecond();
        }
        catch (DateTimeException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value, e);
        }

        int actualPrecision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            actualPrecision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (actualPrecision > precision) {
            fractionValue = round(fractionValue, actualPrecision - precision);
        }

        long millisOfSecond = rescale(fractionValue, actualPrecision, MAX_SHORT_PRECISION);
        return packDateTimeWithZone(epochSecond * MILLISECONDS_PER_SECOND + millisOfSecond, getTimeZoneKey(zone.getId()));
    }

    private static LongTimestampWithTimeZone toLong(int precision, String value, Function<String, ZoneId> zoneId)
    {
        checkArgument(precision > MAX_SHORT_PRECISION && precision <= MAX_PRECISION, "precision out of range");

        Matcher matcher = DateTimes.DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        ZoneId zone;
        long epochSecond;

        try {
            zone = zoneId.apply(timezone);
            epochSecond = ZonedDateTime.of(
                            Integer.parseInt(year),
                            Integer.parseInt(month),
                            Integer.parseInt(day),
                            hour == null ? 0 : Integer.parseInt(hour),
                            minute == null ? 0 : Integer.parseInt(minute),
                            second == null ? 0 : Integer.parseInt(second),
                            0,
                            zone)
                    .toEpochSecond();
        }
        catch (DateTimeException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value, e);
        }

        int actualPrecision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            actualPrecision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (actualPrecision > precision) {
            fractionValue = round(fractionValue, actualPrecision - precision);
        }

        long fractionInPicos = rescale(fractionValue, actualPrecision, MAX_PRECISION);
        return longTimestampWithTimeZone(epochSecond, fractionInPicos, zone);
    }
}
