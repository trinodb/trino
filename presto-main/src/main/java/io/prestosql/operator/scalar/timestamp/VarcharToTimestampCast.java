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
package io.prestosql.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.type.Timestamps;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.function.Function;
import java.util.regex.Matcher;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.operator.scalar.StringFunctions.trim;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.TimestampType.MAX_PRECISION;
import static io.prestosql.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.prestosql.type.Timestamps.MICROSECONDS_PER_SECOND;
import static io.prestosql.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.prestosql.type.Timestamps.longTimestamp;
import static io.prestosql.type.Timestamps.rescale;
import static io.prestosql.type.Timestamps.round;

@ScalarOperator(CAST)
public final class VarcharToTimestampCast
{
    private VarcharToTimestampCast() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static long castToShort(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("varchar(x)") Slice value)
    {
        // This accepts value with or without time zone
        if (session.isLegacyTimestamp()) {
            try {
                return castToLegacyShortTimestamp((int) precision, session.getTimeZoneKey(), trim(value).toStringUtf8());
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
            }
        }

        try {
            return castToShortTimestamp((int) precision, trim(value).toStringUtf8(), timezone -> ZoneOffset.UTC);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static LongTimestamp castToLong(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("varchar(x)") Slice value)
    {
        // This accepts value with or without time zone
        if (session.isLegacyTimestamp()) {
            try {
                return castToLegacyLongTimestamp((int) precision, session.getTimeZoneKey(), trim(value).toStringUtf8());
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
            }
        }

        try {
            return castToLongTimestamp((int) precision, trim(value).toStringUtf8(), timezone -> ZoneOffset.UTC);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    public static long castToLegacyShortTimestamp(int precision, TimeZoneKey timeZoneKey, String value)
    {
        return castToShortTimestamp(precision, value, timezone -> {
            if (timezone == null) {
                return timeZoneKey.getZoneId();
            }
            return ZoneId.of(timezone);
        });
    }

    private static LongTimestamp castToLegacyLongTimestamp(int precision, TimeZoneKey timeZoneKey, String value)
    {
        return castToLongTimestamp(precision, value, timezone -> {
            if (timezone == null) {
                return timeZoneKey.getZoneId();
            }
            return ZoneId.of(timezone);
        });
    }

    private static long castToShortTimestamp(int precision, String value, Function<String, ZoneId> zoneId)
    {
        checkArgument(precision <= MAX_SHORT_PRECISION, "precision must be less than max short timestamp precision");

        Matcher matcher = Timestamps.DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid timestamp: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        long epochSecond = ZonedDateTime.of(
                Integer.parseInt(year),
                Integer.parseInt(month),
                Integer.parseInt(day),
                hour == null ? 0 : Integer.parseInt(hour),
                minute == null ? 0 : Integer.parseInt(minute),
                second == null ? 0 : Integer.parseInt(second),
                0,
                zoneId.apply(timezone))
                .toEpochSecond();

        int actualPrecision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            actualPrecision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (actualPrecision > precision) {
            fractionValue = round(fractionValue, actualPrecision - precision);
        }

        if (precision <= 3) {
            return epochSecond * MILLISECONDS_PER_SECOND + rescale(fractionValue, actualPrecision, 3);
        }

        // scale to micros
        return epochSecond * MICROSECONDS_PER_SECOND + rescale(fractionValue, actualPrecision, 6);
    }

    private static LongTimestamp castToLongTimestamp(int precision, String value, Function<String, ZoneId> zoneId)
    {
        checkArgument(precision > MAX_SHORT_PRECISION && precision <= MAX_PRECISION, "precision out of range");

        Matcher matcher = Timestamps.DATETIME_PATTERN.matcher(value);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid timestamp: " + value);
        }

        String year = matcher.group("year");
        String month = matcher.group("month");
        String day = matcher.group("day");
        String hour = matcher.group("hour");
        String minute = matcher.group("minute");
        String second = matcher.group("second");
        String fraction = matcher.group("fraction");
        String timezone = matcher.group("timezone");

        long epochSecond = ZonedDateTime.of(
                Integer.parseInt(year),
                Integer.parseInt(month),
                Integer.parseInt(day),
                hour == null ? 0 : Integer.parseInt(hour),
                minute == null ? 0 : Integer.parseInt(minute),
                second == null ? 0 : Integer.parseInt(second),
                0,
                zoneId.apply(timezone))
                .toEpochSecond();

        int actualPrecision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            actualPrecision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (actualPrecision > precision) {
            fractionValue = round(fractionValue, actualPrecision - precision);
        }

        return longTimestamp(epochSecond, rescale(fractionValue, actualPrecision, 12));
    }
}
