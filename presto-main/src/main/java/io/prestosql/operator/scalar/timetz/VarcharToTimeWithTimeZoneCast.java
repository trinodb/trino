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
package io.prestosql.operator.scalar.timetz;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimeWithTimeZone;
import io.prestosql.type.DateTimes;

import java.util.regex.Matcher;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.operator.scalar.StringFunctions.trim;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.DateTimeEncoding.packTimeWithTimeZone;
import static io.prestosql.spi.type.TimeWithTimeZoneType.MAX_PRECISION;
import static io.prestosql.spi.type.TimeWithTimeZoneType.MAX_SHORT_PRECISION;
import static io.prestosql.type.DateTimes.NANOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.NANOSECONDS_PER_SECOND;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_SECOND;
import static io.prestosql.type.DateTimes.getOffsetMinutes;
import static io.prestosql.type.DateTimes.isValidOffset;
import static io.prestosql.type.DateTimes.rescale;
import static io.prestosql.type.DateTimes.round;

@ScalarOperator(CAST)
public final class VarcharToTimeWithTimeZoneCast
{
    private VarcharToTimeWithTimeZoneCast() {}

    @LiteralParameters({"x", "p"})
    @SqlType("time(p) with time zone")
    public static long castToShort(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("varchar(x)") Slice value)
    {
        checkArgument((int) precision <= MAX_SHORT_PRECISION, "precision must be less than max short timestamp precision");

        Matcher matcher = DateTimes.TIME_PATTERN.matcher(trim(value).toStringUtf8());
        if (!matcher.matches()) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8());
        }

        try {
            long nanos = parseTime(matcher) * NANOSECONDS_PER_SECOND + parseFraction((int) precision, matcher, 9);
            nanos %= NANOSECONDS_PER_DAY;

            int offsetMinutes = parseOffset(session, matcher);

            return packTimeWithTimeZone(nanos, offsetMinutes);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    @LiteralParameters({"x", "p"})
    @SqlType("time(p) with time zone")
    public static LongTimeWithTimeZone castToLong(@LiteralParameter("p") long precision, ConnectorSession session, @SqlType("varchar(x)") Slice value)
    {
        checkArgument((int) precision > MAX_SHORT_PRECISION && (int) precision <= MAX_PRECISION, "precision out of range");

        Matcher matcher = DateTimes.TIME_PATTERN.matcher(trim(value).toStringUtf8());
        if (!matcher.matches()) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8());
        }

        try {
            long picos = parseTime(matcher) * PICOSECONDS_PER_SECOND + parseFraction((int) precision, matcher, 12);
            picos %= PICOSECONDS_PER_DAY;

            int offsetMinutes = parseOffset(session, matcher);

            return new LongTimeWithTimeZone(picos, offsetMinutes);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
        }
    }

    private static int parseTime(Matcher matcher)
    {
        int hour = Integer.parseInt(matcher.group("hour"));
        int minute = Integer.parseInt(matcher.group("minute"));
        int second = matcher.group("second") == null ? 0 : Integer.parseInt(matcher.group("second"));

        if (hour > 23 || minute > 59 || second > 59) {
            throw new IllegalArgumentException("Invalid time");
        }

        return (((hour * 60) + minute) * 60 + second);
    }

    private static int parseOffset(ConnectorSession session, Matcher matcher)
    {
        if (matcher.group("offsetHour") != null && matcher.group("offsetMinute") != null) {
            int offsetHour = Integer.parseInt((matcher.group("offsetHour")));
            int offsetMinute = Integer.parseInt((matcher.group("offsetMinute")));

            if (!isValidOffset(offsetHour, offsetMinute)) {
                throw new IllegalArgumentException("Invalid time");
            }

            return offsetHour * 60 + offsetMinute;
        }

        return getOffsetMinutes(session.getStart(), session.getTimeZoneKey());
    }

    private static long parseFraction(int precision, Matcher matcher, int targetMagnitude)
    {
        String fraction = matcher.group("fraction");
        int actualPrecision = 0;
        long fractionValue = 0;
        if (fraction != null) {
            actualPrecision = fraction.length();
            fractionValue = Long.parseLong(fraction);
        }

        if (actualPrecision > MAX_PRECISION) {
            throw new IllegalArgumentException("Invalid time");
        }

        if (actualPrecision > precision) {
            fractionValue = round(fractionValue, actualPrecision - precision);
        }

        return rescale(fractionValue, actualPrecision, targetMagnitude);
    }
}
