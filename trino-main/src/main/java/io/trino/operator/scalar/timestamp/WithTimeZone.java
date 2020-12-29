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
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TimeZoneNotSupportedException;
import org.joda.time.DateTimeZone;

import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.type.DateTimes.getMicrosOfMilli;
import static io.prestosql.type.DateTimes.scaleEpochMicrosToMillis;
import static io.prestosql.util.DateTimeZoneIndex.getDateTimeZone;
import static java.lang.String.format;
import static org.joda.time.DateTimeZone.UTC;

@ScalarFunction("with_timezone")
public class WithTimeZone
{
    private WithTimeZone() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static long shortPrecision(@LiteralParameter("p") long precision, @SqlType("timestamp(p)") long timestamp, @SqlType("varchar(x)") Slice zoneId)
    {
        verify(precision <= 3, "Expected precision <= 3");

        TimeZoneKey toTimeZoneKey;
        try {
            toTimeZoneKey = getTimeZoneKey(zoneId.toStringUtf8());
        }
        catch (TimeZoneNotSupportedException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("'%s' is not a valid time zone", zoneId.toStringUtf8()));
        }
        DateTimeZone toDateTimeZone = getDateTimeZone(toTimeZoneKey);
        return packDateTimeWithZone(UTC.getMillisKeepLocal(toDateTimeZone, scaleEpochMicrosToMillis(timestamp)), toTimeZoneKey);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone mediumPrecision(@LiteralParameter("p") long precision, @SqlType("timestamp(p)") long timestamp, @SqlType("varchar(x)") Slice zoneId)
    {
        verify(precision > 3 && precision <= 6, "Expected precision in [4, 6]");
        return toLong(timestamp, 0, zoneId);
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone largePrecision(@LiteralParameter("p") long precision, @SqlType("timestamp(p)") LongTimestamp timestamp, @SqlType("varchar(x)") Slice zoneId)
    {
        verify(precision > 6, "Expected precision > 6");

        return toLong(timestamp.getEpochMicros(), timestamp.getPicosOfMicro(), zoneId);
    }

    private static LongTimestampWithTimeZone toLong(long epochMicros, int picosOfMicro, Slice zoneId)
    {
        TimeZoneKey toTimeZoneKey;
        try {
            toTimeZoneKey = getTimeZoneKey(zoneId.toStringUtf8());
        }
        catch (TimeZoneNotSupportedException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("'%s' is not a valid time zone", zoneId.toStringUtf8()));
        }
        DateTimeZone toDateTimeZone = getDateTimeZone(toTimeZoneKey);

        long epochMillis = scaleEpochMicrosToMillis(epochMicros);
        epochMillis = UTC.getMillisKeepLocal(toDateTimeZone, epochMillis);

        int picosOfMilli = getMicrosOfMilli(epochMicros) * PICOSECONDS_PER_MICROSECOND + picosOfMicro;
        return LongTimestampWithTimeZone.fromEpochMillisAndFraction(epochMillis, picosOfMilli, toTimeZoneKey);
    }
}
