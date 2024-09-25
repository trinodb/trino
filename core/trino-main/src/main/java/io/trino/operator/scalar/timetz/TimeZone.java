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
package io.trino.operator.scalar.timetz;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimeWithTimeZone;
import io.trino.spi.type.StandardTypes;

import static com.google.common.base.Strings.padStart;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.DateTimeEncoding.unpackOffsetMinutes;
import static io.trino.type.DateTimes.MINUTES_PER_HOUR;

@Description("Time zone of the given time")
@ScalarFunction("timezone")
public final class TimeZone
{
    private TimeZone() {}

    /**
     * Extracts the time zone from a `time(p) with time zone` as a VARCHAR.
     * This function takes a packed long value representing
     * the time with time zone and extracts the associated time zone offset.
     * <p>
     *
     * @param timeWithTimeZone the packed long
     * representing a `time(p) with time zone`
     * @return the time zone offset in the format
     * "+HH:mm" or "-HH:mm" as a VARCHAR
     */
    @LiteralParameters("p")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice extractTimeZoneFromTime(@SqlType("time(p) with time zone") long timeWithTimeZone)
    {
        long totalOffsetMinutes = unpackOffsetMinutes(timeWithTimeZone);

        char sign = totalOffsetMinutes < 0 ? '-' : '+';
        long absoluteOffsetMinutes = Math.abs(totalOffsetMinutes);

        String hours = Long.toString(absoluteOffsetMinutes / MINUTES_PER_HOUR);
        String minutes = Long.toString(absoluteOffsetMinutes % MINUTES_PER_HOUR);

        return utf8Slice(sign + padStart(hours, 2, '0') + ":" + padStart(minutes, 2, '0'));
    }

    /**
     * Extracts the time zone from a `time(p) with time
     * zone` with a LongTimeWithTimeZone format as a VARCHAR.
     * <p>
     * This function takes a LongTimeWithTimeZone
     * object and extracts the associated time zone offset.
     *
     * @param timeWithTimeZone the LongTimeWithTimeZone object
     * @return the time zone offset in the format
     * "+HH:mm" or "-HH:mm" as a VARCHAR
     */
    @LiteralParameters("p")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice extractTimeZoneFromTimestamp(@SqlType("time(p) with time zone") LongTimeWithTimeZone timeWithTimeZone)
    {
        long totalOffsetMinutes = timeWithTimeZone.getOffsetMinutes();

        char sign = totalOffsetMinutes < 0 ? '-' : '+';
        long absoluteOffsetMinutes = Math.abs(totalOffsetMinutes);

        String hours = Long.toString(absoluteOffsetMinutes / MINUTES_PER_HOUR);
        String minutes = Long.toString(absoluteOffsetMinutes % MINUTES_PER_HOUR);

        return utf8Slice(sign + padStart(hours, 2, '0') + ':' + padStart(minutes, 2, '0'));
    }
}
