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
package io.prestosql.operator.scalar.timestamptz;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.type.DateTimes;

import java.time.Instant;

import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.MAX_SHORT_PRECISION;
import static io.prestosql.type.DateTimes.NANOSECONDS_PER_MILLISECOND;
import static io.prestosql.type.DateTimes.round;
import static io.prestosql.type.DateTimes.roundToNearest;

@ScalarFunction(value = "$current_timestamp", hidden = true)
public final class CurrentTimestamp
{
    private CurrentTimestamp() {}

    @LiteralParameters("p")
    @SqlType("timestamp(p) with time zone")
    public static long shortTimestamp(
            @LiteralParameter("p") long precision,
            ConnectorSession session,
            @SqlNullable @SqlType("timestamp(p) with time zone") Long dummy) // need a dummy value since the type inferencer can't bind type arguments exclusively from return type
    {
        Instant start = session.getStart();

        long epochMillis = start.toEpochMilli();

        if (precision < MAX_SHORT_PRECISION) {
            epochMillis = round(epochMillis, (int) (MAX_SHORT_PRECISION - precision));
        }
        else {
            long nanosOfMilli = start.getNano() % NANOSECONDS_PER_MILLISECOND;
            if (roundToNearest(nanosOfMilli, NANOSECONDS_PER_MILLISECOND) == NANOSECONDS_PER_MILLISECOND) {
                epochMillis++;
            }
        }

        return packDateTimeWithZone(epochMillis, session.getTimeZoneKey());
    }

    @LiteralParameters("p")
    @SqlType("timestamp(p) with time zone")
    public static LongTimestampWithTimeZone longTimestamp(
            @LiteralParameter("p") long precision,
            ConnectorSession session,
            @SqlNullable @SqlType("timestamp(p) with time zone") LongTimestampWithTimeZone dummy) // need a dummy value since the type inferencer can't bind type arguments exclusively from return type
    {
        Instant start = session.getStart();

        return DateTimes.longTimestampWithTimeZone(precision, start, session.getTimeZoneKey());
    }
}
