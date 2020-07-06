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
import io.prestosql.operator.scalar.DateTimeFunctions;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;

import static io.prestosql.type.Timestamps.getMicrosOfMilli;
import static io.prestosql.type.Timestamps.round;
import static io.prestosql.type.Timestamps.scaleEpochMicrosToMillis;
import static io.prestosql.type.Timestamps.scaleEpochMillisToMicros;
import static io.prestosql.util.DateTimeZoneIndex.getChronology;
import static java.lang.Math.toIntExact;

@Description("Add the specified amount of time to the given timestamp")
@ScalarFunction("date_add")
public class DateAdd
{
    private DateAdd() {}

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static long add(
            @LiteralParameter("p") long precision,
            ConnectorSession session,
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("timestamp(p)") long timestamp)
    {
        long epochMillis = timestamp;
        int microFraction = 0;
        if (precision > 3) {
            epochMillis = scaleEpochMicrosToMillis(timestamp);
            microFraction = getMicrosOfMilli(timestamp);
        }

        long result;
        if (session.isLegacyTimestamp()) {
            result = DateTimeFunctions.getTimestampField(getChronology(session.getTimeZoneKey()), unit).add(epochMillis, toIntExact(value));
        }
        else {
            result = DateTimeFunctions.getTimestampField(ISOChronology.getInstanceUTC(), unit).add(epochMillis, toIntExact(value));
        }

        if (precision <= 3) {
            return round(result, (int) (3 - precision));
        }

        return scaleEpochMillisToMicros(result) + microFraction;
    }

    @LiteralParameters({"x", "p"})
    @SqlType("timestamp(p)")
    public static LongTimestamp add(
            ConnectorSession session,
            @SqlType("varchar(x)") Slice unit,
            @SqlType(StandardTypes.BIGINT) long value,
            @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return new LongTimestamp(
                add(6, session, unit, value, timestamp.getEpochMicros()),
                timestamp.getPicosOfMicro());
    }
}
