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
package io.prestosql.operator.scalar.time;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;

import java.time.LocalDateTime;

import static io.prestosql.spi.type.TimeType.MAX_PRECISION;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_DAY;
import static io.prestosql.type.DateTimes.PICOSECONDS_PER_NANOSECOND;
import static io.prestosql.type.DateTimes.round;

@ScalarFunction(value = "$localtime", hidden = true)
public final class LocalTimeFunction
{
    private LocalTimeFunction() {}

    @LiteralParameters("p")
    @SqlType("time(p)")
    public static long localTime(
            @LiteralParameter("p") long precision,
            ConnectorSession session,
            @SqlNullable @SqlType("time(p)") Long dummy) // need a dummy value since the type inferencer can't bind type arguments exclusively from return type
    {
        long nanos = LocalDateTime.ofInstant(session.getStart(), session.getTimeZoneKey().getZoneId())
                .toLocalTime()
                .toNanoOfDay();

        long picos = nanos * PICOSECONDS_PER_NANOSECOND;
        return round(picos, (int) (MAX_PRECISION - precision)) % PICOSECONDS_PER_DAY;
    }
}
