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

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.type.DateTimes.MICROSECONDS_PER_DAY;
import static java.lang.Math.floorDiv;

@ScalarOperator(CAST)
@ScalarFunction("date")
public final class TimestampToDateCast
{
    private TimestampToDateCast() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long cast(ConnectorSession session, @SqlType("timestamp(p)") long timestamp)
    {
        return floorDiv(timestamp, MICROSECONDS_PER_DAY);
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.DATE)
    public static long cast(ConnectorSession session, @SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return cast(session, timestamp.getEpochMicros());
    }
}
