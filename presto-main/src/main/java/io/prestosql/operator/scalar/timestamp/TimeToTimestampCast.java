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

import io.prestosql.spi.function.LiteralParameter;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.type.Timestamps.round;
import static io.prestosql.type.Timestamps.scaleEpochMillisToMicros;

@ScalarOperator(CAST)
public final class TimeToTimestampCast
{
    private TimeToTimestampCast() {}

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static long cast(@LiteralParameter("p") long precision, @SqlType(StandardTypes.TIME) long time)
    {
        if (precision > 3) {
            return scaleEpochMillisToMicros(time);
        }

        if (precision < 3) {
            // time is implicitly time(3), so truncate to expected precision
            return round(time, (int) (3 - precision));
        }

        return time;
    }

    @LiteralParameters("p")
    @SqlType("timestamp(p)")
    public static LongTimestamp cast(@SqlType(StandardTypes.TIME) long time)
    {
        return new LongTimestamp(cast(6, time), 0);
    }
}
