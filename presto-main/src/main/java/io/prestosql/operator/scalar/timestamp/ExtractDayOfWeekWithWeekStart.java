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

import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.StandardTypes;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.util.Failures.checkCondition;
import static java.lang.Math.floorMod;
import static java.lang.String.format;

@Description("Day of the week of the given timestamp based on start day of the week")
@ScalarFunction(value = "day_of_week", alias = "dow")
public class ExtractDayOfWeekWithWeekStart
{
    private static final int DAYS_IN_WEEK = 7;

    private ExtractDayOfWeekWithWeekStart() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long extract(@SqlType("timestamp(p)") long timestamp, @SqlType(StandardTypes.BIGINT) long weekStart)
    {
        checkCondition(weekStart > 0 && weekStart <= 7, INVALID_FUNCTION_ARGUMENT, format("Invalid start day of the week : %s, valid values are [1-7]", weekStart));
        return floorMod(ExtractDayOfWeek.extract(timestamp) - weekStart, DAYS_IN_WEEK) + 1;
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.BIGINT)
    public static long extract(@SqlType("timestamp(p)") LongTimestamp timestamp, @SqlType(StandardTypes.BIGINT) long weekStart)
    {
        return extract(timestamp.getEpochMicros(), weekStart);
    }
}
