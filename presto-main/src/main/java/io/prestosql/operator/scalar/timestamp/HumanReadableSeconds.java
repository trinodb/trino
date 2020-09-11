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

import com.google.common.base.Strings;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;
import static org.joda.time.DateTimeConstants.SECONDS_PER_DAY;
import static org.joda.time.DateTimeConstants.SECONDS_PER_HOUR;
import static org.joda.time.DateTimeConstants.SECONDS_PER_MINUTE;

@ScalarFunction("human_readable_seconds")
public final class HumanReadableSeconds
{
    private static final int DAYS_IN_WEEK = 7;
    private static final int SECONDS_IN_WEEK = SECONDS_PER_DAY * DAYS_IN_WEEK;
    private static final String WEEK = "week";
    private static final String DAY = "day";
    private static final String HOUR = "hour";
    private static final String MINUTE = "minute";
    private static final String SECOND = "second";

    private HumanReadableSeconds() {}

    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice humanReadableSeconds(@SqlType(StandardTypes.DOUBLE) double inputSeconds)
    {
        if (Double.isNaN(inputSeconds) || Double.isInfinite(inputSeconds)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Invalid argument found: %s", inputSeconds));
        }

        long seconds = Math.round(Math.abs(inputSeconds));

        long weeks = TimeUnit.SECONDS.toDays(seconds) / DAYS_IN_WEEK;
        seconds = seconds % SECONDS_IN_WEEK;

        long days = TimeUnit.SECONDS.toDays(seconds);
        seconds = seconds % SECONDS_PER_DAY;

        long hours = TimeUnit.SECONDS.toHours(seconds);
        seconds = seconds % SECONDS_PER_HOUR;

        long minutes = TimeUnit.SECONDS.toMinutes(seconds);
        seconds = seconds % SECONDS_PER_MINUTE;

        return getTimePeriods(weeks, days, hours, minutes, seconds);
    }

    private static Slice getTimePeriods(long weeks, long days, long hours, long minutes, long seconds)
    {
        StringJoiner stringJoiner = new StringJoiner(", ");

        if (weeks > 0) {
            stringJoiner.add(renderPeriodType(weeks, WEEK));
        }
        if (days > 0) {
            stringJoiner.add(renderPeriodType(days, DAY));
        }
        if (hours > 0) {
            stringJoiner.add(renderPeriodType(hours, HOUR));
        }
        if (minutes > 0) {
            stringJoiner.add(renderPeriodType(minutes, MINUTE));
        }
        if (seconds > 0) {
            stringJoiner.add(renderPeriodType(seconds, SECOND));
        }

        String timePeriod = stringJoiner.toString();
        if (Strings.isNullOrEmpty(timePeriod)) {
            return utf8Slice(renderPeriodType(0, SECOND));
        }
        return utf8Slice(timePeriod);
    }

    private static String renderPeriodType(long value, String periodType)
    {
        if (value == 1) {
            return value + " " + periodType;
        }
        return value + " " + periodType + "s";
    }
}
