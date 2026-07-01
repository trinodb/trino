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
package io.trino.type;

import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.concurrent.TimeUnit;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.SUBTRACT;

public final class DateTimeOperators
{
    private static final DateTimeField MONTH_OF_YEAR_UTC = ISOChronology.getInstanceUTC().monthOfYear();

    private DateTimeOperators() {}

    @ScalarOperator(ADD)
    @LiteralParameters("q")
    @SqlType(StandardTypes.DATE)
    public static long datePlusIntervalDayToSecond(@SqlType(StandardTypes.DATE) long date, @SqlType("interval day(q) to second") long interval)
    {
        if (interval % TimeUnit.DAYS.toMicros(1) != 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot add hour, minutes or seconds to a date");
        }
        return date + TimeUnit.MICROSECONDS.toDays(interval);
    }

    @ScalarOperator(ADD)
    @LiteralParameters("q")
    @SqlType(StandardTypes.DATE)
    public static long intervalDayToSecondPlusDate(@SqlType("interval day(q) to second") long interval, @SqlType(StandardTypes.DATE) long date)
    {
        return datePlusIntervalDayToSecond(date, interval);
    }

    @ScalarOperator(ADD)
    @LiteralParameters("q")
    @SqlType(StandardTypes.DATE)
    public static long datePlusIntervalYearToMonth(@SqlType(StandardTypes.DATE) long date, @SqlType("interval year(q) to month") long interval)
    {
        long millis = MONTH_OF_YEAR_UTC.add(TimeUnit.DAYS.toMillis(date), interval);
        return TimeUnit.MILLISECONDS.toDays(millis);
    }

    @ScalarOperator(ADD)
    @LiteralParameters("q")
    @SqlType(StandardTypes.DATE)
    public static long intervalYearToMonthPlusDate(@SqlType("interval year(q) to month") long interval, @SqlType(StandardTypes.DATE) long date)
    {
        return datePlusIntervalYearToMonth(date, interval);
    }

    @ScalarOperator(SUBTRACT)
    @LiteralParameters("q")
    @SqlType(StandardTypes.DATE)
    public static long dateMinusIntervalDayToSecond(@SqlType(StandardTypes.DATE) long date, @SqlType("interval day(q) to second") long interval)
    {
        if (interval % TimeUnit.DAYS.toMicros(1) != 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Cannot subtract hour, minutes or seconds from a date");
        }
        return date - TimeUnit.MICROSECONDS.toDays(interval);
    }

    @ScalarOperator(SUBTRACT)
    @LiteralParameters("q")
    @SqlType(StandardTypes.DATE)
    public static long dateMinusIntervalYearToMonth(@SqlType(StandardTypes.DATE) long date, @SqlType("interval year(q) to month") long interval)
    {
        return datePlusIntervalYearToMonth(date, -interval);
    }
}
