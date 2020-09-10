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
package io.prestosql.type;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.ScalarOperator;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.SUBTRACT;

public final class DateTimeOperators
{
    private static final DateTimeField MILLIS_OF_DAY = ISOChronology.getInstanceUTC().millisOfDay();
    private static final DateTimeField MONTH_OF_YEAR_UTC = ISOChronology.getInstanceUTC().monthOfYear();

    private DateTimeOperators()
    {
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long datePlusIntervalDayToSecond(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        if (MILLIS_OF_DAY.get(interval) != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot add hour, minutes or seconds to a date");
        }
        return date + TimeUnit.MILLISECONDS.toDays(interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long intervalDayToSecondPlusDate(@SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval, @SqlType(StandardTypes.DATE) long date)
    {
        return datePlusIntervalDayToSecond(date, interval);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long datePlusIntervalYearToMonth(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        long millis = MONTH_OF_YEAR_UTC.add(TimeUnit.DAYS.toMillis(date), interval);
        return TimeUnit.MILLISECONDS.toDays(millis);
    }

    @ScalarOperator(ADD)
    @SqlType(StandardTypes.DATE)
    public static long intervalYearToMonthPlusDate(@SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval, @SqlType(StandardTypes.DATE) long date)
    {
        return datePlusIntervalYearToMonth(date, interval);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.DATE)
    public static long dateMinusIntervalDayToSecond(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND) long interval)
    {
        if (MILLIS_OF_DAY.get(interval) != 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot subtract hour, minutes or seconds from a date");
        }
        return date - TimeUnit.MILLISECONDS.toDays(interval);
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.DATE)
    public static long dateMinusIntervalYearToMonth(@SqlType(StandardTypes.DATE) long date, @SqlType(StandardTypes.INTERVAL_YEAR_TO_MONTH) long interval)
    {
        return datePlusIntervalYearToMonth(date, -interval);
    }
}
