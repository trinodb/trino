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
package io.trino.util;

import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import static java.util.concurrent.TimeUnit.DAYS;

/**
 * Pre-optimization Joda-based implementations of year/month/day, registered under
 * {@code legacy_*} names for direct A/B comparison against the FastDate path in
 * {@link BenchmarkDateProjection}.
 */
public final class LegacyJodaDateTimeFunctions
{
    private LegacyJodaDateTimeFunctions() {}

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstanceUTC();
    private static final DateTimeField YEAR = UTC_CHRONOLOGY.year();
    private static final DateTimeField MONTH_OF_YEAR = UTC_CHRONOLOGY.monthOfYear();
    private static final DateTimeField DAY_OF_MONTH = UTC_CHRONOLOGY.dayOfMonth();

    @Description("Year of the given date (legacy Joda)")
    @ScalarFunction(value = "legacy_year", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return YEAR.get(DAYS.toMillis(date));
    }

    @Description("Month of the year of the given date (legacy Joda)")
    @ScalarFunction(value = "legacy_month", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return MONTH_OF_YEAR.get(DAYS.toMillis(date));
    }

    @Description("Day of the month of the given date (legacy Joda)")
    @ScalarFunction(value = "legacy_day", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return DAY_OF_MONTH.get(DAYS.toMillis(date));
    }
}
