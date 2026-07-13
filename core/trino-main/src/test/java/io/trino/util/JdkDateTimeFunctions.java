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

import java.time.LocalDate;

/**
 * JDK {@link java.time.LocalDate}-based implementations of year/month/day, registered under
 * {@code jdk_*} names for direct A/B comparison against Joda and FastDate paths in
 * {@link BenchmarkDateProjection}.
 */
public final class JdkDateTimeFunctions
{
    private JdkDateTimeFunctions() {}

    @Description("Year of the given date (JDK java.time)")
    @ScalarFunction(value = "jdk_year", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long yearFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return LocalDate.ofEpochDay(date).getYear();
    }

    @Description("Month of the year of the given date (JDK java.time)")
    @ScalarFunction(value = "jdk_month", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long monthFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return LocalDate.ofEpochDay(date).getMonthValue();
    }

    @Description("Day of the month of the given date (JDK java.time)")
    @ScalarFunction(value = "jdk_day", neverFails = true)
    @SqlType(StandardTypes.BIGINT)
    public static long dayFromDate(@SqlType(StandardTypes.DATE) long date)
    {
        return LocalDate.ofEpochDay(date).getDayOfMonth();
    }
}
