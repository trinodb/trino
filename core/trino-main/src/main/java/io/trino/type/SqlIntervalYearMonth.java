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

import java.util.Objects;

import static io.trino.client.IntervalYearMonth.formatMonths;
import static io.trino.client.IntervalYearMonth.toMonths;

public class SqlIntervalYearMonth
{
    private final int months;

    public SqlIntervalYearMonth(int months)
    {
        this.months = months;
    }

    public SqlIntervalYearMonth(int year, int months)
    {
        this.months = toMonths(year, months);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(months);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SqlIntervalYearMonth other = (SqlIntervalYearMonth) obj;
        return this.months == other.months;
    }

    @Override
    public String toString()
    {
        return formatMonths(months);
    }

    public int getMonths()
    {
        return months;
    }
}
