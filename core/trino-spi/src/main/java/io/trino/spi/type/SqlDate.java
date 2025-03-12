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
package io.trino.spi.type;

import java.time.LocalDate;

public final class SqlDate
{
    private final int days;

    // TODO accept long
    public SqlDate(int days)
    {
        this.days = days;
    }

    public int getDays()
    {
        return days;
    }

    @Override
    public int hashCode()
    {
        return days;
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
        SqlDate other = (SqlDate) obj;
        return days == other.days;
    }

    @Override
    public String toString()
    {
        return LocalDate.ofEpochDay(days).toString();
    }
}
