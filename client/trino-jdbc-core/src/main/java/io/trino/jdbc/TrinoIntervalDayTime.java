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
package io.trino.jdbc;

import java.util.Objects;

import static io.trino.client.IntervalDayTime.formatMillis;
import static io.trino.client.IntervalDayTime.toMillis;

public class TrinoIntervalDayTime
        implements Comparable<TrinoIntervalDayTime>
{
    private final long milliSeconds;

    public TrinoIntervalDayTime(long milliSeconds)
    {
        this.milliSeconds = milliSeconds;
    }

    public TrinoIntervalDayTime(int day, int hour, int minute, int second, int millis)
    {
        milliSeconds = toMillis(day, hour, minute, second, millis);
    }

    public long getMilliSeconds()
    {
        return milliSeconds;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(milliSeconds);
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
        TrinoIntervalDayTime other = (TrinoIntervalDayTime) obj;
        return this.milliSeconds == other.milliSeconds;
    }

    @Override
    public int compareTo(TrinoIntervalDayTime o)
    {
        return Long.compare(milliSeconds, o.milliSeconds);
    }

    @Override
    public String toString()
    {
        return formatMillis(milliSeconds);
    }
}
