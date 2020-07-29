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
package io.prestosql.spi.type;

import java.util.Objects;

import static io.prestosql.spi.type.TimeWithTimezoneTypes.normalize;

public final class LongTimeWithTimeZone
        implements Comparable<LongTimeWithTimeZone>
{
    private final long picoSeconds;
    private final int offsetMinutes;

    public LongTimeWithTimeZone(long picoSeconds, int offsetMinutes)
    {
        this.picoSeconds = picoSeconds;
        this.offsetMinutes = offsetMinutes;
    }

    public long getPicoSeconds()
    {
        return picoSeconds;
    }

    public int getOffsetMinutes()
    {
        return offsetMinutes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LongTimeWithTimeZone that = (LongTimeWithTimeZone) o;
        return picoSeconds == that.picoSeconds && offsetMinutes == that.offsetMinutes;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(picoSeconds, offsetMinutes);
    }

    @Override
    public int compareTo(LongTimeWithTimeZone other)
    {
        return Long.compare(normalize(this), normalize(other));
    }
}
