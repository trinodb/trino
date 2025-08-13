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
package io.trino.lance.file.v2.reader;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public record Range(long start, long end)
{
    public Range
    {
        checkArgument(start >= 0, "start must be greater than or equal to zero");
        checkArgument(end >= start, "end must be greater start");
    }

    public static Range of(long start, long end)
    {
        return new Range(start, end);
    }

    public long length()
    {
        return end - start;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(start, end);
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
        Range other = (Range) obj;
        return start == other.start && end == other.end;
    }
}
