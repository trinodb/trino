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
package io.trino.client.spooling;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;

import static java.lang.String.format;

public final class InlineSegment
        extends Segment
{
    private final byte[] data;

    public InlineSegment(@JsonProperty("data") byte[] data, @JsonProperty("metadata") DataAttributes metadata)
    {
        super(metadata);
        this.data = data;
    }

    @JsonProperty("data")
    public byte[] getData()
    {
        return data;
    }

    @Override
    public String toString()
    {
        return format("InlineSegment{offset=%d, rows=%d, size=%d}", getOffset(), getRowsCount(), getSegmentSize());
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InlineSegment segment = (InlineSegment) o;
        return Arrays.equals(data, segment.data)
                && Objects.equals(getMetadata(), segment.getMetadata());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getMetadata(), Arrays.hashCode(data));
    }
}
