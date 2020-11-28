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
package io.prestosql.plugin.kudu.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;

public class KuduRangePartition
{
    private final byte[] rangeKeyStart;
    private final byte[] rangeKeyEnd;

    @JsonCreator
    public KuduRangePartition(byte[] rangeKeyStart, byte[] rangeKeyEnd)
    {
        this.rangeKeyStart = rangeKeyStart;
        this.rangeKeyEnd = rangeKeyEnd;
    }

    @JsonProperty
    public byte[] getRangeKeyStart()
    {
        return rangeKeyStart;
    }

    @JsonProperty
    public byte[] getRangeKeyEnd()
    {
        return rangeKeyEnd;
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
        KuduRangePartition that = (KuduRangePartition) o;
        return Arrays.equals(rangeKeyStart, that.rangeKeyStart)
                && Arrays.equals(rangeKeyEnd, that.rangeKeyEnd);
    }

    @Override
    public int hashCode()
    {
        int result = Arrays.hashCode(rangeKeyStart);
        result = 31 * result + Arrays.hashCode(rangeKeyEnd);
        return result;
    }
}
