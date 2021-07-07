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
package io.trino.plugin.base.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.metrics.Count;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class LongCount
        implements Count<LongCount>
{
    private final long total;

    @JsonCreator
    public LongCount(long total)
    {
        this.total = total;
    }

    @JsonProperty("total")
    @Override
    public long getTotal()
    {
        return total;
    }

    @Override
    public LongCount mergeWith(LongCount other)
    {
        return new LongCount(total + other.getTotal());
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
        LongCount count = (LongCount) o;
        return total == count.total;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(total);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("total", total)
                .toString();
    }
}
