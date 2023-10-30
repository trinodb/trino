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

public class BooleanCount
        implements Count<BooleanCount>
{
    private final boolean value;

    @JsonCreator
    public BooleanCount(boolean value)
    {
        this.value = value;
    }

    @JsonProperty("value")
    @Override
    public long getTotal()
    {
        return value ? 1L : 0L;
    }

    @Override
    public BooleanCount mergeWith(BooleanCount other)
    {
        return new BooleanCount(value || other.value);
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
        BooleanCount count = (BooleanCount) o;
        return value == count.value;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    @Override
    public String toString()
    {
        return Boolean.toString(value);
    }
}
