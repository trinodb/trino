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
package io.trino.plugin.varada.storage.write;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class WarmupElementStats
{
    public static final WarmupElementStats UNINITIALIZED = new WarmupElementStats(false, 0, null, null);

    private final boolean initialized;
    private final int nullsCount;
    private final Object maxValue;
    private final Object minValue;

    public WarmupElementStats(int nullsCount, Object minValue, Object maxValue)
    {
        this(true, nullsCount, minValue, maxValue);
    }

    @JsonCreator
    public WarmupElementStats(
            @JsonProperty("initialized") boolean initialized,
            @JsonProperty("nullsCount") int nullsCount,
            @JsonProperty("minValue") Object minValue,
            @JsonProperty("maxValue") Object maxValue)
    {
        this.initialized = initialized;
        this.nullsCount = nullsCount;
        this.maxValue = maxValue;
        this.minValue = minValue;
    }

    @JsonProperty("initialized")
    public boolean isInitialized()
    {
        return initialized;
    }

    @JsonProperty("nullsCount")
    public int getNullsCount()
    {
        return nullsCount;
    }

    @JsonProperty("minValue")
    public Object getMinValue()
    {
        return minValue;
    }

    @JsonProperty("maxValue")
    public Object getMaxValue()
    {
        return maxValue;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (WarmupElementStats) obj;
        return this.initialized == that.initialized &&
                this.nullsCount == that.nullsCount &&
                Objects.equals(this.maxValue, that.maxValue) &&
                Objects.equals(this.minValue, that.minValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(initialized, nullsCount, maxValue, minValue);
    }

    @Override
    public String toString()
    {
        return "WarmupElementStats[" +
                "initialized=" + initialized + ", " +
                "nullsCount=" + nullsCount + ", " +
                "maxValue=" + maxValue + ", " +
                "minValue=" + minValue + ']';
    }
}
