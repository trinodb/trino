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
import io.airlift.slice.Slice;

import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class WarmupElementStats
{
    private boolean firstTime;
    private int nullsCount;
    private Object maxValue;
    private Object minValue;

    @JsonCreator
    public WarmupElementStats(@JsonProperty("nullsCount") int nullsCount,
            @JsonProperty("minValue") Object minValue,
            @JsonProperty("maxValue") Object maxValue)
    {
        this.nullsCount = nullsCount;
        this.maxValue = maxValue;
        this.minValue = minValue;
        this.firstTime = true;
    }

    public void updateMinMax(long val)
    {
        if (firstTime) {
            handleFirstTime(val);
        }
        else {
            this.minValue = Math.min((long) this.minValue, val);
            this.maxValue = Math.max((long) this.maxValue, val);
        }
    }

    public void updateMinMax(short val)
    {
        if (firstTime) {
            handleFirstTime(val);
        }
        else {
            this.minValue = (short) Math.min((short) this.minValue, val);
            this.maxValue = (short) Math.max((short) this.maxValue, val);
        }
    }

    public void updateMinMax(int val)
    {
        if (firstTime) {
            handleFirstTime(val);
        }
        else {
            this.minValue = Math.min((int) this.minValue, val);
            this.maxValue = Math.max((int) this.maxValue, val);
        }
    }

    public void updateMinMax(Double val)
    {
        if (val.isNaN()) {
            return;
        }
        if (firstTime) {
            handleFirstTime(val);
        }
        else {
            this.minValue = Math.min((double) this.minValue, val);
            this.maxValue = Math.max((double) this.maxValue, val);
        }
    }

    public void updateMinMax(Float val)
    {
        if (val.isNaN()) {
            return;
        }
        if (firstTime) {
            handleFirstTime(val);
        }
        else {
            this.minValue = Math.min((float) this.minValue, val);
            this.maxValue = Math.max((float) this.maxValue, val);
        }
    }

    public void updateMinMax(byte val)
    {
        if (firstTime) {
            handleFirstTime(val);
        }
        else {
            this.minValue = (byte) Math.min((byte) this.minValue, val);
            this.maxValue = (byte) Math.max((byte) this.maxValue, val);
        }
    }

    public void updateMinMax(Slice val)
    {
        if (firstTime) {
            handleFirstTime(val);
        }
        else {
            if (val.compareTo(((Slice) this.minValue)) < 0) {
                this.minValue = val;
            }
            if (val.compareTo(((Slice) this.maxValue)) > 0) {
                this.maxValue = val;
            }
        }
    }

    private void handleFirstTime(Object val)
    {
        checkArgument(val != null, "adding min/max value must be non-null");
        this.minValue = val;
        this.maxValue = val;
        firstTime = false;
    }

    public void incNullCount(int nullsCount)
    {
        this.nullsCount += nullsCount;
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

    public boolean isValidRange()
    {
        //case all values are null it never init
        return maxValue != null && minValue != null;
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
        return this.nullsCount == that.nullsCount &&
                Objects.equals(this.maxValue, that.maxValue) &&
                Objects.equals(this.minValue, that.minValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nullsCount, maxValue, minValue);
    }

    @Override
    public String toString()
    {
        return "WarmupElementStats[" +
                "nullsCount=" + nullsCount + ", " +
                "maxValue=" + maxValue + ", " +
                "minValue=" + minValue + ']';
    }
}
