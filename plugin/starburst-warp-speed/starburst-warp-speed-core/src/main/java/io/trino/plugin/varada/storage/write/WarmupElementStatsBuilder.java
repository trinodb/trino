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

import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;

public class WarmupElementStatsBuilder
{
    private boolean initialized;
    private int nullsCount;
    private Object maxValue;
    private Object minValue;

    public WarmupElementStatsBuilder(WarmupElementStats warmupElementStats)
    {
        this(warmupElementStats.isInitialized(), warmupElementStats.getNullsCount(), warmupElementStats.getMinValue(), warmupElementStats.getMaxValue());
    }

    public WarmupElementStatsBuilder()
    {
        this(false, 0, null, null);
    }

    private WarmupElementStatsBuilder(
            boolean initialized,
            int nullsCount,
            Object minValue,
            Object maxValue)
    {
        this.initialized = initialized;
        this.nullsCount = nullsCount;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public WarmupElementStats build()
    {
        return new WarmupElementStats(initialized, nullsCount, minValue, maxValue);
    }

    public void incNullCount(int nullsCount)
    {
        this.nullsCount += nullsCount;
    }

    public void updateMinMax(long val)
    {
        if (initialized) {
            this.minValue = Math.min((long) this.minValue, val);
            this.maxValue = Math.max((long) this.maxValue, val);
        }
        else {
            initialize(val);
        }
    }

    public void updateMinMax(short val)
    {
        if (initialized) {
            this.minValue = (short) Math.min((short) this.minValue, val);
            this.maxValue = (short) Math.max((short) this.maxValue, val);
        }
        else {
            initialize(val);
        }
    }

    public void updateMinMax(int val)
    {
        if (initialized) {
            this.minValue = Math.min((int) this.minValue, val);
            this.maxValue = Math.max((int) this.maxValue, val);
        }
        else {
            initialize(val);
        }
    }

    public void updateMinMax(Double val)
    {
        if (val.isNaN()) {
            return;
        }
        if (initialized) {
            this.minValue = Math.min((double) this.minValue, val);
            this.maxValue = Math.max((double) this.maxValue, val);
        }
        else {
            initialize(val);
        }
    }

    public void updateMinMax(Float val)
    {
        if (val.isNaN()) {
            return;
        }
        if (initialized) {
            this.minValue = Math.min((float) this.minValue, val);
            this.maxValue = Math.max((float) this.maxValue, val);
        }
        else {
            initialize(val);
        }
    }

    public void updateMinMax(byte val)
    {
        if (initialized) {
            this.minValue = (byte) Math.min((byte) this.minValue, val);
            this.maxValue = (byte) Math.max((byte) this.maxValue, val);
        }
        else {
            initialize(val);
        }
    }

    public void updateMinMax(Slice val)
    {
        if (initialized) {
            if (val.compareTo(((Slice) this.minValue)) < 0) {
                this.minValue = val;
            }
            if (val.compareTo(((Slice) this.maxValue)) > 0) {
                this.maxValue = val;
            }
        }
        else {
            initialize(val);
        }
    }

    private void initialize(Object val)
    {
        checkArgument(val != null, "adding min/max value must be non-null");
        this.minValue = val;
        this.maxValue = val;
        initialized = true;
    }
}
