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
package io.trino.spi.statistics;

import java.util.Objects;

import static java.lang.Double.isNaN;
import static java.lang.String.format;

/**
 * Represents a single bucket in an equi-height histogram.
 * Each bucket covers a contiguous value range [low, high] and stores
 * an estimated row count and distinct values count for that range.
 */
public final class HistogramBucket
{
    private final double low;
    private final double high;
    private final double rowCount;
    private final double distinctValuesCount;

    public HistogramBucket(double low, double high, double rowCount, double distinctValuesCount)
    {
        if (isNaN(low)) {
            throw new IllegalArgumentException("low must not be NaN");
        }
        if (isNaN(high)) {
            throw new IllegalArgumentException("high must not be NaN");
        }
        if (low > high) {
            throw new IllegalArgumentException(format("high must be greater than or equal to low. low: %s. high: %s.", low, high));
        }
        if (isNaN(rowCount) || rowCount < 0) {
            throw new IllegalArgumentException(format("rowCount must be a non-negative finite number: %s", rowCount));
        }
        if (isNaN(distinctValuesCount) || distinctValuesCount < 0) {
            throw new IllegalArgumentException(format("distinctValuesCount must be a non-negative finite number: %s", distinctValuesCount));
        }
        this.low = low;
        this.high = high;
        this.rowCount = rowCount;
        this.distinctValuesCount = distinctValuesCount;
    }

    public double getLow()
    {
        return low;
    }

    public double getHigh()
    {
        return high;
    }

    public double getRowCount()
    {
        return rowCount;
    }

    public double getDistinctValuesCount()
    {
        return distinctValuesCount;
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
        HistogramBucket that = (HistogramBucket) o;
        return Double.compare(low, that.low) == 0 &&
                Double.compare(high, that.high) == 0 &&
                Double.compare(rowCount, that.rowCount) == 0 &&
                Double.compare(distinctValuesCount, that.distinctValuesCount) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(low, high, rowCount, distinctValuesCount);
    }

    @Override
    public String toString()
    {
        return "HistogramBucket{" +
                "low=" + low +
                ", high=" + high +
                ", rowCount=" + rowCount +
                ", distinctValuesCount=" + distinctValuesCount +
                '}';
    }
}
