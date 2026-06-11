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

import java.util.List;
import java.util.Objects;

import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * An equi-height histogram for a column, where each bucket covers a value range
 * and contains a roughly equal number of rows. Buckets must be non-overlapping
 * and ordered by lower bound.
 * <p>
 * The histogram provides an {@link #estimateFraction(double, double)} method
 * that can be used by the query optimizer to estimate the selectivity of range
 * and equality predicates more accurately than the uniform-distribution assumption.
 */
public final class Histogram
{
    private final List<HistogramBucket> buckets;

    /**
     * Creates a histogram from the given list of buckets.
     *
     * @param buckets a non-null list of buckets that must be sorted by lower bound
     *                and must not overlap (each bucket's high must be less than or
     *                equal to the next bucket's low)
     */
    public Histogram(List<HistogramBucket> buckets)
    {
        requireNonNull(buckets, "buckets is null");
        this.buckets = List.copyOf(buckets);
        for (int i = 1; i < this.buckets.size(); i++) {
            HistogramBucket previous = this.buckets.get(i - 1);
            HistogramBucket current = this.buckets.get(i);
            if (previous.getHigh() > current.getLow()) {
                throw new IllegalArgumentException(format(
                        "Histogram buckets must be non-overlapping and sorted by lower bound. " +
                                "Bucket %d [%s, %s] overlaps bucket %d [%s, %s]",
                        i - 1, previous.getLow(), previous.getHigh(),
                        i, current.getLow(), current.getHigh()));
            }
        }
    }

    public List<HistogramBucket> getBuckets()
    {
        return buckets;
    }

    /**
     * Estimates the fraction of non-null rows whose values fall within [low, high].
     * Uses linear interpolation within each bucket (uniform distribution assumption
     * within a single bucket).
     *
     * @return a value in [0.0, 1.0], or {@link Double#NaN} if the histogram is empty
     *         or total row count is zero
     */
    public double estimateFraction(double low, double high)
    {
        if (isNaN(low) || isNaN(high) || low > high) {
            return Double.NaN;
        }
        if (buckets.isEmpty()) {
            return Double.NaN;
        }

        double totalRowCount = 0;
        for (HistogramBucket bucket : buckets) {
            totalRowCount += bucket.getRowCount();
        }
        if (totalRowCount == 0) {
            return 0.0;
        }

        double matchingRowCount = 0;
        for (HistogramBucket bucket : buckets) {
            matchingRowCount += rowsInRange(bucket, low, high);
        }
        return matchingRowCount / totalRowCount;
    }

    private static double rowsInRange(HistogramBucket bucket, double low, double high)
    {
        double bucketLow = bucket.getLow();
        double bucketHigh = bucket.getHigh();

        // No overlap
        if (low > bucketHigh || high < bucketLow) {
            return 0.0;
        }
        // Bucket fully covered by the queried range
        if (low <= bucketLow && high >= bucketHigh) {
            return bucket.getRowCount();
        }
        // Point bucket (single value): included if the value falls within [low, high]
        if (bucketLow == bucketHigh) {
            return bucket.getRowCount();
        }
        // Partial overlap: interpolate assuming uniform distribution within the bucket
        double overlapFraction = (min(high, bucketHigh) - max(low, bucketLow)) / (bucketHigh - bucketLow);
        return bucket.getRowCount() * overlapFraction;
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
        Histogram that = (Histogram) o;
        return Objects.equals(buckets, that.buckets);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(buckets);
    }

    @Override
    public String toString()
    {
        return "Histogram{buckets=" + buckets + '}';
    }
}
