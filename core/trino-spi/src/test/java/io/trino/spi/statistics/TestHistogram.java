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

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class TestHistogram
{
    @Test
    public void testEmptyHistogram()
    {
        Histogram histogram = new Histogram(ImmutableList.of());
        assertThat(histogram.getBuckets()).isEmpty();
        assertThat(histogram.estimateFraction(0, 100)).isNaN();
    }

    @Test
    public void testSingleBucketFullRange()
    {
        Histogram histogram = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 100.0, 1000.0, 100.0)));
        assertThat(histogram.estimateFraction(0.0, 100.0)).isCloseTo(1.0, within(1e-9));
    }

    @Test
    public void testSingleBucketHalfRange()
    {
        Histogram histogram = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 100.0, 1000.0, 100.0)));
        assertThat(histogram.estimateFraction(0.0, 50.0)).isCloseTo(0.5, within(1e-9));
    }

    @Test
    public void testMultipleBucketsExactMatch()
    {
        // Three equal buckets: [0,10], [10,20], [20,30], each with 300 rows
        Histogram histogram = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 10.0, 300.0, 10.0),
                new HistogramBucket(10.0, 20.0, 300.0, 10.0),
                new HistogramBucket(20.0, 30.0, 300.0, 10.0)));

        // Query the entire range
        assertThat(histogram.estimateFraction(0.0, 30.0)).isCloseTo(1.0, within(1e-9));

        // Query one bucket exactly
        assertThat(histogram.estimateFraction(0.0, 10.0)).isCloseTo(1.0 / 3, within(1e-9));

        // Query two buckets exactly
        assertThat(histogram.estimateFraction(0.0, 20.0)).isCloseTo(2.0 / 3, within(1e-9));
    }

    @Test
    public void testRangeOutsideHistogram()
    {
        Histogram histogram = new Histogram(ImmutableList.of(
                new HistogramBucket(10.0, 20.0, 1000.0, 50.0)));

        // Query entirely below the histogram
        assertThat(histogram.estimateFraction(0.0, 5.0)).isCloseTo(0.0, within(1e-9));

        // Query entirely above the histogram
        assertThat(histogram.estimateFraction(30.0, 40.0)).isCloseTo(0.0, within(1e-9));
    }

    @Test
    public void testPartialOverlapWithBucket()
    {
        Histogram histogram = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 100.0, 1000.0, 100.0)));

        // Query overlaps the first quarter of the bucket
        assertThat(histogram.estimateFraction(0.0, 25.0)).isCloseTo(0.25, within(1e-9));

        // Query overlaps the last quarter of the bucket
        assertThat(histogram.estimateFraction(75.0, 100.0)).isCloseTo(0.25, within(1e-9));
    }

    @Test
    public void testPointBucket()
    {
        // A single-value bucket at x=5
        Histogram histogram = new Histogram(ImmutableList.of(
                new HistogramBucket(5.0, 5.0, 200.0, 1.0)));

        // The point is within the queried range
        assertThat(histogram.estimateFraction(0.0, 10.0)).isCloseTo(1.0, within(1e-9));

        // The point is outside the queried range
        assertThat(histogram.estimateFraction(6.0, 10.0)).isCloseTo(0.0, within(1e-9));
    }

    @Test
    public void testNaNInputReturnedAsNaN()
    {
        Histogram histogram = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 100.0, 1000.0, 100.0)));

        assertThat(histogram.estimateFraction(Double.NaN, 50.0)).isNaN();
        assertThat(histogram.estimateFraction(0.0, Double.NaN)).isNaN();
    }

    @Test
    public void testInvertedRangeReturnedAsNaN()
    {
        Histogram histogram = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 100.0, 1000.0, 100.0)));

        assertThat(histogram.estimateFraction(50.0, 0.0)).isNaN();
    }

    @Test
    public void testZeroTotalRowCountReturnsZero()
    {
        Histogram histogram = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 100.0, 0.0, 0.0)));
        assertThat(histogram.estimateFraction(0.0, 100.0)).isCloseTo(0.0, within(1e-9));
    }

    @Test
    public void testUnequalBucketWeights()
    {
        // Skewed distribution: bucket [0,50] has 900 rows, bucket [50,100] has 100 rows
        Histogram histogram = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 50.0, 900.0, 50.0),
                new HistogramBucket(50.0, 100.0, 100.0, 50.0)));

        // The first bucket represents 90% of rows
        assertThat(histogram.estimateFraction(0.0, 50.0)).isCloseTo(0.9, within(1e-9));

        // The second bucket represents 10% of rows
        assertThat(histogram.estimateFraction(50.0, 100.0)).isCloseTo(0.1, within(1e-9));
    }

    @Test
    public void testOverlappingBucketsRejected()
    {
        List<HistogramBucket> overlapping = ImmutableList.of(
                new HistogramBucket(0.0, 10.0, 100.0, 5.0),
                new HistogramBucket(5.0, 20.0, 100.0, 5.0));

        assertThatThrownBy(() -> new Histogram(overlapping))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-overlapping");
    }

    @Test
    public void testUnsortedBucketsRejected()
    {
        List<HistogramBucket> unsorted = ImmutableList.of(
                new HistogramBucket(10.0, 20.0, 100.0, 5.0),
                new HistogramBucket(0.0, 5.0, 100.0, 5.0));

        assertThatThrownBy(() -> new Histogram(unsorted))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-overlapping");
    }

    @Test
    public void testEqualsAndHashCode()
    {
        Histogram histogram1 = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 10.0, 100.0, 5.0)));
        Histogram histogram2 = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 10.0, 100.0, 5.0)));
        Histogram histogram3 = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 20.0, 100.0, 5.0)));

        assertThat(histogram1).isEqualTo(histogram2);
        assertThat(histogram1.hashCode()).isEqualTo(histogram2.hashCode());
        assertThat(histogram1).isNotEqualTo(histogram3);
    }

    @Test
    public void testToString()
    {
        Histogram histogram = new Histogram(ImmutableList.of(
                new HistogramBucket(0.0, 10.0, 100.0, 5.0)));
        assertThat(histogram.toString()).contains("Histogram{buckets=");
    }
}
