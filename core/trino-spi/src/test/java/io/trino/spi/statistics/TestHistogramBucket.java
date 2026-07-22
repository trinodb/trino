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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHistogramBucket
{
    @Test
    public void testValidBucket()
    {
        HistogramBucket bucket = new HistogramBucket(1.0, 5.0, 100.0, 10.0);
        assertThat(bucket.getLow()).isEqualTo(1.0);
        assertThat(bucket.getHigh()).isEqualTo(5.0);
        assertThat(bucket.getRowCount()).isEqualTo(100.0);
        assertThat(bucket.getDistinctValuesCount()).isEqualTo(10.0);
    }

    @Test
    public void testPointBucket()
    {
        HistogramBucket bucket = new HistogramBucket(3.0, 3.0, 50.0, 1.0);
        assertThat(bucket.getLow()).isEqualTo(3.0);
        assertThat(bucket.getHigh()).isEqualTo(3.0);
    }

    @Test
    public void testZeroRowCountAllowed()
    {
        HistogramBucket bucket = new HistogramBucket(0.0, 10.0, 0.0, 0.0);
        assertThat(bucket.getRowCount()).isEqualTo(0.0);
    }

    @Test
    public void testNaNLowRejected()
    {
        assertThatThrownBy(() -> new HistogramBucket(Double.NaN, 5.0, 100.0, 10.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("low must not be NaN");
    }

    @Test
    public void testNaNHighRejected()
    {
        assertThatThrownBy(() -> new HistogramBucket(1.0, Double.NaN, 100.0, 10.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("high must not be NaN");
    }

    @Test
    public void testLowGreaterThanHighRejected()
    {
        assertThatThrownBy(() -> new HistogramBucket(5.0, 1.0, 100.0, 10.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("high must be greater than or equal to low");
    }

    @Test
    public void testNegativeRowCountRejected()
    {
        assertThatThrownBy(() -> new HistogramBucket(1.0, 5.0, -1.0, 10.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowCount must be a non-negative finite number");
    }

    @Test
    public void testNaNRowCountRejected()
    {
        assertThatThrownBy(() -> new HistogramBucket(1.0, 5.0, Double.NaN, 10.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowCount must be a non-negative finite number");
    }

    @Test
    public void testNegativeDistinctValuesCountRejected()
    {
        assertThatThrownBy(() -> new HistogramBucket(1.0, 5.0, 100.0, -1.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("distinctValuesCount must be a non-negative finite number");
    }

    @Test
    public void testEqualsAndHashCode()
    {
        HistogramBucket bucket1 = new HistogramBucket(1.0, 5.0, 100.0, 10.0);
        HistogramBucket bucket2 = new HistogramBucket(1.0, 5.0, 100.0, 10.0);
        HistogramBucket bucket3 = new HistogramBucket(2.0, 5.0, 100.0, 10.0);

        assertThat(bucket1).isEqualTo(bucket2);
        assertThat(bucket1.hashCode()).isEqualTo(bucket2.hashCode());
        assertThat(bucket1).isNotEqualTo(bucket3);
    }

    @Test
    public void testToString()
    {
        HistogramBucket bucket = new HistogramBucket(1.0, 5.0, 100.0, 10.0);
        assertThat(bucket.toString())
                .contains("low=1.0")
                .contains("high=5.0")
                .contains("rowCount=100.0")
                .contains("distinctValuesCount=10.0");
    }
}
