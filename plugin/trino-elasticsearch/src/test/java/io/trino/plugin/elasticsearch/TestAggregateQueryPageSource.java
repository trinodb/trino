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
package io.trino.plugin.elasticsearch;

import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAggregateQueryPageSource
{
    @Test
    public void testExtractSingleValueReturnsNullForInfiniteValue()
    {
        assertThat(AggregateQueryPageSource.extractSingleValue(new TestingSingleValue(Double.POSITIVE_INFINITY))).isNull();
    }

    @Test
    public void testExtractSingleValueReturnsFiniteValue()
    {
        assertThat(AggregateQueryPageSource.extractSingleValue(new TestingSingleValue(42.5))).isEqualTo(42.5);
    }

    @Test
    public void testExtractSumFromStatsValueReturnsNullWhenStatsHasNoValues()
    {
        assertThat(AggregateQueryPageSource.extractSumFromStatsValue(new TestingStats(Double.POSITIVE_INFINITY, 0.0))).isNull();
    }

    @Test
    public void testExtractSumFromStatsValueReturnsSumForNonEmptyStats()
    {
        assertThat(AggregateQueryPageSource.extractSumFromStatsValue(new TestingStats(10.0, 30.0))).isEqualTo(30.0);
    }

    private abstract static class TestingAggregation
            implements Aggregation
    {
        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public String getType()
        {
            return "test";
        }

        @Override
        public Map<String, Object> getMetadata()
        {
            return Map.of();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params)
                throws IOException
        {
            return builder;
        }
    }

    private static final class TestingSingleValue
            extends TestingAggregation
            implements NumericMetricsAggregation.SingleValue
    {
        private final double value;

        private TestingSingleValue(double value)
        {
            this.value = value;
        }

        @Override
        public double value()
        {
            return value;
        }

        @Override
        public String getValueAsString()
        {
            return Double.toString(value);
        }
    }

    private static final class TestingStats
            extends TestingAggregation
            implements Stats
    {
        private final double min;
        private final double sum;

        private TestingStats(double min, double sum)
        {
            this.min = min;
            this.sum = sum;
        }

        @Override
        public long getCount()
        {
            return Double.isInfinite(min) ? 0 : 1;
        }

        @Override
        public double getMin()
        {
            return min;
        }

        @Override
        public double getMax()
        {
            return min;
        }

        @Override
        public double getAvg()
        {
            return sum;
        }

        @Override
        public double getSum()
        {
            return sum;
        }

        @Override
        public String getMinAsString()
        {
            return Double.toString(min);
        }

        @Override
        public String getMaxAsString()
        {
            return Double.toString(min);
        }

        @Override
        public String getAvgAsString()
        {
            return Double.toString(sum);
        }

        @Override
        public String getSumAsString()
        {
            return Double.toString(sum);
        }

        @Override
        public Iterable<String> valueNames()
        {
            return List.of("min", "max", "avg", "sum");
        }

        @Override
        public double value(String name)
        {
            return switch (name) {
                case "min", "max" -> min;
                case "avg", "sum" -> sum;
                default -> throw new IllegalArgumentException("Unknown value name: " + name);
            };
        }
    }
}
