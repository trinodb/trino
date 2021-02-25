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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.elasticsearch.aggregation.MetricAggregation;
import io.trino.plugin.elasticsearch.aggregation.TermAggregation;
import io.trino.spi.type.RealType;
import io.trino.spi.type.VarcharType;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestElasticsearchQueryBuilder
{
    private final List<ElasticsearchColumnHandle> columns =
            ImmutableList.of(new ElasticsearchColumnHandle("hostname", VarcharType.VARCHAR, true),
                    new ElasticsearchColumnHandle("total", RealType.REAL, true),
                    new ElasticsearchColumnHandle("values", RealType.REAL, true));
    private final MetricAggregation metricAggregation1 =
            new MetricAggregation("avg", RealType.REAL, Optional.of(columns.get(2)), "avg_values");
    private final MetricAggregation metricAggregation2 =
            new MetricAggregation("max", RealType.REAL, Optional.of(columns.get(2)), "max_values");
    private final MetricAggregation metricAggregation3 =
            new MetricAggregation("min", RealType.REAL, Optional.of(columns.get(2)), "min_values");
    private final MetricAggregation metricAggregation4 =
            new MetricAggregation("sum", RealType.REAL, Optional.of(columns.get(2)), "sum_values");
    private final MetricAggregation metricAggregation5 =
            new MetricAggregation("count", RealType.REAL, Optional.of(columns.get(2)), "count_values");
    private final MetricAggregation metricAggregation6 =
            new MetricAggregation("count", RealType.REAL, Optional.empty(), "count_all");

    @Test
    public void testBuildAggregationQuery()
    {
        TermAggregation termAggregation1 = TermAggregation.fromColumnHandle(columns.get(0));
        TermAggregation termAggregation2 = TermAggregation.fromColumnHandle(columns.get(1));
        List<AggregationBuilder> aggBuilder =
                ElasticsearchQueryBuilder.buildAggregationQuery(
                        ImmutableList.of(termAggregation1, termAggregation2),
                        ImmutableList.of(
                                metricAggregation1, metricAggregation2, metricAggregation3, metricAggregation4, metricAggregation5, metricAggregation6));
        assertThat(aggBuilder).hasSize(1);
        assertThat(aggBuilder.get(0)).isExactlyInstanceOf(TermsAggregationBuilder.class).hasFieldOrPropertyWithValue("field", "hostname");
        List<AggregationBuilder> sub1 = aggBuilder.get(0).getSubAggregations();
        assertThat(sub1).hasSize(1);
        assertThat(sub1.get(0)).isExactlyInstanceOf(TermsAggregationBuilder.class).hasFieldOrPropertyWithValue("field", "total");
        List<AggregationBuilder> subaggs = sub1.get(0).getSubAggregations();
        assertAllAgg(subaggs);
    }

    @Test
    public void testBuildAggregationQueryWithEmptyTerm()
    {
        List<AggregationBuilder> empty1 =
                ElasticsearchQueryBuilder.buildAggregationQuery(null, ImmutableList.of(
                        metricAggregation1, metricAggregation2, metricAggregation3, metricAggregation4, metricAggregation5, metricAggregation6));
        assertAllAgg(empty1);
    }

    @Test
    public void testBuildAggregationQueryWithEmptyAggregation()
    {
        TermAggregation termAggregation1 = TermAggregation.fromColumnHandle(columns.get(0));
        List<AggregationBuilder> aggBuilder =
                ElasticsearchQueryBuilder.buildAggregationQuery(ImmutableList.of(termAggregation1), Collections.emptyList());
        assertThat(aggBuilder).hasSize(1);
        assertThat(aggBuilder.get(0)).isExactlyInstanceOf(TermsAggregationBuilder.class).hasFieldOrPropertyWithValue("field", "hostname");
        List<AggregationBuilder> sub1 = aggBuilder.get(0).getSubAggregations();
        assertThat(sub1).hasSize(0);
    }

    private void assertAllAgg(List<AggregationBuilder> allAggs)
    {
        assertThat(allAggs).hasSize(6);
        assertThat(allAggs.get(0)).isExactlyInstanceOf(AvgAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", "values")
                .hasFieldOrPropertyWithValue("name", "avg_values");
        assertThat(allAggs.get(1)).isExactlyInstanceOf(MaxAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", "values")
                .hasFieldOrPropertyWithValue("name", "max_values");
        assertThat(allAggs.get(2)).isExactlyInstanceOf(MinAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", "values")
                .hasFieldOrPropertyWithValue("name", "min_values");
        assertThat(allAggs.get(3)).isExactlyInstanceOf(SumAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", "values")
                .hasFieldOrPropertyWithValue("name", "sum_values");
        assertThat(allAggs.get(4)).isExactlyInstanceOf(ValueCountAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", "values")
                .hasFieldOrPropertyWithValue("name", "count_values");
        assertThat(allAggs.get(5)).isExactlyInstanceOf(ValueCountAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", "_id")
                .hasFieldOrPropertyWithValue("name", "count_all");
    }
}
