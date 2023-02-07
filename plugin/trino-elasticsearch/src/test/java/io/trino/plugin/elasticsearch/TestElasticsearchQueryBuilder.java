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
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.elasticsearch.aggregation.MetricAggregation;
import io.trino.plugin.elasticsearch.aggregation.TermAggregation;
import io.trino.plugin.elasticsearch.decoders.DoubleDecoder;
import io.trino.plugin.elasticsearch.decoders.IntegerDecoder;
import io.trino.plugin.elasticsearch.decoders.VarcharDecoder;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.RealType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestElasticsearchQueryBuilder
{
    private static final ElasticsearchColumnHandle NAME = new ElasticsearchColumnHandle("name", VARCHAR, new VarcharDecoder.Descriptor("name"), true);
    private static final ElasticsearchColumnHandle AGE = new ElasticsearchColumnHandle("age", INTEGER, new IntegerDecoder.Descriptor("age"), true);
    private static final ElasticsearchColumnHandle SCORE = new ElasticsearchColumnHandle("score", DOUBLE, new DoubleDecoder.Descriptor("score"), true);
    private static final ElasticsearchColumnHandle LENGTH = new ElasticsearchColumnHandle("length", DOUBLE, new DoubleDecoder.Descriptor("length"), true);
    private static final ElasticsearchColumnHandle TOTAL = new ElasticsearchColumnHandle("total", RealType.REAL, new DoubleDecoder.Descriptor("total"), true);
    private static final ElasticsearchColumnHandle VALUES = new ElasticsearchColumnHandle("values", RealType.REAL, new DoubleDecoder.Descriptor("values"), true);

    private static final MetricAggregation AGG_AVG_VALUES = new MetricAggregation("avg", RealType.REAL, Optional.of(VALUES), "avg_values");
    private static final MetricAggregation AGG_MAX_VALUES = new MetricAggregation("max", RealType.REAL, Optional.of(VALUES), "max_values");
    private static final MetricAggregation AGG_MIN_VALUES = new MetricAggregation("min", RealType.REAL, Optional.of(VALUES), "min_values");
    private static final MetricAggregation AGG_SUM_VALUES = new MetricAggregation("sum", RealType.REAL, Optional.of(VALUES), "sum_values");
    private static final MetricAggregation AGG_COUNT_VALUES = new MetricAggregation("count", RealType.REAL, Optional.of(VALUES), "count_values");
    private static final MetricAggregation AGG_COUNT_ALL = new MetricAggregation("count", RealType.REAL, Optional.empty(), "count_all");

    @Test
    public void testMatchAll()
    {
        assertQueryBuilder(
                ImmutableMap.of(),
                new MatchAllQueryBuilder());
    }

    @Test
    public void testOneConstraint()
    {
        // SingleValue
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.singleValue(INTEGER, 1L)),
                new BoolQueryBuilder().filter(new TermQueryBuilder(AGE.getName(), 1L)));

        // Range
        assertQueryBuilder(
                ImmutableMap.of(SCORE, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 65.0, false, 80.0, true)), false)),
                new BoolQueryBuilder().filter(new RangeQueryBuilder(SCORE.getName()).gt(65.0).lte(80.0)));

        // List
        assertQueryBuilder(
                ImmutableMap.of(NAME, Domain.multipleValues(VARCHAR, ImmutableList.of("alice", "bob"))),
                new BoolQueryBuilder().filter(
                        new BoolQueryBuilder()
                                .should(new TermQueryBuilder(NAME.getName(), "alice"))
                                .should(new TermQueryBuilder(NAME.getName(), "bob"))));
        // all
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.all(INTEGER)),
                new MatchAllQueryBuilder());

        // notNull
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.notNull(INTEGER)),
                new BoolQueryBuilder().filter(new ExistsQueryBuilder(AGE.getName())));

        // isNull
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.onlyNull(INTEGER)),
                new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(AGE.getName())));

        // isNullAllowed
        assertQueryBuilder(
                ImmutableMap.of(AGE, Domain.singleValue(INTEGER, 1L, true)),
                new BoolQueryBuilder().filter(
                        new BoolQueryBuilder()
                                .should(new TermQueryBuilder(AGE.getName(), 1L))
                                .should(new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(AGE.getName())))));
    }

    @Test
    public void testMultiConstraint()
    {
        assertQueryBuilder(
                ImmutableMap.of(
                        AGE, Domain.singleValue(INTEGER, 1L),
                        SCORE, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 65.0, false, 80.0, true)), false)),
                new BoolQueryBuilder()
                        .filter(new TermQueryBuilder(AGE.getName(), 1L))
                        .filter(new RangeQueryBuilder(SCORE.getName()).gt(65.0).lte(80.0)));

        assertQueryBuilder(
                ImmutableMap.of(
                        LENGTH, Domain.create(ValueSet.ofRanges(Range.range(DOUBLE, 160.0, true, 180.0, true)), false),
                        SCORE, Domain.create(ValueSet.ofRanges(
                                Range.range(DOUBLE, 65.0, false, 80.0, true),
                                Range.equal(DOUBLE, 90.0)), false)),
                new BoolQueryBuilder()
                        .filter(new RangeQueryBuilder(LENGTH.getName()).gte(160.0).lte(180.0))
                        .filter(new BoolQueryBuilder()
                                .should(new RangeQueryBuilder(SCORE.getName()).gt(65.0).lte(80.0))
                                .should(new TermQueryBuilder(SCORE.getName(), 90.0))));

        assertQueryBuilder(
                ImmutableMap.of(
                        AGE, Domain.singleValue(INTEGER, 10L),
                        SCORE, Domain.onlyNull(DOUBLE)),
                new BoolQueryBuilder()
                        .filter(new TermQueryBuilder(AGE.getName(), 10L))
                        .mustNot(new ExistsQueryBuilder(SCORE.getName())));
    }

    private static void assertQueryBuilder(Map<ElasticsearchColumnHandle, Domain> domains, QueryBuilder expected)
    {
        QueryBuilder actual = buildSearchQuery(TupleDomain.withColumnDomains(domains), Optional.empty(), Map.of());
        assertEquals(actual, expected);
    }

    @Test
    public void testBuildAggregationQuery()
    {
        TermAggregation termAggregation1 = TermAggregation.fromColumnHandle(NAME).get();
        TermAggregation termAggregation2 = TermAggregation.fromColumnHandle(TOTAL).get();
        List<AggregationBuilder> aggBuilder =
                ElasticsearchQueryBuilder.buildAggregationQuery(
                        ImmutableList.of(termAggregation1, termAggregation2),
                        ImmutableList.of(
                                AGG_AVG_VALUES, AGG_MAX_VALUES, AGG_MIN_VALUES, AGG_SUM_VALUES, AGG_COUNT_VALUES, AGG_COUNT_ALL),
                        OptionalInt.empty(),
                        Optional.empty());
        assertThat(aggBuilder).hasSize(1);
        assertThat(aggBuilder.get(0)).isExactlyInstanceOf(CompositeAggregationBuilder.class);
        List<AggregationBuilder> subaggs = aggBuilder.get(0).getSubAggregations().stream().toList();
        assertAllAgg(subaggs);
        CompositeAggregationBuilder compositeAggregationBuilder = (CompositeAggregationBuilder) aggBuilder.get(0);
        List<CompositeValuesSourceBuilder<?>> sources = compositeAggregationBuilder.sources();
        assertThat(sources.get(0)).isExactlyInstanceOf(TermsValuesSourceBuilder.class).hasFieldOrPropertyWithValue("field", NAME.getName());
        assertThat(sources.get(1)).isExactlyInstanceOf(TermsValuesSourceBuilder.class).hasFieldOrPropertyWithValue("field", TOTAL.getName());
    }

    @Test
    public void testBuildAggregationQueryWithEmptyAggregation()
    {
        TermAggregation termAggregation1 = TermAggregation.fromColumnHandle(NAME).get();
        List<AggregationBuilder> aggBuilder =
                ElasticsearchQueryBuilder.buildAggregationQuery(ImmutableList.of(termAggregation1), Collections.emptyList(), OptionalInt.empty(), Optional.empty());
        assertThat(aggBuilder).hasSize(1);
        assertThat(aggBuilder.get(0)).isExactlyInstanceOf(CompositeAggregationBuilder.class);
        CompositeAggregationBuilder compositeAggregationBuilder = (CompositeAggregationBuilder) aggBuilder.get(0);
        List<CompositeValuesSourceBuilder<?>> sources = compositeAggregationBuilder.sources();
        assertThat(sources.get(0)).isExactlyInstanceOf(TermsValuesSourceBuilder.class).hasFieldOrPropertyWithValue("field", NAME.getName());
        List<AggregationBuilder> sub1 = aggBuilder.get(0).getSubAggregations().stream().toList();
        assertThat(sub1).hasSize(0);
    }

    private void assertAllAgg(List<AggregationBuilder> allAggs)
    {
        assertThat(allAggs).hasSize(6);
        assertThat(allAggs.get(0)).isExactlyInstanceOf(AvgAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", VALUES.getName())
                .hasFieldOrPropertyWithValue("name", AGG_AVG_VALUES.getAlias());
        assertThat(allAggs.get(1)).isExactlyInstanceOf(MaxAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", VALUES.getName())
                .hasFieldOrPropertyWithValue("name", AGG_MAX_VALUES.getAlias());
        assertThat(allAggs.get(2)).isExactlyInstanceOf(MinAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", VALUES.getName())
                .hasFieldOrPropertyWithValue("name", AGG_MIN_VALUES.getAlias());
        assertThat(allAggs.get(3)).isExactlyInstanceOf(SumAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", VALUES.getName())
                .hasFieldOrPropertyWithValue("name", AGG_SUM_VALUES.getAlias());
        assertThat(allAggs.get(4)).isExactlyInstanceOf(ValueCountAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", VALUES.getName())
                .hasFieldOrPropertyWithValue("name", AGG_COUNT_VALUES.getAlias());
        assertThat(allAggs.get(5)).isExactlyInstanceOf(ValueCountAggregationBuilder.class)
                .hasFieldOrPropertyWithValue("field", "_id")
                .hasFieldOrPropertyWithValue("name", AGG_COUNT_ALL.getAlias());
    }
}
