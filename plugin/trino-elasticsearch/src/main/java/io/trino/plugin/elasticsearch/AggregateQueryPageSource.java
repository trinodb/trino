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
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.decoders.Decoder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.TypeManager;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.Stats;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildAggregationQuery;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static java.util.Objects.requireNonNull;

public class AggregateQueryPageSource
        implements ConnectorPageSource
{
    private static final SearchHit NO_HIT = new SearchHit(0);
    private final List<Decoder> decoders;

    private final ElasticsearchClient client;
    private final ElasticsearchTableHandle table;
    private final ElasticsearchSplit split;
    private QueryBuilder queryBuilder;

    private final BlockBuilder[] columnBuilders;
    private final List<ElasticsearchColumnHandle> columns;
    private final int aggregationPageSize;
    private long totalBytes;
    private long readTimeNanos;
    private Optional<Map<String, Object>> after = Optional.empty();
    private boolean fetched;
    private long fetchedSize;

    public AggregateQueryPageSource(
            ElasticsearchClient client,
            TypeManager typeManager,
            ElasticsearchTableHandle table,
            ElasticsearchSplit split,
            List<ElasticsearchColumnHandle> columns,
            int aggregationPageSize)
    {
        requireNonNull(client, "client is null");
        requireNonNull(table, "table is null");
        requireNonNull(split, "split is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(columns, "columns is null");

        this.client = client;
        this.table = table;
        this.split = split;
        this.aggregationPageSize = aggregationPageSize;

        this.columns = ImmutableList.copyOf(columns);

        decoders = createDecoders(columns);

        columnBuilders = columns.stream()
                .map(ElasticsearchColumnHandle::type)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);
        this.queryBuilder = buildSearchQuery(table.constraint().transformKeys(ElasticsearchColumnHandle.class::cast), table.query(), table.regexes());

        long start = System.nanoTime();
        readTimeNanos += System.nanoTime() - start;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        // One of the following situation may stop the fetching
        // 1. afterKey is empty, that means no more result can be fetched
        // 2. fetchedSize >= the potential limit constraint
        return (fetched && after.isEmpty()) || (table.topN().isPresent() && fetchedSize >= table.topN().get().limit());
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        long start = System.nanoTime();
        OptionalInt pageSize = table.topN().isEmpty() ? OptionalInt.of(aggregationPageSize) : OptionalInt.of((int) table.topN().get().limit());
        SearchResponse searchResponse = client.beginSearch(
                split.index(),
                split.shard(),
                this.queryBuilder,
                Optional.ofNullable(
                        buildAggregationQuery(table.termAggregations(), table.metricAggregations(), pageSize, after)),
                Optional.empty(),
                Collections.emptyList(),
                table.topN());
        readTimeNanos += System.nanoTime() - start;
        fetched = true;
        List<Map<String, Object>> flatResult = getResult(searchResponse);
        fetchedSize += flatResult.size();
        after = extractAfter(searchResponse);
        for (Map<String, Object> result : flatResult) {
            for (int i = 0; i < columns.size(); i++) {
                String key = columns.get(i).name();
                decoders.get(i).decode(NO_HIT, () -> result.get(key), columnBuilders[i]);
            }
        }
        Block[] blocks = new Block[columnBuilders.length];
        for (int i = 0; i < columnBuilders.length; i++) {
            blocks[i] = columnBuilders[i].build();
            columnBuilders[i] = columnBuilders[i].newBlockBuilderLike(null);
        }
        return SourcePage.create(new Page(blocks));
    }

    private Optional<Map<String, Object>> extractAfter(SearchResponse searchResponse)
    {
        Aggregations aggregations = searchResponse.getAggregations();
        if (aggregations == null) {
            return Optional.empty();
        }
        for (Aggregation agg : aggregations) {
            if (agg instanceof CompositeAggregation compositeAggregation) {
                return Optional.ofNullable(compositeAggregation.afterKey());
            }
        }
        return Optional.empty();
    }

    private List<Decoder> createDecoders(List<ElasticsearchColumnHandle> columns)
    {
        return columns.stream()
                .map(ElasticsearchColumnHandle::decoderDescriptor)
                .map(DecoderDescriptor::createDecoder)
                .collect(toImmutableList());
    }

    private static String appendPath(String base, String element)
    {
        if (base.isEmpty()) {
            return element;
        }

        return base + "." + element;
    }

    private List<Map<String, Object>> getResult(SearchResponse searchResponse)
    {
        Aggregations aggregations = searchResponse.getAggregations();

        // Handle global COUNT(*) when there are no aggregations
        // This happens because buildMetricAggregation returns null for COUNT(*)
        if (aggregations == null || aggregations.asList().isEmpty()) {
            Map<String, Object> singleValueMap = new LinkedHashMap<>();
            for (MetricAggregation metricAgg : table.metricAggregations()) {
                if (metricAgg.getColumnHandle().isEmpty() && MetricAggregation.COUNT.equals(metricAgg.getFunctionName())) {
                    // This is global COUNT(*) - use total hits
                    singleValueMap.put(metricAgg.getAlias(), (double) searchResponse.getHits().getTotalHits().value);
                }
            }
            if (!singleValueMap.isEmpty()) {
                return ImmutableList.of(singleValueMap);
            }
            return Collections.emptyList();
        }

        Map<String, Object> singleValueMap = new LinkedHashMap<>();
        ImmutableList.Builder<Map<String, Object>> result = ImmutableList.builder();
        boolean hasBucketsAggregation = false;
        boolean hasMetricAggregation = false;
        for (Aggregation agg : aggregations) {
            if (agg instanceof CompositeAggregation compositeAggregation) {
                hasBucketsAggregation = true;
                if (hasMetricAggregation) {
                    throw new IllegalStateException("Bucket and metric aggregations should not be both present.");
                }
                for (CompositeAggregation.Bucket bucket : compositeAggregation.getBuckets()) {
                    Map<String, Object> line = new HashMap<>();
                    line.putAll(bucket.getKey());

                    // Process metric aggregations
                    for (Aggregation metricAgg : bucket.getAggregations()) {
                        if (metricAgg instanceof NumericMetricsAggregation.SingleValue sv) {
                            line.put(metricAgg.getName(), extractSingleValue(sv));
                        }
                        else if (metricAgg instanceof Stats stats) {
                            line.put(metricAgg.getName(), extractSumFromStatsValue(stats));
                        }
                    }

                    // Handle COUNT(*) - these don't have sub-aggregations, just use bucket doc count
                    for (MetricAggregation metricAgg : table.metricAggregations()) {
                        if (metricAgg.getColumnHandle().isEmpty() && MetricAggregation.COUNT.equals(metricAgg.getFunctionName())) {
                            line.put(metricAgg.getAlias(), (double) bucket.getDocCount());
                        }
                    }

                    result.add(line);
                }
            }
            else if (agg instanceof NumericMetricsAggregation.SingleValue sv) {
                hasMetricAggregation = true;
                if (hasBucketsAggregation) {
                    throw new IllegalStateException("Bucket and metric aggregations should not be both present.");
                }
                singleValueMap.put(agg.getName(), extractSingleValue(sv));
            }
            // We use stats aggregation instead of sum aggregation
            else if (agg instanceof Stats stats) {
                hasMetricAggregation = true;
                if (hasBucketsAggregation) {
                    throw new IllegalStateException("Bucket and metric aggregations should not be both present.");
                }
                singleValueMap.put(agg.getName(), extractSumFromStatsValue(stats));
            }
            else {
                throw new IllegalStateException("Unrecognized aggregation type: " + agg.getType());
            }
        }

        // Inject COUNT(*) for non-bucket metric aggregations (e.g., SELECT count(*), sum(x))
        // COUNT(*) doesn't create a sub-aggregation, so we use total hits from the search response
        if (hasMetricAggregation) {
            for (MetricAggregation metricAgg : table.metricAggregations()) {
                if (metricAgg.getColumnHandle().isEmpty() && MetricAggregation.COUNT.equals(metricAgg.getFunctionName())) {
                    singleValueMap.put(metricAgg.getAlias(), (double) searchResponse.getHits().getTotalHits().value);
                }
            }
        }

        if (hasBucketsAggregation) {
            return result.build();
        }
        else {
            return ImmutableList.of(singleValueMap);
        }
    }

    private Double extractSumFromStatsValue(Stats statsValue)
    {
        double sumValue = statsValue.getSum();
        // Check min(not avg) result to see if the sum result is valid.
        // Es server return null for avg/min/max, but the es client return 0 as the result of avg even server return null.
        // See org.elasticsearch.search.aggregations.metrics.stats.ParsedStats for more
        double minValue = statsValue.getMin();
        if (Double.isInfinite(minValue)) {
            return null;
        }
        return sumValue;
    }

    private Double extractSingleValue(NumericMetricsAggregation.SingleValue singleValue)
    {
        // null will be decoded as Double.POSITIVE_INFINITY or Double.NEGATIVE_INFINITY
        if (Double.isInfinite(singleValue.value())) {
            return null;
        }
        else {
            return singleValue.value();
        }
    }
}
