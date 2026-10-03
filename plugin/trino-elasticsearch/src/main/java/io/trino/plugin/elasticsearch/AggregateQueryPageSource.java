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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.elasticsearch.aggregation.MetricAggregation;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.client.SearchDocument;
import io.trino.plugin.elasticsearch.decoders.Decoder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.TypeManager;

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
    private static final SearchDocument NO_HIT = new SearchDocument("", Float.NaN, Map.of(), "", 0, Map.of());
    private final List<Decoder> decoders;

    private final ElasticsearchClient client;
    private final ElasticsearchTableHandle table;
    private final ElasticsearchSplit split;
    private final JsonNode queryBuilder;

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
    public void close() {}

    @Override
    public SourcePage getNextSourcePage()
    {
        long start = System.nanoTime();
        OptionalInt pageSize = table.topN().isEmpty() ? OptionalInt.of(aggregationPageSize) : OptionalInt.of((int) table.topN().get().limit());
        JsonNode searchResponse = client.beginAggregationSearch(
                split.index(),
                queryBuilder,
                buildAggregationQuery(table.termAggregations(), table.metricAggregations(), pageSize, after));
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

    private static Optional<Map<String, Object>> extractAfter(JsonNode searchResponse)
    {
        JsonNode afterKey = searchResponse.path("aggregations").path("groupBy").path("after_key");
        if (afterKey.isMissingNode() || afterKey.isNull()) {
            return Optional.empty();
        }
        return Optional.of(extractKey(afterKey));
    }

    private List<Decoder> createDecoders(List<ElasticsearchColumnHandle> columns)
    {
        return columns.stream()
                .map(ElasticsearchColumnHandle::decoderDescriptor)
                .map(DecoderDescriptor::createDecoder)
                .collect(toImmutableList());
    }

    private static Map<String, Object> extractKey(JsonNode key)
    {
        Map<String, Object> result = new HashMap<>();
        key.properties().forEach(entry -> {
            JsonNode value = entry.getValue();
            result.put(entry.getKey(), switch (value.getNodeType()) {
                case NULL -> null;
                case NUMBER -> value.numberValue();
                case STRING -> value.textValue();
                case BOOLEAN -> value.booleanValue();
                default -> throw new IllegalStateException("Unexpected composite aggregation key: " + value);
            });
        });
        return result;
    }

    private List<Map<String, Object>> getResult(JsonNode searchResponse)
    {
        JsonNode aggregations = searchResponse.path("aggregations");

        // COUNT(*) does not create a metric aggregation.
        if (aggregations.isMissingNode() || aggregations.isNull() || aggregations.isEmpty()) {
            Map<String, Object> singleValueMap = new LinkedHashMap<>();
            for (MetricAggregation metricAgg : table.metricAggregations()) {
                if (metricAgg.getColumnHandle().isEmpty() && MetricAggregation.COUNT.equals(metricAgg.getFunctionName())) {
                    singleValueMap.put(metricAgg.getAlias(), getTotalHits(searchResponse));
                }
            }
            if (!singleValueMap.isEmpty()) {
                return ImmutableList.of(singleValueMap);
            }
            return Collections.emptyList();
        }

        Map<String, Object> singleValueMap = new LinkedHashMap<>();
        ImmutableList.Builder<Map<String, Object>> result = ImmutableList.builder();
        boolean hasBucketsAggregation = aggregations.has("groupBy");
        if (hasBucketsAggregation) {
            if (aggregations.size() != 1) {
                throw new IllegalStateException("Bucket and metric aggregations should not be both present.");
            }
            for (JsonNode bucket : aggregations.path("groupBy").path("buckets")) {
                Map<String, Object> line = extractKey(bucket.path("key"));
                for (MetricAggregation metricAgg : table.metricAggregations()) {
                    if (metricAgg.getColumnHandle().isEmpty() && MetricAggregation.COUNT.equals(metricAgg.getFunctionName())) {
                        line.put(metricAgg.getAlias(), bucket.path("doc_count").doubleValue());
                    }
                    else {
                        line.put(metricAgg.getAlias(), extractMetricValue(bucket.path(metricAgg.getAlias())));
                    }
                }
                result.add(line);
            }
            return result.build();
        }

        for (Map.Entry<String, JsonNode> aggregation : aggregations.properties()) {
            singleValueMap.put(aggregation.getKey(), extractMetricValue(aggregation.getValue()));
        }

        // COUNT(*) has no sub-aggregation, so use total hits alongside the other global metrics.
        for (MetricAggregation metricAgg : table.metricAggregations()) {
            if (metricAgg.getColumnHandle().isEmpty() && MetricAggregation.COUNT.equals(metricAgg.getFunctionName())) {
                singleValueMap.put(metricAgg.getAlias(), getTotalHits(searchResponse));
            }
        }
        return ImmutableList.of(singleValueMap);
    }

    private static double getTotalHits(JsonNode searchResponse)
    {
        JsonNode totalHits = searchResponse.path("hits").path("total");
        return totalHits.isNumber() ? totalHits.doubleValue() : totalHits.path("value").doubleValue();
    }

    private static Double extractMetricValue(JsonNode aggregation)
    {
        if (aggregation.has("value")) {
            return extractSingleValue(aggregation.path("value"));
        }
        if (aggregation.has("min") && aggregation.has("sum")) {
            return extractSumFromStatsValue(aggregation);
        }
        throw new IllegalStateException("Unrecognized aggregation: " + aggregation);
    }

    static Double extractSumFromStatsValue(JsonNode statsValue)
    {
        if (extractSingleValue(statsValue.path("min")) == null) {
            return null;
        }
        return statsValue.path("sum").asDouble();
    }

    static Double extractSingleValue(JsonNode singleValue)
    {
        if (singleValue.isNull() || Double.isInfinite(singleValue.asDouble())) {
            return null;
        }
        return singleValue.asDouble();
    }
}
