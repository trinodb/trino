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
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.decoders.Decoder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildAggregationQuery;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static java.util.Objects.requireNonNull;

public class AggregationQueryPageSource
        implements ConnectorPageSource
{
    private final long readTimeNanos;
    private final SearchResponse searchResponse;
    private final List<Decoder> decoders;
    private final BlockBuilder[] columnBuilders;
    private final List<ElasticsearchColumnHandle> columns;
    private boolean fetched;

    public AggregationQueryPageSource(
            ElasticsearchClient client,
            ElasticsearchTableHandle table,
            ElasticsearchSplit split,
            List<ElasticsearchColumnHandle> columns)
    {
        requireNonNull(client, "client is null");
        requireNonNull(table, "table is null");
        requireNonNull(split, "split is null");
        requireNonNull(columns, "columns is null");
        this.columns = columns;
        columnBuilders = columns.stream()
                .map(ElasticsearchColumnHandle::getType)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);
        decoders = createDecoders(columns);

        long start = System.nanoTime();
        this.searchResponse = client.beginSearch(
                split.getIndex(),
                split.getShard(),
                buildSearchQuery(table.getConstraint().transform(ElasticsearchColumnHandle.class::cast), table.getQuery()),
                Optional.ofNullable(buildAggregationQuery(table.getAggTerms(), table.getAggregates())),
                Optional.empty(),
                Collections.emptyList(),
                Optional.empty(),
                table.getLimit());
        readTimeNanos = System.nanoTime() - start;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return fetched;
    }

    @Override
    public Page getNextPage()
    {
        fetched = true;
        final SearchHit noUsed = new SearchHit(0);
        List<Map<String, Object>> flatResult = flattern(searchResponse.getAggregations());
        for (Map<String, Object> result : flatResult) {
            for (int i = 0; i < columns.size(); i++) {
                String key = columns.get(i).getName();
                decoders.get(i).decode(noUsed, () -> result.get(key), columnBuilders[i]);
            }
        }
        Block[] blocks = new Block[columnBuilders.length];
        for (int i = 0; i < columnBuilders.length; i++) {
            blocks[i] = columnBuilders[i].build();
            columnBuilders[i] = columnBuilders[i].newBlockBuilderLike(null);
        }
        return new Page(blocks);
    }

    private List<Map<String, Object>> flattern(Aggregations aggregations)
    {
        if (aggregations == null) {
            return Collections.emptyList();
        }
        Map<String, Object> singleValueMap = new LinkedHashMap<>();
        List<Map<String, Object>> result = new ArrayList<>();
        boolean hasBucketsAggregation = false;
        for (Aggregation agg : aggregations) {
            if (agg instanceof MultiBucketsAggregation) {
                if (hasBucketsAggregation) {
                    throw new UnsupportedOperationException("Impossible merge different bucket aggregation, this must not be appear.");
                }
                hasBucketsAggregation = true;
                MultiBucketsAggregation bucketsAggregation = (MultiBucketsAggregation) agg;
                // each key of the bucket is the value of current aggregation level
                for (MultiBucketsAggregation.Bucket bucket : bucketsAggregation.getBuckets()) {
                    List<Map<String, Object>> subResults = flattern(bucket.getAggregations());
                    if (!subResults.isEmpty()) {
                        // add the current bucket key and value into each sub result
                        for (Map<String, Object> sr : subResults) {
                            sr.put(bucketsAggregation.getName(), bucket.getKey());
                        }
                        result.addAll(subResults);
                    }
                    else {
                        Map<String, Object> singleKeyMap = new LinkedHashMap<>();
                        singleKeyMap.put(bucketsAggregation.getName(), bucket.getKey());
                        result.add(singleKeyMap);
                    }
                }
            }
            else if (agg instanceof NumericMetricsAggregation.SingleValue) {
                if (hasBucketsAggregation) {
                    throw new UnsupportedOperationException("Impossible merge different bucket and metric aggregation, this must not be appear.");
                }
                NumericMetricsAggregation.SingleValue sv = (NumericMetricsAggregation.SingleValue) agg;
                singleValueMap.put(agg.getName(), sv.value());
            }
        }
        if (hasBucketsAggregation) {
            return result;
        }
        else {
            return ImmutableList.of(singleValueMap);
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
    }

    private List<Decoder> createDecoders(List<ElasticsearchColumnHandle> columns)
    {
        return columns.stream()
                .map(column -> ScanQueryPageSource.createDecoder(column.getName(), column.getType()))
                .collect(toImmutableList());
    }
}
