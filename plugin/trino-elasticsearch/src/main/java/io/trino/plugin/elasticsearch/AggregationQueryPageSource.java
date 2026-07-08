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
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.elasticsearch.ElasticsearchAggregate.Function.COUNT_ALL;
import static io.trino.plugin.elasticsearch.ElasticsearchAggregate.Function.SUM;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildAggregationQuery;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

/**
 * Serves a table handle carrying a pushed-down aggregation. A global aggregation runs a single {@code size:0} metric
 * query and returns one row; a {@code GROUP BY} runs a composite aggregation and pages through the buckets with the
 * {@code after} key, decoding each bucket into a row.
 */
class AggregationQueryPageSource
        implements ConnectorPageSource
{
    private static final int COMPOSITE_SIZE = 1000;

    private final ElasticsearchClient client;
    private final String index;
    private final JsonNode filterQuery;
    private final List<ElasticsearchColumnHandle> groupingColumns;
    private final List<ElasticsearchAggregate> aggregates;
    private final List<Output> outputs;

    private long readTimeNanos;
    private Optional<JsonNode> afterKey = Optional.empty();
    private boolean finished;

    public AggregationQueryPageSource(ElasticsearchClient client, ElasticsearchTableHandle table, List<ElasticsearchColumnHandle> columns)
    {
        this.client = requireNonNull(client, "client is null");
        requireNonNull(table, "table is null");
        this.index = table.index();
        this.filterQuery = buildSearchQuery(
                table.constraint().transformKeys(ElasticsearchColumnHandle.class::cast),
                table.query(),
                table.regexes(),
                table.prefixes());
        ElasticsearchAggregation aggregation = table.aggregation().orElseThrow(() -> new IllegalArgumentException("table handle has no aggregation"));
        this.groupingColumns = aggregation.groupingColumns();
        this.aggregates = aggregation.aggregates();

        ImmutableList.Builder<Output> outputs = ImmutableList.builder();
        for (ElasticsearchColumnHandle column : columns) {
            outputs.add(resolveOutput(column));
        }
        this.outputs = outputs.build();
    }

    private Output resolveOutput(ElasticsearchColumnHandle column)
    {
        for (int i = 0; i < groupingColumns.size(); i++) {
            if (groupingColumns.get(i).name().equals(column.name())) {
                return new Output(column.type(), "g" + i, Optional.empty());
            }
        }
        for (ElasticsearchAggregate aggregate : aggregates) {
            if (aggregate.outputName().equals(column.name())) {
                return new Output(column.type(), aggregate.outputName(), Optional.of(aggregate.function()));
            }
        }
        throw new IllegalArgumentException("Column is neither a grouping key nor an aggregate: " + column.name());
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        long start = System.nanoTime();
        SourcePage page = groupingColumns.isEmpty() ? globalPage() : groupedPage();
        readTimeNanos += System.nanoTime() - start;
        return page;
    }

    private SourcePage globalPage()
    {
        JsonNode body = buildAggregationQuery(filterQuery, groupingColumns, aggregates, COMPOSITE_SIZE, Optional.empty());
        JsonNode response = client.aggregate(index, body);
        long totalHits = response.path("hits").path("total").path("value").asLong();
        JsonNode metrics = response.path("aggregations");

        BlockBuilder[] builders = createBuilders(1);
        // A global aggregation has no grouping keys, so every output is a metric
        for (int i = 0; i < outputs.size(); i++) {
            appendMetric(builders[i], outputs.get(i), metrics, totalHits);
        }
        finished = true;
        return page(builders);
    }

    private SourcePage groupedPage()
    {
        JsonNode body = buildAggregationQuery(filterQuery, groupingColumns, aggregates, COMPOSITE_SIZE, afterKey);
        JsonNode groups = client.aggregate(index, body).path("aggregations").path("groups");
        JsonNode buckets = groups.path("buckets");

        BlockBuilder[] builders = createBuilders(buckets.size());
        for (JsonNode bucket : buckets) {
            JsonNode key = bucket.path("key");
            long docCount = bucket.path("doc_count").asLong();
            for (int i = 0; i < outputs.size(); i++) {
                Output output = outputs.get(i);
                if (output.function().isEmpty()) {
                    appendValue(builders[i], output.type(), key.path(output.key()));
                }
                else {
                    appendMetric(builders[i], output, bucket, docCount);
                }
            }
        }

        // A composite page smaller than the requested size is the last page
        if (buckets.size() < COMPOSITE_SIZE || groups.path("after_key").isMissingNode()) {
            finished = true;
        }
        else {
            afterKey = Optional.of(groups.path("after_key"));
        }
        return page(builders);
    }

    private static void appendMetric(BlockBuilder builder, Output output, JsonNode metrics, long count)
    {
        ElasticsearchAggregate.Function function = output.function().orElseThrow();
        if (function == COUNT_ALL) {
            output.type().writeLong(builder, count);
            return;
        }
        if (function == SUM) {
            // sum() is 0 over an all-null group; a value_count of 0 means SQL sum() is NULL there
            if (metrics.path(output.key() + "_count").path("value").asLong() == 0) {
                builder.appendNull();
                return;
            }
        }
        appendValue(builder, output.type(), metrics.path(output.key()).path("value"));
    }

    private static void appendValue(BlockBuilder builder, Type type, JsonNode value)
    {
        if (value == null || value.isNull() || value.isMissingNode()) {
            builder.appendNull();
            return;
        }
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            type.writeLong(builder, value.asLong());
        }
        else if (type.equals(DOUBLE)) {
            type.writeDouble(builder, value.asDouble());
        }
        else if (type.equals(REAL)) {
            type.writeLong(builder, floatToRawIntBits((float) value.asDouble()));
        }
        else if (type.equals(BOOLEAN)) {
            type.writeBoolean(builder, value.isBoolean() ? value.asBoolean() : value.asInt() != 0);
        }
        else if (type instanceof VarcharType) {
            type.writeSlice(builder, utf8Slice(value.asText()));
        }
        else {
            throw new UnsupportedOperationException("Unsupported aggregation output type: " + type);
        }
    }

    private BlockBuilder[] createBuilders(int positions)
    {
        BlockBuilder[] builders = new BlockBuilder[outputs.size()];
        for (int i = 0; i < outputs.size(); i++) {
            builders[i] = outputs.get(i).type().createBlockBuilder(null, Math.max(positions, 1));
        }
        return builders;
    }

    private static SourcePage page(BlockBuilder[] builders)
    {
        Block[] blocks = new Block[builders.length];
        for (int i = 0; i < builders.length; i++) {
            blocks[i] = builders[i].build();
        }
        return SourcePage.create(new Page(blocks));
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public void close() {}

    private record Output(Type type, String key, Optional<ElasticsearchAggregate.Function> function) {}
}
