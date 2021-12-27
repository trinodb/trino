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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.plugin.elasticsearch.decoders.Decoder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.elasticsearch.BuiltinColumns.SOURCE;
import static io.trino.plugin.elasticsearch.BuiltinColumns.isBuiltinColumn;
import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toList;

public class ScanQueryPageSource
        implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(ScanQueryPageSource.class);

    private final List<Decoder> decoders;

    private final SearchHitIterator iterator;
    private final BlockBuilder[] columnBuilders;
    private final List<ElasticsearchColumnHandle> columns;
    private long totalBytes;
    private long readTimeNanos;

    public ScanQueryPageSource(
            ElasticsearchClient client,
            TypeManager typeManager,
            ElasticsearchTableHandle table,
            ElasticsearchSplit split,
            List<ElasticsearchColumnHandle> columns)
    {
        requireNonNull(client, "client is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(columns, "columns is null");

        this.columns = ImmutableList.copyOf(columns);

        decoders = createDecoders(columns);

        // When the _source field is requested, we need to bypass column pruning when fetching the document
        boolean needAllFields = columns.stream()
                .map(ElasticsearchColumnHandle::getName)
                .anyMatch(isEqual(SOURCE.getName()));

        // Columns to fetch as doc_fields instead of pulling them out of the JSON source
        // This is convenient for types such as DATE, TIMESTAMP, etc, which have multiple possible
        // representations in JSON, but a single normalized representation as doc_field.
        List<String> documentFields = flattenFields(columns).entrySet().stream()
                .filter(entry -> entry.getValue().equals(TIMESTAMP_MILLIS))
                .map(Map.Entry::getKey)
                .collect(toImmutableList());

        columnBuilders = columns.stream()
                .map(ElasticsearchColumnHandle::getType)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);

        List<String> requiredFields = columns.stream()
                .map(ElasticsearchColumnHandle::getName)
                .filter(name -> !isBuiltinColumn(name))
                .collect(toList());

        // sorting by _doc (index order) get special treatment in Elasticsearch and is more efficient
        Optional<String> sort = Optional.of("_doc");

        if (table.getQuery().isPresent()) {
            // However, if we're using a custom Elasticsearch query, use default sorting.
            // Documents will be scored and returned based on relevance
            sort = Optional.empty();
        }

        long start = System.nanoTime();
        SearchResponse searchResponse = client.beginSearch(
                split.getIndex(),
                split.getShard(),
                buildSearchQuery(table.getConstraint().transformKeys(ElasticsearchColumnHandle.class::cast), table.getQuery()),
                needAllFields ? Optional.empty() : Optional.of(requiredFields),
                documentFields,
                sort,
                table.getLimit());
        readTimeNanos += System.nanoTime() - start;
        this.iterator = new SearchHitIterator(client, () -> searchResponse, table.getLimit());
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos + iterator.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return !iterator.hasNext();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        iterator.close();
    }

    @Override
    public Page getNextPage()
    {
        long size = 0;
        while (size < PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES && iterator.hasNext()) {
            SearchHit hit = iterator.next();
            Map<String, Object> document = hit.getSourceAsMap();

            for (int i = 0; i < decoders.size(); i++) {
                String field = columns.get(i).getName();
                decoders.get(i).decode(hit, () -> getField(document, field), columnBuilders[i]);
            }

            if (hit.getSourceRef() != null) {
                totalBytes += hit.getSourceRef().length();
            }

            size = Arrays.stream(columnBuilders)
                    .mapToLong(BlockBuilder::getSizeInBytes)
                    .sum();
        }

        Block[] blocks = new Block[columnBuilders.length];
        for (int i = 0; i < columnBuilders.length; i++) {
            blocks[i] = columnBuilders[i].build();
            columnBuilders[i] = columnBuilders[i].newBlockBuilderLike(null);
        }

        return new Page(blocks);
    }

    public static Object getField(Map<String, Object> document, String field)
    {
        Object value = document.get(field);
        if (value == null) {
            Map<String, Object> result = new HashMap<>();
            String prefix = field + ".";
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith(prefix)) {
                    result.put(key.substring(prefix.length()), entry.getValue());
                }
            }

            if (!result.isEmpty()) {
                return result;
            }
        }

        return value;
    }

    private Map<String, Type> flattenFields(List<ElasticsearchColumnHandle> columns)
    {
        Map<String, Type> result = new HashMap<>();

        for (ElasticsearchColumnHandle column : columns) {
            flattenFields(result, column.getName(), column.getType());
        }

        return result;
    }

    private void flattenFields(Map<String, Type> result, String fieldName, Type type)
    {
        if (type instanceof RowType) {
            for (RowType.Field field : ((RowType) type).getFields()) {
                flattenFields(result, appendPath(fieldName, field.getName().get()), field.getType());
            }
        }
        else {
            result.put(fieldName, type);
        }
    }

    private List<Decoder> createDecoders(List<ElasticsearchColumnHandle> columns)
    {
        return columns.stream()
                .map(ElasticsearchColumnHandle::getDecoderDescriptor)
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

    private static class SearchHitIterator
            extends AbstractIterator<SearchHit>
    {
        private final ElasticsearchClient client;
        private final Supplier<SearchResponse> first;
        private final OptionalLong limit;

        private SearchHits searchHits;
        private String scrollId;
        private int currentPosition;

        private long readTimeNanos;
        private long totalRecordCount;

        public SearchHitIterator(ElasticsearchClient client, Supplier<SearchResponse> first, OptionalLong limit)
        {
            this.client = client;
            this.first = first;
            this.limit = limit;
            this.totalRecordCount = 0;
        }

        public long getReadTimeNanos()
        {
            return readTimeNanos;
        }

        @Override
        protected SearchHit computeNext()
        {
            if (limit.isPresent() && totalRecordCount == limit.getAsLong()) {
                // No more record is necessary.
                return endOfData();
            }

            if (scrollId == null) {
                long start = System.nanoTime();
                SearchResponse response = first.get();
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }
            else if (currentPosition == searchHits.getHits().length) {
                long start = System.nanoTime();
                SearchResponse response = client.nextPage(scrollId);
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }

            if (currentPosition == searchHits.getHits().length) {
                return endOfData();
            }

            SearchHit hit = searchHits.getAt(currentPosition);
            currentPosition++;
            totalRecordCount++;

            return hit;
        }

        private void reset(SearchResponse response)
        {
            scrollId = response.getScrollId();
            searchHits = response.getHits();
            currentPosition = 0;
        }

        public void close()
        {
            if (scrollId != null) {
                try {
                    client.clearScroll(scrollId);
                }
                catch (Exception e) {
                    // ignore
                    LOG.debug(e, "Error clearing scroll");
                }
            }
        }
    }
}
