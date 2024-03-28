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
package io.trino.plugin.opensearch;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.opensearch.client.OpenSearchClient;
import io.trino.plugin.opensearch.decoders.Decoder;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.opensearch.BuiltinColumns.SOURCE;
import static io.trino.plugin.opensearch.BuiltinColumns.isBuiltinColumn;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.isEqual;
import static java.util.stream.Collectors.toList;

public class PITScanQueryPageSource
        implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(PITScanQueryPageSource.class);

    private final List<Decoder> decoders;

    private final PITSearchHitIterator iterator;
    private final BlockBuilder[] columnBuilders;
    private final List<OpenSearchColumnHandle> columns;
    private long totalBytes;
    private long readTimeNanos;

    public PITScanQueryPageSource(
            OpenSearchClient client,
            TypeManager typeManager,
            OpenSearchTableHandle table,
            OpenSearchSplit split,
            List<OpenSearchColumnHandle> columns)
    {
        requireNonNull(client, "client is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(columns, "columns is null");

        this.columns = ImmutableList.copyOf(columns);

        decoders = createDecoders(columns);

        // When the _source field is requested, we need to bypass column pruning when fetching the document
        boolean needAllFields = columns.stream()
                .map(OpenSearchColumnHandle::getName)
                .anyMatch(isEqual(SOURCE.getName()));

        // Columns to fetch as doc_fields instead of pulling them out of the JSON source
        // This is convenient for types such as DATE, TIMESTAMP, etc, which have multiple possible
        // representations in JSON, but a single normalized representation as doc_field.
        List<String> documentFields = flattenFields(columns).entrySet().stream()
                .filter(entry -> entry.getValue().equals(TIMESTAMP_MILLIS))
                .map(Map.Entry::getKey)
                .collect(toImmutableList());

        columnBuilders = columns.stream()
                .map(OpenSearchColumnHandle::getType)
                .map(type -> type.createBlockBuilder(null, 1))
                .toArray(BlockBuilder[]::new);

        List<String> requiredFields = columns.stream()
                .map(OpenSearchColumnHandle::getName)
                .filter(name -> !isBuiltinColumn(name))
                .collect(toList());

        // sorting by _doc (index order) get special treatment in OpenSearch and is more efficient
        Optional<String> sort = Optional.of("_doc");

        if (table.getQuery().isPresent()) {
            // However, if we're using a custom OpenSearch query, use default sorting.
            // Documents will be scored and returned based on relevance
            sort = Optional.empty();
        }

        long start = System.nanoTime();

        QueryBuilder queryBuilder = OpenSearchQueryBuilder.buildSearchQuery(
                table.getConstraint().transformKeys(OpenSearchColumnHandle.class::cast),
                table.getQuery(),
                table.getRegexes());
        Optional<List<String>> fields = needAllFields ? Optional.empty() : Optional.of(requiredFields);

        SearchResponse searchResponse = client.pitSearch(
                split.getIndex(),
                split.getShard(),
                split.getPitId(),
                split.getShardCount(),
                queryBuilder,
                fields,
                documentFields,
                sort,
                table.getLimit(), null);
        readTimeNanos += System.nanoTime() - start;
        System.out.println("Received response from PIT search docs retreived : " + searchResponse.getHits().getHits().length + " for split: " + split.getShard());
        this.iterator = new PITSearchHitIterator(client, () -> searchResponse, table.getLimit(), split, queryBuilder, fields, documentFields, sort);
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
    public long getMemoryUsage()
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

    private Map<String, Type> flattenFields(List<OpenSearchColumnHandle> columns)
    {
        Map<String, Type> result = new HashMap<>();

        for (OpenSearchColumnHandle column : columns) {
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

    private List<Decoder> createDecoders(List<OpenSearchColumnHandle> columns)
    {
        return columns.stream()
                .map(OpenSearchColumnHandle::getDecoderDescriptor)
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

    private static class PITSearchHitIterator
            extends AbstractIterator<SearchHit>
    {
        private final OpenSearchClient client;
        private final Supplier<SearchResponse> first;
        private final OptionalLong limit;
        private SearchHits searchHits;
        private String pitId;
        private int currentPosition;
        private long readTimeNanos;
        private long totalRecordCount;
        private Object[] searchAfterValues;
        private final OpenSearchSplit split;
        private QueryBuilder queryBuilder;
        private Optional<List<String>> fields;
        private List<String> documentFields;
        private Optional<String> sort;

        public PITSearchHitIterator(OpenSearchClient client, Supplier<SearchResponse> first, OptionalLong limit, OpenSearchSplit split, QueryBuilder queryBuilder, Optional<List<String>> fields, List<String> documentFields, Optional<String> sort)
        {
            this.client = client;
            this.first = first;
            this.limit = limit;
            this.totalRecordCount = 0;
            this.split = split;
            this.queryBuilder = queryBuilder;
            this.fields = fields;
            this.documentFields = documentFields;
            this.sort = sort;
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

            if (pitId == null) {
                long start = System.nanoTime();
                SearchResponse response = first.get();
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }
            else if (currentPosition == searchHits.getHits().length) {
                long start = System.nanoTime();
                System.out.println("Fetching search again , searchAfterValues: " + searchAfterValues);
                SearchResponse response = client.pitSearch(
                        split.getIndex(),
                        split.getShard(),
                        pitId,
                        split.getShardCount(),
                        queryBuilder,
                        fields,
                        documentFields,
                        sort,
                        limit,
                        searchAfterValues);
                readTimeNanos += System.nanoTime() - start;
                reset(response);
            }

            if (currentPosition == searchHits.getHits().length) {
                return endOfData();
            }

            SearchHit hit = searchHits.getAt(currentPosition);
            searchAfterValues = hit.getSortValues();
            currentPosition++;
            totalRecordCount++;

            return hit;
        }

        private void reset(SearchResponse response)
        {
            pitId = response.pointInTimeId();
            searchHits = response.getHits();
            currentPosition = 0;
        }

        public void close()
        {
            if (pitId != null) {
                try {
                    System.out.println("Finished scanning, PIT: " + pitId + " split: " + split.getShard() + " current position: " + currentPosition);
                    //client.deletePit(pitId);
                    //throw new Exception("Deleting PIT");
                }
                catch (Exception e) {
                    // ignore
                    LOG.debug(e, "Error Deleting PIT");
                }
            }
        }
    }
}
