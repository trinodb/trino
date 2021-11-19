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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import org.elasticsearch.common.collect.Tuple;

import java.io.IOException;
import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class PassthroughQueryPageSource
        implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(PassthroughQueryPageSource.class);

    private SearchHitIterator iterator;
    private long totalBytes;
    private final long readTimeNanos;
    private final Tuple<String, Boolean> result;
    private boolean done;

    public PassthroughQueryPageSource(ElasticsearchClient client, ElasticsearchTableHandle table)
    {
        requireNonNull(client, "client is null");
        requireNonNull(table, "table is null");

        long start = System.nanoTime();
        result = client.executeQuery(table.getIndex(), table.getQuery().get());
        readTimeNanos = System.nanoTime() - start;

        if (result.v2()) {
            this.iterator = new SearchHitIterator(client, result.v1());
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos + (iterator != null ? iterator.getReadTimeNanos() : 0);
    }

    @Override
    public boolean isFinished()
    {
        return (iterator != null && !iterator.hasNext()) || done;
    }

    @Override
    public Page getNextPage()
    {
        if (!done) {
            String hit;

            // Get next result
            if (iterator != null) {
                hit = iterator.next();
            }
            else {
                hit = result.v1();
                done = true;
            }

            totalBytes += hit.length();

            PageBuilder page = new PageBuilder(1, ImmutableList.of(VARCHAR));
            page.declarePosition();
            BlockBuilder column = page.getBlockBuilder(0);
            VARCHAR.writeSlice(column, Slices.utf8Slice(hit));
            return page.build();
        }

        return null;
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
        iterator.close();
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ScrollSearchResponse
    {
        @JsonProperty("_scroll_id")
        private String scrollId;

        @JsonProperty("hits")
        private Hits hits;

        public String getScrollId()
        {
            return scrollId;
        }

        public boolean hasMoreHits()
        {
            return hits != null && hits.getHits() != null && !hits.getHits().isEmpty();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class Hits
    {
        @JsonProperty("hits")
        private List<Object> hits;

        public List<Object> getHits()
        {
            return hits;
        }
    }

    private static class SearchHitIterator
            extends AbstractIterator<String>
    {
        private final ElasticsearchClient client;
        private final String first;

        private String scrollId;
        private Boolean hasMoreHits;
        private long readTimeNanos;
        String response;
        ObjectMapper objectMapper;

        public SearchHitIterator(ElasticsearchClient client, String first)
        {
            this.client = client;
            this.first = first;
            this.objectMapper = new ObjectMapper();
        }

        public long getReadTimeNanos()
        {
            return readTimeNanos;
        }

        @Override
        protected String computeNext()
        {
            long start = System.nanoTime();

            if (scrollId == null) {
                response = first;
            }
            else {
                response = client.executeScrollQuery(scrollId);
            }

            readTimeNanos += System.nanoTime() - start;
            reset(response);

            if (!hasMoreHits || scrollId == null) {
                return endOfData();
            }

            return response;
        }

        private void reset(String response)
        {
            ScrollSearchResponse scrollSearchResponse = extractSearchResponse(response);
            if (scrollSearchResponse != null) {
                scrollId = scrollSearchResponse.getScrollId();
                hasMoreHits = scrollSearchResponse.hasMoreHits();
            }
        }

        private ScrollSearchResponse extractSearchResponse(String response)
        {
            try {
                ScrollSearchResponse scrollSearchResponse = objectMapper.readValue(response, ScrollSearchResponse.class);
                if (scrollSearchResponse != null) {
                    return scrollSearchResponse;
                }
            }
            catch (JsonProcessingException e) {
                LOG.warn(e, "No Scroll_id returned from elasticSearch");
            }

            return null;
        }

        public void close()
        {
            if (scrollId != null) {
                try {
                    client.clearScroll(scrollId);
                }
                catch (Exception e) {
                    LOG.debug(e, "Error clearing scroll");
                }
            }
        }
    }
}
