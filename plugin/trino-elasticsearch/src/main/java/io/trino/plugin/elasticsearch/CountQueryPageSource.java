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

import io.trino.plugin.elasticsearch.client.ElasticsearchClient;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import static io.trino.plugin.elasticsearch.ElasticsearchQueryBuilder.buildSearchQuery;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

class CountQueryPageSource
        implements ConnectorPageSource
{
    // This implementation of the page source is used whenever a query doesn't reference any columns
    // from the ES table. We need to limit the number of rows per page in case there are projections
    // in the query that can cause page sizes to explode. For example: SELECT rand() FROM some_table
    // TODO (https://github.com/trinodb/trino/issues/16824) allow connector to return pages of arbitrary row count and handle this gracefully in engine
    private static final int BATCH_SIZE = 10000;

    private final long readTimeNanos;
    private long remaining;

    public CountQueryPageSource(ElasticsearchClient client, ElasticsearchTableHandle table, ElasticsearchSplit split)
    {
        requireNonNull(client, "client is null");
        requireNonNull(table, "table is null");
        requireNonNull(split, "split is null");

        long start = System.nanoTime();
        long count = client.count(
                split.index(),
                split.shard(),
                buildSearchQuery(table.constraint().transformKeys(ElasticsearchColumnHandle.class::cast), table.query(), table.regexes()));
        readTimeNanos = System.nanoTime() - start;

        if (table.limit().isPresent()) {
            count = Math.min(table.limit().getAsLong(), count);
        }

        remaining = count;
    }

    @Override
    public boolean isFinished()
    {
        return remaining == 0;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        int batch = toIntExact(Math.min(BATCH_SIZE, remaining));
        remaining -= batch;

        return SourcePage.create(batch);
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
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() {}
}
