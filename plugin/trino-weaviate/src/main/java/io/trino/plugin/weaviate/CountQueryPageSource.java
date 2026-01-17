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
package io.trino.plugin.weaviate;

import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class CountQueryPageSource
        implements ConnectorPageSource
{
    // This implementation is used whenever a query doesn't reference any of the columns
    // in the Weaviate collection. We need to limit the number of rows per page, as some
    // queries may use projections which cause pages to explode.
    // See trino-elasticsearch/CountQueryPageSource.java.
    static final int MAX_PAGE_SIZE = 10_000;
    private final long readTimeNanos;
    private long remaining;

    public CountQueryPageSource(WeaviateService weaviateService, WeaviateTableHandle tableHandle)
    {
        requireNonNull(weaviateService, "weaviateService is null");
        requireNonNull(tableHandle, "tableHandle is null");

        long start = System.nanoTime();
        long count = weaviateService.countRows(tableHandle);
        readTimeNanos = System.nanoTime() - start;

        if (tableHandle.limit().isPresent()) {
            count = Math.min(tableHandle.limit().getAsLong(), count);
        }

        this.remaining = count;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        int pageSize = toIntExact(Math.min(MAX_PAGE_SIZE, remaining));
        remaining -= pageSize;

        return SourcePage.create(pageSize);
    }

    @Override
    public boolean isFinished()
    {
        return true;
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
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close() {}
}
