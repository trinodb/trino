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
package io.trino.plugin.lance;

import io.trino.plugin.lance.internal.LanceClient;
import io.trino.plugin.lance.internal.LanceDynamicTable;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;


public class LanceHttpPageSource
        implements ConnectorPageSource
{
    private final BlockBuilder[] columnBuilders;
    private final LanceDynamicTable query;
    private final Iterator<Page> pageIterator;

    private long readTimeNanos;
    private long estimatedMemoryUsageInBytes;
    private long completedBytes;
    private boolean finished;

    public LanceHttpPageSource(
            ConnectorSession session,
            LanceDynamicTable query,
            List<ColumnHandle> columnHandles,
            LanceClient lanceClient)
    {
        this.query = requireNonNull(query, "query is null");
        // convert lance-to-trino column builder
        this.columnBuilders = createColumnBuilder(columnHandles);
        long start = System.nanoTime();
        // when creating the page iterator, data is already fetched by the lance client.
        this.pageIterator = lanceClient.readTable(query);
        readTimeNanos = System.nanoTime() - start;
    }

    private BlockBuilder[] createColumnBuilder(List<ColumnHandle> columnHandles) {
        // TODO: implement me
        return null;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }
        if (!pageIterator.hasNext()) {
            finished = true;
            return null;
        } else {
            // TODO: implement this:
            return pageIterator.next();
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return estimatedMemoryUsageInBytes;
    }

    @Override
    public void close() {}
}
