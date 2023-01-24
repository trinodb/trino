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
package io.trino.plugin.iceberg.aggregation;

import io.trino.plugin.iceberg.IcebergColumnHandle;
import io.trino.plugin.iceberg.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

public class AggregatePageSource
        implements ConnectorPageSource
{
    private final List<Type> columnTypes;
    private long readTimeNanos;
    private Iterator<Page> pages;
    private final long recordCount;

    public AggregatePageSource(List<IcebergColumnHandle> columnHandles, long recordCount)
    {
        // _pos columns are not required.
        this.columnTypes = columnHandles.stream().filter(columnHandle -> !columnHandle.isRowPositionColumn()).map(ch -> ch.getType()).collect(toList());
        this.recordCount = recordCount;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    /**
     * Gets the number of input rows processed by this page source so far.
     * By default, the positions count of the page returned from getNextPage
     * is used to calculate the number of input rows.
     */
    @Override
    public OptionalLong getCompletedPositions()
    {
        return ConnectorPageSource.super.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return pages != null && !pages.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (pages != null && pages.hasNext()) {
            return pages.next();
        }

        long start = System.nanoTime();
        PageListBuilder pageListBuilder = new PageListBuilder(columnTypes);

        pageListBuilder.beginRow();
        pageListBuilder.appendBigint(recordCount);
        pageListBuilder.endRow();

        this.readTimeNanos += System.nanoTime() - start;
        this.pages = pageListBuilder.build().iterator();
        return pages.next();
    }

    /**
     * Get the total memory that needs to be reserved in the memory pool.
     * This memory should include any buffers, etc. that are used for reading data.
     *
     * @return the memory used so far in table read
     */
    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
    }

    /**
     * Returns a future that will be completed when the page source becomes
     * unblocked.  If the page source is not blocked, this method should return
     * {@code NOT_BLOCKED}.
     */
    @Override
    public CompletableFuture<?> isBlocked()
    {
        return ConnectorPageSource.super.isBlocked();
    }

    /**
     * Returns the connector's metrics, mapping a metric ID to its latest value.
     * Each call must return an immutable snapshot of available metrics.
     * Same ID metrics are merged across all tasks and exposed via OperatorStats.
     * This method can be called after the page source is closed.
     */
    @Override
    public Metrics getMetrics()
    {
        return ConnectorPageSource.super.getMetrics();
    }
}
