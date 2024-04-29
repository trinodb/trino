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

import io.trino.plugin.lance.internal.LanceDataFetcher;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.type.Type;
import java.util.List;

import static java.util.Objects.requireNonNull;


public class LanceFragmentPageSource
        implements ConnectorPageSource
{
    private final List<ColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private LanceConfig config;
    private final LanceDataFetcher lanceDataFetcher;

    // statistic variables
    private long completedBytes;
    private long estimatedMemoryUsageInBytes;
    private long readTimeNanos;
    private boolean closed;

    public LanceFragmentPageSource(
            ConnectorSplit split,
            List<ColumnHandle> columnHandles,
            LanceConfig config)
    {
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        // convert lance-to-trino type mapping
        this.columnTypes = createColumnType(columnHandles);
        this.config = config;
        this.lanceDataFetcher = new LanceDataFetcher(config, split);
    }

    private List<Type> createColumnType(List<ColumnHandle> columnHandles) {
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
    public long getMemoryUsage()
    {
        return estimatedMemoryUsageInBytes;
    }

    /**
     * @return true if is closed or all Pinot data have been processed.
     */
    @Override
    public boolean isFinished()
    {
        return closed;
    }

    /**
     * @return constructed page for pinot data.
     */
    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            close();
            return null;
        }
        return lanceDataFetcher.fetchBlock();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
    }
}
