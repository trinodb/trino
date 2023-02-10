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
package io.trino.plugin.iceberg;

import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;

public class IncrementalPopulatingPageSource
        implements ConnectorPageSource
{
    private final CloseableIterator<Page> pages;
    private final long memoryUsageBytes;

    private long completedBytes;
    private boolean closed;

    public IncrementalPopulatingPageSource(CloseableIterable<Page> pages, long estimatedMemoryUsageBytes)
    {
        this.pages = pages.iterator();
        this.memoryUsageBytes = estimatedMemoryUsageBytes;
    }

    @Override
    public void close()
            throws IOException
    {
        pages.close();
        closed = true;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return closed || !pages.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            return null;
        }
        Page page = pages.next();
        completedBytes += page.getSizeInBytes();
        return page;
    }

    @Override
    public long getMemoryUsage()
    {
        return memoryUsageBytes;
    }
}
