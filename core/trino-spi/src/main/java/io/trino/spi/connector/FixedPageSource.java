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
package io.trino.spi.connector;

import io.trino.spi.Page;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class FixedPageSource
        implements ConnectorPageSource
{
    private Iterator<Page> pages;
    private final MemoryContext memoryContext;

    private long completedBytes;
    private boolean closed;

    /**
     * Reports the retained size of {@code pages} to {@code memoryContext}.
     */
    public FixedPageSource(List<Page> pages, MemoryContext memoryContext)
    {
        this(pages.iterator(), memoryUsage(pages), memoryContext);
    }

    private static long memoryUsage(List<Page> pages)
    {
        long memoryUsageBytes = 0;
        for (Page page : pages) {
            memoryUsageBytes += page.getRetainedSizeInBytes();
        }
        return memoryUsageBytes;
    }

    /**
     * Reports {@code memoryUsageBytes} to {@code memoryContext}.
     */
    public FixedPageSource(Iterator<Page> pages, long memoryUsageBytes, MemoryContext memoryContext)
    {
        this.pages = requireNonNull(pages, "pages is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        memoryContext.setBytes(memoryUsageBytes);
    }

    @Override
    public void close()
    {
        closed = true;
        // Drop the remaining pages, so that they are not retained after their memory is released
        pages = Collections.emptyIterator();
        memoryContext.setBytes(0);
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
    public SourcePage getNextSourcePage()
    {
        if (isFinished()) {
            return null;
        }
        Page page = pages.next();
        completedBytes += page.getSizeInBytes();
        return SourcePage.create(page);
    }
}
