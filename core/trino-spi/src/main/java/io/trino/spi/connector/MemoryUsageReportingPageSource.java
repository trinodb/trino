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
import io.trino.spi.block.Block;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.ObjLongConsumer;

import static java.util.Objects.requireNonNull;

/// A [ConnectorPageSource] wrapper that polls the delegate's
/// [ConnectorPageSource#getMemoryUsage()] and forwards to [MemoryContext].
public final class MemoryUsageReportingPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource pageSource;
    private final MemoryContext memoryContext;

    public MemoryUsageReportingPageSource(ConnectorPageSource pageSource, MemoryContext memoryContext)
    {
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        memoryContext.setBytes(pageSource.getMemoryUsage());
    }

    @Override
    public long getCompletedBytes()
    {
        return pageSource.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return pageSource.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return pageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        boolean finished = pageSource.isFinished();
        memoryContext.setBytes(pageSource.getMemoryUsage());
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        SourcePage page = pageSource.getNextSourcePage();
        memoryContext.setBytes(pageSource.getMemoryUsage());
        return page == null ? null : new MemoryUsageReportingSourcePage(page);
    }

    @Override
    public long getMemoryUsage()
    {
        return pageSource.getMemoryUsage();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return pageSource.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return pageSource.getMetrics();
    }

    @Override
    public void close()
            throws IOException
    {
        pageSource.close();
        memoryContext.setBytes(pageSource.getMemoryUsage());
    }

    class MemoryUsageReportingSourcePage
            implements SourcePage
    {
        private final SourcePage sourcePage;

        public MemoryUsageReportingSourcePage(SourcePage sourcePage)
        {
            this.sourcePage = requireNonNull(sourcePage, "sourcePage is null");
        }

        @Override
        public int getPositionCount()
        {
            return sourcePage.getPositionCount();
        }

        @Override
        public long getSizeInBytes()
        {
            return sourcePage.getSizeInBytes();
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return sourcePage.getRetainedSizeInBytes();
        }

        @Override
        public void retainedBytesForEachPart(ObjLongConsumer<Object> consumer)
        {
            sourcePage.retainedBytesForEachPart(consumer);
        }

        @Override
        public int getChannelCount()
        {
            return sourcePage.getChannelCount();
        }

        @Override
        public Block getBlock(int channel)
        {
            Block block = sourcePage.getBlock(channel);
            memoryContext.setBytes(pageSource.getMemoryUsage());
            return block;
        }

        @Override
        public Page getPage()
        {
            Page page = sourcePage.getPage();
            memoryContext.setBytes(pageSource.getMemoryUsage());
            return page;
        }

        @Override
        public Page getColumns(int[] channels)
        {
            Page page = sourcePage.getColumns(channels);
            memoryContext.setBytes(pageSource.getMemoryUsage());
            return page;
        }

        @Override
        public void selectPositions(int[] positions, int offset, int size)
        {
            sourcePage.selectPositions(positions, offset, size);
            memoryContext.setBytes(pageSource.getMemoryUsage());
        }
    }
}
