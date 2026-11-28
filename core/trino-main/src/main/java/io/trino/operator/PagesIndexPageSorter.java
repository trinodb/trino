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
package io.trino.operator;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.spi.Page;
import io.trino.spi.PageSorter;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.function.LongConsumer;

import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static java.util.Objects.requireNonNull;

public class PagesIndexPageSorter
        implements PageSorter
{
    private final PagesIndex.Factory pagesIndexFactory;

    @Inject
    public PagesIndexPageSorter(PagesIndex.Factory pagesIndexFactory)
    {
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
    }

    @Override
    public Iterator<Page> sort(List<Type> types, List<Page> pages, List<Integer> sortChannels, List<SortOrder> sortOrders, int expectedPositions, LongConsumer memoryUsage)
    {
        PagesIndex pagesIndex = pagesIndexFactory.newPagesIndex(types, expectedPositions);
        pages.forEach(pagesIndex::addPage);

        LocalMemoryContext memoryContext = new MemoryUsageReporter(memoryUsage);
        memoryContext.setBytes(pagesIndex.getEstimatedSize().toBytes());
        pagesIndex.sort(sortChannels, sortOrders, memoryContext);

        return pagesIndex.getSortedPages();
    }

    /**
     * Adapts the SPI memory usage consumer to the memory context the sort accounts its
     * working memory in.
     */
    private static class MemoryUsageReporter
            implements LocalMemoryContext
    {
        private final LongConsumer memoryUsage;
        private long bytes;

        private MemoryUsageReporter(LongConsumer memoryUsage)
        {
            this.memoryUsage = requireNonNull(memoryUsage, "memoryUsage is null");
        }

        @Override
        public long getBytes()
        {
            return bytes;
        }

        @Override
        public ListenableFuture<Void> setBytes(long bytes)
        {
            this.bytes = bytes;
            memoryUsage.accept(bytes);
            return immediateVoidFuture();
        }

        @Override
        public ListenableFuture<Void> addBytes(long delta)
        {
            return setBytes(bytes + delta);
        }

        @Override
        public boolean trySetBytes(long bytes)
        {
            setBytes(bytes);
            return true;
        }

        @Override
        public void close()
        {
            setBytes(0);
        }
    }
}
