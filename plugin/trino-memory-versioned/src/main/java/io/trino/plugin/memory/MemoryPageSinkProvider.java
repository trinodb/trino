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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MemoryPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final MemoryPagesStore pagesStore;
    private final MemoryVersionedPagesStore versionedPagesStore;

    @Inject
    public MemoryPageSinkProvider(MemoryPagesStore pagesStore, MemoryVersionedPagesStore versionedPagesStore)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
        this.versionedPagesStore = requireNonNull(versionedPagesStore, "versionedPagesStore is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        MemoryOutputTableHandle memoryOutputTableHandle = (MemoryOutputTableHandle) outputTableHandle;
        return createPageSink(
                memoryOutputTableHandle.getActiveTableIds(),
                memoryOutputTableHandle.getTable(),
                memoryOutputTableHandle.getVersion(),
                memoryOutputTableHandle.getKeyColumnIndex());
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        MemoryInsertTableHandle memoryInsertTableHandle = (MemoryInsertTableHandle) insertTableHandle;
        return createPageSink(
                memoryInsertTableHandle.getActiveTableIds(),
                memoryInsertTableHandle.getTable(),
                memoryInsertTableHandle.getVersion(),
                memoryInsertTableHandle.getKeyColumnIndex());
    }

    private MemoryPageSink createPageSink(Set<Long> activeTablesIds, long tableId, Optional<Long> version, Optional<Integer> keyColumnIndex)
    {
        checkState(activeTablesIds.contains(tableId));

        pagesStore.cleanUp(activeTablesIds);
        versionedPagesStore.cleanUp(activeTablesIds);

        if (version.isEmpty()) {
            pagesStore.initialize(tableId);
            return new MemoryPageSink(page -> pagesStore.add(tableId, page));
        }

        versionedPagesStore.initialize(tableId);
        return new MemoryPageSink(page -> versionedPagesStore.add(tableId, version.get(), keyColumnIndex.orElseThrow(), page));
    }

    private static class MemoryPageSink
            implements ConnectorPageSink
    {
        private final Consumer<Page> pagesConsumer;
        private final List<Page> appendedPages = new ArrayList<>();

        public MemoryPageSink(Consumer<Page> pagesConsumer)
        {
            this.pagesConsumer = requireNonNull(pagesConsumer, "pagesConsumer is null");
        }

        @Override
        public CompletableFuture<?> appendPage(Page page)
        {
            appendedPages.add(page);
            return NOT_BLOCKED;
        }

        @Override
        public CompletableFuture<Collection<Slice>> finish()
        {
            // add pages to pagesStore
            for (Page page : appendedPages) {
                pagesConsumer.accept(page);
            }

            return completedFuture(ImmutableList.of());
        }

        @Override
        public void abort()
        {
        }
    }
}
