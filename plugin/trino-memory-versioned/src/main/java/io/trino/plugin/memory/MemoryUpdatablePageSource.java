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
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.UpdatablePageSource;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class MemoryUpdatablePageSource
        implements UpdatablePageSource
{
    private final ConnectorPageSource delegate;
    private final MemoryVersionedPagesStore versionedPagesStore;
    private final long tableId;
    private final long version;

    public MemoryUpdatablePageSource(ConnectorPageSource delegate, MemoryVersionedPagesStore versionedPagesStore, long tableId, long version)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.versionedPagesStore = requireNonNull(versionedPagesStore, "versionedPagesStore is null");
        this.tableId = tableId;
        this.version = version;
    }

    @Override
    public void deleteRows(Block rowIds)
    {
        versionedPagesStore.delete(tableId, version, rowIds);
    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {
        throw new UnsupportedOperationException("This connector does not support row-level delete");
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        return delegate.getNextPage();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }
}
