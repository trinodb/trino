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
package io.trino.plugin.raptor.legacy;

import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.plugin.raptor.legacy.storage.StorageManager;
import io.trino.plugin.raptor.legacy.storage.StorageManagerConfig;
import io.trino.spi.PageSorter;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorMergeTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RaptorPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final StorageManager storageManager;
    private final PageSorter pageSorter;
    private final DataSize maxBufferSize;

    @Inject
    public RaptorPageSinkProvider(StorageManager storageManager, PageSorter pageSorter, StorageManagerConfig config)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.maxBufferSize = config.getMaxBufferSize();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        RaptorOutputTableHandle handle = (RaptorOutputTableHandle) tableHandle;
        return new RaptorPageSink(
                pageSorter,
                storageManager,
                handle.getTransactionId(),
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders(),
                handle.getBucketCount(),
                toColumnIds(handle.getBucketColumnHandles()),
                handle.getTemporalColumnHandle(),
                maxBufferSize);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, ConnectorPageSinkId pageSinkId)
    {
        RaptorInsertTableHandle handle = (RaptorInsertTableHandle) tableHandle;
        return new RaptorPageSink(
                pageSorter,
                storageManager,
                handle.getTransactionId(),
                toColumnIds(handle.getColumnHandles()),
                handle.getColumnTypes(),
                toColumnIds(handle.getSortColumnHandles()),
                handle.getSortOrders(),
                handle.getBucketCount(),
                toColumnIds(handle.getBucketColumnHandles()),
                handle.getTemporalColumnHandle(),
                maxBufferSize);
    }

    @Override
    public ConnectorMergeSink createMergeSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorMergeTableHandle mergeHandle, ConnectorPageSinkId pageSinkId)
    {
        RaptorMergeTableHandle merge = (RaptorMergeTableHandle) mergeHandle;
        ConnectorPageSink pageSink = createPageSink(transactionHandle, session, merge.getInsertTableHandle(), pageSinkId);
        long transactionId = merge.getInsertTableHandle().getTransactionId();
        int columnCount = merge.getInsertTableHandle().getColumnHandles().size();
        return new RaptorMergeSink(pageSink, storageManager, transactionId, columnCount);
    }

    private static List<Long> toColumnIds(List<RaptorColumnHandle> columnHandles)
    {
        return columnHandles.stream().map(RaptorColumnHandle::getColumnId).collect(toList());
    }
}
