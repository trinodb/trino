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

import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.raptor.legacy.storage.StorageManager;
import io.trino.plugin.raptor.legacy.util.ConcatPageSource;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;
import java.util.UUID;

import static io.trino.plugin.raptor.legacy.RaptorSessionProperties.getReaderMaxMergeDistance;
import static io.trino.plugin.raptor.legacy.RaptorSessionProperties.getReaderMaxReadSize;
import static io.trino.plugin.raptor.legacy.RaptorSessionProperties.getReaderStreamBufferSize;
import static io.trino.plugin.raptor.legacy.RaptorSessionProperties.getReaderTinyStripeThreshold;
import static io.trino.plugin.raptor.legacy.RaptorSessionProperties.isReaderLazyReadSmallRanges;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RaptorPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final StorageManager storageManager;

    @Inject
    public RaptorPageSourceProvider(StorageManager storageManager)
    {
        this.storageManager = requireNonNull(storageManager, "storageManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        RaptorSplit raptorSplit = (RaptorSplit) split;
        RaptorTableHandle raptorTable = (RaptorTableHandle) table;

        OptionalInt bucketNumber = raptorSplit.getBucketNumber();
        TupleDomain<RaptorColumnHandle> predicate = raptorTable.getConstraint();
        OrcReaderOptions options = new OrcReaderOptions()
                .withMaxMergeDistance(getReaderMaxMergeDistance(session))
                .withMaxBufferSize(getReaderMaxReadSize(session))
                .withStreamBufferSize(getReaderStreamBufferSize(session))
                .withTinyStripeThreshold(getReaderTinyStripeThreshold(session))
                .withLazyReadSmallRanges(isReaderLazyReadSmallRanges(session));

        if (raptorSplit.getShardUuids().size() == 1) {
            UUID shardUuid = raptorSplit.getShardUuids().iterator().next();
            return createPageSource(shardUuid, bucketNumber, columns, predicate, options);
        }

        Iterator<ConnectorPageSource> iterator = raptorSplit.getShardUuids().stream()
                .map(shardUuid -> createPageSource(shardUuid, bucketNumber, columns, predicate, options))
                .iterator();

        return new ConcatPageSource(iterator);
    }

    private ConnectorPageSource createPageSource(
            UUID shardUuid,
            OptionalInt bucketNumber,
            List<ColumnHandle> columns,
            TupleDomain<RaptorColumnHandle> predicate,
            OrcReaderOptions orcReaderOptions)
    {
        List<RaptorColumnHandle> columnHandles = columns.stream().map(RaptorColumnHandle.class::cast).collect(toList());
        List<Long> columnIds = columnHandles.stream().map(RaptorColumnHandle::getColumnId).collect(toList());
        List<Type> columnTypes = columnHandles.stream().map(RaptorColumnHandle::getColumnType).collect(toList());

        return storageManager.getPageSource(shardUuid, bucketNumber, columnIds, columnTypes, predicate, orcReaderOptions);
    }
}
