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
package io.prestosql.plugin.raptor.legacy;

import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.plugin.raptor.legacy.storage.StorageManager;
import io.prestosql.plugin.raptor.legacy.util.ConcatPageSource;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

import static io.prestosql.plugin.raptor.legacy.RaptorSessionProperties.getReaderMaxMergeDistance;
import static io.prestosql.plugin.raptor.legacy.RaptorSessionProperties.getReaderMaxReadSize;
import static io.prestosql.plugin.raptor.legacy.RaptorSessionProperties.getReaderStreamBufferSize;
import static io.prestosql.plugin.raptor.legacy.RaptorSessionProperties.getReaderTinyStripeThreshold;
import static io.prestosql.plugin.raptor.legacy.RaptorSessionProperties.isReaderLazyReadSmallRanges;
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
            TupleDomain<ColumnHandle> dynamicFilter)
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

        OptionalLong transactionId = raptorSplit.getTransactionId();

        if (raptorSplit.getShardUuids().size() == 1) {
            UUID shardUuid = raptorSplit.getShardUuids().iterator().next();
            return createPageSource(shardUuid, bucketNumber, columns, predicate, options, transactionId);
        }

        Iterator<ConnectorPageSource> iterator = raptorSplit.getShardUuids().stream()
                .map(shardUuid -> createPageSource(shardUuid, bucketNumber, columns, predicate, options, transactionId))
                .iterator();

        return new ConcatPageSource(iterator);
    }

    private ConnectorPageSource createPageSource(
            UUID shardUuid,
            OptionalInt bucketNumber,
            List<ColumnHandle> columns,
            TupleDomain<RaptorColumnHandle> predicate,
            OrcReaderOptions orcReaderOptions,
            OptionalLong transactionId)
    {
        List<RaptorColumnHandle> columnHandles = columns.stream().map(RaptorColumnHandle.class::cast).collect(toList());
        List<Long> columnIds = columnHandles.stream().map(RaptorColumnHandle::getColumnId).collect(toList());
        List<Type> columnTypes = columnHandles.stream().map(RaptorColumnHandle::getColumnType).collect(toList());

        return storageManager.getPageSource(shardUuid, bucketNumber, columnIds, columnTypes, predicate, orcReaderOptions, transactionId);
    }
}
