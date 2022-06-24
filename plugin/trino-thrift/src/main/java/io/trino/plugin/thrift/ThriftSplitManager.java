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
package io.trino.plugin.thrift;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.client.DriftClient;
import io.trino.plugin.thrift.api.TrinoThriftHostAddress;
import io.trino.plugin.thrift.api.TrinoThriftId;
import io.trino.plugin.thrift.api.TrinoThriftNullableColumnSet;
import io.trino.plugin.thrift.api.TrinoThriftNullableToken;
import io.trino.plugin.thrift.api.TrinoThriftSchemaTableName;
import io.trino.plugin.thrift.api.TrinoThriftService;
import io.trino.plugin.thrift.api.TrinoThriftSplit;
import io.trino.plugin.thrift.api.TrinoThriftSplitBatch;
import io.trino.plugin.thrift.api.TrinoThriftTupleDomain;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.thrift.util.ThriftExceptions.catchingThriftException;
import static io.trino.plugin.thrift.util.TupleDomainConversion.tupleDomainToThriftTupleDomain;
import static java.util.Objects.requireNonNull;

public class ThriftSplitManager
        implements ConnectorSplitManager
{
    private final DriftClient<TrinoThriftService> client;
    private final ThriftHeaderProvider thriftHeaderProvider;

    @Inject
    public ThriftSplitManager(DriftClient<TrinoThriftService> client, ThriftHeaderProvider thriftHeaderProvider)
    {
        this.client = requireNonNull(client, "client is null");
        this.thriftHeaderProvider = requireNonNull(thriftHeaderProvider, "thriftHeaderProvider is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        ThriftTableHandle tableHandle = (ThriftTableHandle) table;
        return new ThriftSplitSource(
                client.get(thriftHeaderProvider.getHeaders(session)),
                new TrinoThriftSchemaTableName(tableHandle.getSchemaName(), tableHandle.getTableName()),
                tableHandle.getDesiredColumns().map(ThriftSplitManager::columnNames),
                tupleDomainToThriftTupleDomain(tableHandle.getConstraint()));
    }

    private static Set<String> columnNames(Set<ColumnHandle> columns)
    {
        return columns.stream()
                .map(ThriftColumnHandle.class::cast)
                .map(ThriftColumnHandle::getColumnName)
                .collect(toImmutableSet());
    }

    @NotThreadSafe
    private static class ThriftSplitSource
            implements ConnectorSplitSource
    {
        private final TrinoThriftService client;
        private final TrinoThriftSchemaTableName schemaTableName;
        private final Optional<Set<String>> columnNames;
        private final TrinoThriftTupleDomain constraint;

        // the code assumes getNextBatch is called by a single thread

        private final AtomicBoolean hasMoreData;
        private final AtomicReference<TrinoThriftId> nextToken;
        private final AtomicReference<Future<?>> future;

        public ThriftSplitSource(
                TrinoThriftService client,
                TrinoThriftSchemaTableName schemaTableName,
                Optional<Set<String>> columnNames,
                TrinoThriftTupleDomain constraint)
        {
            this.client = requireNonNull(client, "client is null");
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.columnNames = requireNonNull(columnNames, "columnNames is null");
            this.constraint = requireNonNull(constraint, "constraint is null");
            this.nextToken = new AtomicReference<>(null);
            this.hasMoreData = new AtomicBoolean(true);
            this.future = new AtomicReference<>(null);
        }

        /**
         * Returns a future with a list of splits.
         * This method is assumed to be called in a single-threaded way.
         * It can be called by multiple threads, but only if the previous call finished.
         */
        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            checkState(future.get() == null || future.get().isDone(), "previous batch not completed");
            checkState(hasMoreData.get(), "this method cannot be invoked when there's no more data");
            TrinoThriftId currentToken = nextToken.get();
            ListenableFuture<TrinoThriftSplitBatch> splitsFuture = client.getSplits(
                    schemaTableName,
                    new TrinoThriftNullableColumnSet(columnNames.orElse(null)),
                    constraint,
                    maxSize,
                    new TrinoThriftNullableToken(currentToken));
            ListenableFuture<ConnectorSplitBatch> resultFuture = Futures.transform(
                    splitsFuture,
                    batch -> {
                        requireNonNull(batch, "batch is null");
                        List<ConnectorSplit> splits = batch.getSplits().stream()
                                .map(ThriftSplitSource::toConnectorSplit)
                                .collect(toImmutableList());
                        checkState(nextToken.compareAndSet(currentToken, batch.getNextToken()));
                        checkState(hasMoreData.compareAndSet(true, nextToken.get() != null));
                        return new ConnectorSplitBatch(splits, isFinished());
                    }, directExecutor());
            resultFuture = catchingThriftException(resultFuture);
            future.set(resultFuture);
            return toCompletableFuture(resultFuture);
        }

        @Override
        public boolean isFinished()
        {
            return !hasMoreData.get();
        }

        @Override
        public void close()
        {
            Future<?> currentFuture = future.getAndSet(null);
            if (currentFuture != null) {
                currentFuture.cancel(true);
            }
        }

        private static ThriftConnectorSplit toConnectorSplit(TrinoThriftSplit thriftSplit)
        {
            return new ThriftConnectorSplit(
                    thriftSplit.getSplitId(),
                    toHostAddressList(thriftSplit.getHosts()));
        }

        private static List<HostAddress> toHostAddressList(List<TrinoThriftHostAddress> hosts)
        {
            return hosts.stream().map(TrinoThriftHostAddress::toHostAddress).collect(toImmutableList());
        }
    }
}
