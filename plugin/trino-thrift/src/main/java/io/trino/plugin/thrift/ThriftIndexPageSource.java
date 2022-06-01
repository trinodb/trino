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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.client.DriftClient;
import io.trino.plugin.thrift.api.TrinoThriftId;
import io.trino.plugin.thrift.api.TrinoThriftNullableToken;
import io.trino.plugin.thrift.api.TrinoThriftPageResult;
import io.trino.plugin.thrift.api.TrinoThriftSchemaTableName;
import io.trino.plugin.thrift.api.TrinoThriftService;
import io.trino.plugin.thrift.api.TrinoThriftSplit;
import io.trino.plugin.thrift.api.TrinoThriftSplitBatch;
import io.trino.plugin.thrift.api.TrinoThriftTupleDomain;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.trino.plugin.thrift.api.TrinoThriftPageResult.fromRecordSet;
import static io.trino.plugin.thrift.util.ThriftExceptions.catchingThriftException;
import static io.trino.plugin.thrift.util.TupleDomainConversion.tupleDomainToThriftTupleDomain;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class ThriftIndexPageSource
        implements ConnectorPageSource
{
    private static final int MAX_SPLIT_COUNT = 10_000_000;

    private final DriftClient<TrinoThriftService> client;
    private final Map<String, String> thriftHeaders;
    private final TrinoThriftSchemaTableName schemaTableName;
    private final List<String> lookupColumnNames;
    private final List<String> outputColumnNames;
    private final List<Type> outputColumnTypes;
    private final TrinoThriftTupleDomain outputConstraint;
    private final TrinoThriftPageResult keys;
    private final long maxBytesPerResponse;
    private final int lookupRequestsConcurrency;

    private final AtomicLong readTimeNanos = new AtomicLong(0);
    private long completedBytes;

    private CompletableFuture<?> statusFuture;
    private ListenableFuture<TrinoThriftSplitBatch> splitFuture;
    private ListenableFuture<TrinoThriftPageResult> dataSignalFuture;

    private final List<TrinoThriftSplit> splits = new ArrayList<>();
    private final Queue<ListenableFuture<TrinoThriftPageResult>> dataRequests = new LinkedList<>();
    private final Map<ListenableFuture<TrinoThriftPageResult>, RunningSplitContext> contexts;
    private final ThriftConnectorStats stats;

    private int splitIndex;
    private boolean haveSplits;
    private boolean finished;

    public ThriftIndexPageSource(
            DriftClient<TrinoThriftService> client,
            Map<String, String> thriftHeaders,
            ThriftConnectorStats stats,
            ThriftIndexHandle indexHandle,
            List<ColumnHandle> lookupColumns,
            List<ColumnHandle> outputColumns,
            RecordSet keys,
            long maxBytesPerResponse,
            int lookupRequestsConcurrency)
    {
        this.client = requireNonNull(client, "client is null");
        this.thriftHeaders = requireNonNull(thriftHeaders, "thriftHeaders is null");
        this.stats = requireNonNull(stats, "stats is null");

        requireNonNull(indexHandle, "indexHandle is null");
        this.schemaTableName = new TrinoThriftSchemaTableName(indexHandle.getSchemaTableName());
        this.outputConstraint = tupleDomainToThriftTupleDomain(indexHandle.getTupleDomain());

        requireNonNull(lookupColumns, "lookupColumns is null");
        this.lookupColumnNames = lookupColumns.stream()
                .map(ThriftColumnHandle.class::cast)
                .map(ThriftColumnHandle::getColumnName)
                .collect(toImmutableList());

        requireNonNull(outputColumns, "outputColumns is null");
        ImmutableList.Builder<String> outputColumnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> outputColumnTypes = ImmutableList.builder();
        for (ColumnHandle columnHandle : outputColumns) {
            ThriftColumnHandle thriftColumnHandle = (ThriftColumnHandle) columnHandle;
            outputColumnNames.add(thriftColumnHandle.getColumnName());
            outputColumnTypes.add(thriftColumnHandle.getColumnType());
        }
        this.outputColumnNames = outputColumnNames.build();
        this.outputColumnTypes = outputColumnTypes.build();

        this.keys = fromRecordSet(requireNonNull(keys, "keys is null"));

        checkArgument(maxBytesPerResponse > 0, "maxBytesPerResponse is zero or negative");
        this.maxBytesPerResponse = maxBytesPerResponse;
        checkArgument(lookupRequestsConcurrency >= 1, "lookupRequestsConcurrency is less than one");
        this.lookupRequestsConcurrency = lookupRequestsConcurrency;

        this.contexts = new HashMap<>(lookupRequestsConcurrency);
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos.get();
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return statusFuture == null ? NOT_BLOCKED : statusFuture;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }
        if (!loadAllSplits()) {
            return null;
        }

        // check if any data requests were started
        if (dataSignalFuture == null) {
            // no data requests were started, start a number of initial requests
            checkState(contexts.isEmpty() && dataRequests.isEmpty(), "some splits are already started");
            if (splits.isEmpty()) {
                // all done: no splits
                finished = true;
                return null;
            }
            for (int i = 0; i < min(lookupRequestsConcurrency, splits.size()); i++) {
                startDataFetchForNextSplit();
            }
            updateSignalAndStatusFutures();
        }
        // check if any data request is finished
        if (!dataSignalFuture.isDone()) {
            // not finished yet
            return null;
        }

        // at least one of data requests completed
        ListenableFuture<TrinoThriftPageResult> resultFuture = getAndRemoveNextCompletedRequest();
        RunningSplitContext resultContext = contexts.remove(resultFuture);
        checkState(resultContext != null, "no associated context for the request");
        TrinoThriftPageResult pageResult = getFutureValue(resultFuture);
        Page page = pageResult.toPage(outputColumnTypes);
        if (page != null) {
            long pageSize = page.getSizeInBytes();
            completedBytes += pageSize;
            stats.addIndexPageSize(pageSize);
        }
        else {
            stats.addIndexPageSize(0);
        }
        if (pageResult.getNextToken() != null) {
            // can get more data
            sendDataRequest(resultContext, pageResult.getNextToken());
            updateSignalAndStatusFutures();
            return page;
        }

        // are there more splits available
        if (splitIndex < splits.size()) {
            // can send data request for a new split
            startDataFetchForNextSplit();
            updateSignalAndStatusFutures();
        }
        else if (!dataRequests.isEmpty()) {
            // no more new splits, but some requests are still in progress, wait for them
            updateSignalAndStatusFutures();
        }
        else {
            // all done: no more new splits, no requests in progress
            dataSignalFuture = null;
            statusFuture = null;
            finished = true;
        }
        return page;
    }

    private boolean loadAllSplits()
    {
        if (haveSplits) {
            return true;
        }
        // check if request for splits was sent
        if (splitFuture == null) {
            // didn't start fetching splits, send the first request now
            splitFuture = sendSplitRequest(null);
            statusFuture = toCompletableFuture(nonCancellationPropagating(splitFuture));
        }
        if (!splitFuture.isDone()) {
            // split request is in progress
            return false;
        }
        // split request is ready
        TrinoThriftSplitBatch batch = getFutureValue(splitFuture);
        splits.addAll(batch.getSplits());
        // check if it's possible to request more splits
        if (batch.getNextToken() != null) {
            // can get more splits, send request
            splitFuture = sendSplitRequest(batch.getNextToken());
            statusFuture = toCompletableFuture(nonCancellationPropagating(splitFuture));
            return false;
        }
        else {
            // no more splits
            splitFuture = null;
            statusFuture = null;
            haveSplits = true;
            return true;
        }
    }

    private void updateSignalAndStatusFutures()
    {
        dataSignalFuture = whenAnyComplete(dataRequests);
        statusFuture = toCompletableFuture(nonCancellationPropagating(dataSignalFuture));
    }

    private void startDataFetchForNextSplit()
    {
        TrinoThriftSplit split = splits.get(splitIndex);
        splitIndex++;
        RunningSplitContext context = new RunningSplitContext(openClient(split), split);
        sendDataRequest(context, null);
    }

    private ListenableFuture<TrinoThriftSplitBatch> sendSplitRequest(@Nullable TrinoThriftId nextToken)
    {
        long start = System.nanoTime();
        ListenableFuture<TrinoThriftSplitBatch> future = client.get(thriftHeaders).getIndexSplits(
                schemaTableName,
                lookupColumnNames,
                outputColumnNames,
                keys,
                outputConstraint,
                MAX_SPLIT_COUNT,
                new TrinoThriftNullableToken(nextToken));
        future = catchingThriftException(future);
        future.addListener(() -> readTimeNanos.addAndGet(System.nanoTime() - start), directExecutor());
        return future;
    }

    private void sendDataRequest(RunningSplitContext context, @Nullable TrinoThriftId nextToken)
    {
        long start = System.nanoTime();
        ListenableFuture<TrinoThriftPageResult> future = context.getClient().getRows(
                context.getSplit().getSplitId(),
                outputColumnNames,
                maxBytesPerResponse,
                new TrinoThriftNullableToken(nextToken));
        future = catchingThriftException(future);
        future.addListener(() -> readTimeNanos.addAndGet(System.nanoTime() - start), directExecutor());
        dataRequests.add(future);
        contexts.put(future, context);
    }

    private TrinoThriftService openClient(TrinoThriftSplit split)
    {
        if (split.getHosts().isEmpty()) {
            return client.get(thriftHeaders);
        }
        String hosts = split.getHosts().stream()
                .map(host -> host.toHostAddress().toString())
                .collect(joining(","));
        return client.get(Optional.of(hosts), thriftHeaders);
    }

    @Override
    public void close()
    {
        // cancel futures if available
        cancelQuietly(splitFuture);
        dataRequests.forEach(ThriftIndexPageSource::cancelQuietly);
    }

    private ListenableFuture<TrinoThriftPageResult> getAndRemoveNextCompletedRequest()
    {
        Iterator<ListenableFuture<TrinoThriftPageResult>> iterator = dataRequests.iterator();
        while (iterator.hasNext()) {
            ListenableFuture<TrinoThriftPageResult> future = iterator.next();
            if (future.isDone()) {
                iterator.remove();
                return future;
            }
        }
        throw new IllegalStateException("No completed splits in the queue");
    }

    private static void cancelQuietly(Future<?> future)
    {
        if (future != null) {
            future.cancel(true);
        }
    }

    private static final class RunningSplitContext
    {
        private final TrinoThriftService client;
        private final TrinoThriftSplit split;

        public RunningSplitContext(TrinoThriftService client, TrinoThriftSplit split)
        {
            this.client = client;
            this.split = split;
        }

        public TrinoThriftService getClient()
        {
            return client;
        }

        public TrinoThriftSplit getSplit()
        {
            return split;
        }
    }
}
