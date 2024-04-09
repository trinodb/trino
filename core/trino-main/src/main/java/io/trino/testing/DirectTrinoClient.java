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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.dispatcher.DispatchManager;
import io.trino.dispatcher.DispatchQuery;
import io.trino.exchange.DirectExchangeInput;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryState;
import io.trino.execution.buffer.PageDeserializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.DirectExchangeClient;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.server.ResultQueryInfo;
import io.trino.server.SessionContext;
import io.trino.server.protocol.ProtocolUtil;
import io.trino.server.protocol.Slug;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.type.Type;
import org.intellij.lang.annotations.Language;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.execution.buffer.CompressionCodec.NONE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.util.MoreLists.mappedCopy;
import static java.util.Objects.requireNonNull;

class DirectTrinoClient
{
    private final DispatchManager dispatchManager;
    private final QueryManager queryManager;
    private final DirectExchangeClientSupplier directExchangeClientSupplier;
    private final BlockEncodingSerde blockEncodingSerde;

    public DirectTrinoClient(DispatchManager dispatchManager, QueryManager queryManager, DirectExchangeClientSupplier directExchangeClientSupplier, BlockEncodingSerde blockEncodingSerde)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.directExchangeClientSupplier = requireNonNull(directExchangeClientSupplier, "directExchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
    }

    public Result execute(Session session, @Language("SQL") String sql)
    {
        return execute(SessionContext.fromSession(session), sql);
    }

    public Result execute(SessionContext sessionContext, @Language("SQL") String sql)
    {
        // create the query and wait for it to be dispatched
        QueryId queryId = dispatchManager.createQueryId();
        getQueryFuture(dispatchManager.createQuery(queryId, Span.getInvalid(), Slug.createNew(), sessionContext, sql));
        getQueryFuture(dispatchManager.waitForDispatched(queryId));
        DispatchQuery dispatchQuery = dispatchManager.getQuery(queryId);
        if (dispatchQuery.getState().isDone()) {
            return new Result(queryId, toMaterializedRows(dispatchQuery, ImmutableList.of(), ImmutableList.of(), ImmutableList.of()));
        }

        // read all output data
        AtomicReference<List<String>> columnNames = new AtomicReference<>();
        AtomicReference<List<Type>> columnTypes = new AtomicReference<>();
        List<Page> pages = new ArrayList<>();
        try (DirectExchangeClient exchangeClient = createExchangeClient(dispatchQuery)) {
            queryManager.setOutputInfoListener(queryId, outputInfo -> {
                // the listener is executed concurrently, so the call back must be synchronized to avoid a race between adding locations and setting no more locations
                synchronized (this) {
                    columnNames.compareAndSet(null, outputInfo.getColumnNames());
                    columnTypes.compareAndSet(null, outputInfo.getColumnTypes());

                    outputInfo.drainInputs(input -> {
                        DirectExchangeInput exchangeInput = (DirectExchangeInput) input;
                        exchangeClient.addLocation(exchangeInput.getTaskId(), URI.create(exchangeInput.getLocation()));
                    });
                    if (outputInfo.isNoMoreInputs()) {
                        exchangeClient.noMoreLocations();
                    }
                }
            });

            PageDeserializer pageDeserializer = new PagesSerdeFactory(blockEncodingSerde, NONE).createDeserializer(Optional.empty());
            for (QueryState state = queryManager.getQueryState(queryId); (state != FAILED) && !exchangeClient.isFinished(); state = queryManager.getQueryState(queryId)) {
                for (Slice serializedPage = exchangeClient.pollPage(); serializedPage != null; serializedPage = exchangeClient.pollPage()) {
                    Page page = pageDeserializer.deserialize(serializedPage);
                    pages.add(page);
                }
                getQueryFuture(whenAnyComplete(ImmutableList.of(queryManager.getStateChange(queryId, state), exchangeClient.isBlocked())));
            }
        }

        // wait for the query to be totally finished
        queryManager.resultsConsumed(queryId);
        for (QueryState queryState = queryManager.getQueryState(queryId); !queryState.isDone(); queryState = queryManager.getQueryState(queryId)) {
            getQueryFuture(queryManager.getStateChange(queryId, queryState));
        }

        return new Result(queryId, toMaterializedRows(dispatchQuery, columnTypes.get(), columnNames.get(), pages));
    }

    private DirectExchangeClient createExchangeClient(DispatchQuery dispatchQuery)
    {
        return directExchangeClientSupplier.get(
                dispatchQuery.getQueryId(),
                new ExchangeId("direct-exchange-query-results"),
                Span.current(),
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "Query"),
                queryManager::outputTaskFailed,
                getRetryPolicy(dispatchQuery.getSession()));
    }

    private static MaterializedResult toMaterializedRows(DispatchQuery dispatchQuery, List<Type> columnTypes, List<String> columnNames, List<Page> pages)
    {
        QueryInfo queryInfo = dispatchQuery.getFullQueryInfo();
        ConnectorSession session = dispatchQuery.getSession().toConnectorSession();

        if (queryInfo.getState() != FINISHED) {
            if (queryInfo.getFailureInfo() == null) {
                throw new QueryFailedException(queryInfo.getQueryId(), "Query failed without failure info");
            }
            RuntimeException remoteException = queryInfo.getFailureInfo().toException();
            throw new QueryFailedException(queryInfo.getQueryId(), Optional.ofNullable(remoteException.getMessage()).orElseGet(remoteException::toString), remoteException);
        }

        List<MaterializedRow> materializedRows = toMaterializedRows(session, columnTypes, pages);

        OptionalLong updateCount = OptionalLong.empty();
        if (queryInfo.getUpdateType() != null && materializedRows.size() == 1 && columnTypes.size() == 1 && columnTypes.get(0).equals(BIGINT)) {
            Number value = (Number) materializedRows.get(0).getField(0);
            if (value != null) {
                updateCount = OptionalLong.of(value.longValue());
            }
        }

        return new MaterializedResult(
                materializedRows,
                columnTypes,
                columnNames,
                queryInfo.getSetSessionProperties(),
                queryInfo.getResetSessionProperties(),
                Optional.ofNullable(queryInfo.getUpdateType()),
                updateCount,
                mappedCopy(queryInfo.getWarnings(), ProtocolUtil::toClientWarning),
                Optional.of(ProtocolUtil.toStatementStats(new ResultQueryInfo(queryInfo))));
    }

    private static List<MaterializedRow> toMaterializedRows(ConnectorSession session, List<Type> types, List<Page> pages)
    {
        ImmutableList.Builder<MaterializedRow> rows = ImmutableList.builder();
        for (Page page : pages) {
            checkArgument(page.getChannelCount() == types.size(), "Expected a page with %s columns, but got %s columns", types.size(), page.getChannelCount());
            for (int position = 0; position < page.getPositionCount(); position++) {
                List<Object> values = new ArrayList<>(page.getChannelCount());
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Type type = types.get(channel);
                    Block block = page.getBlock(channel);
                    values.add(type.getObjectValue(session, block, position));
                }
                values = Collections.unmodifiableList(values);

                rows.add(new MaterializedRow(DEFAULT_PRECISION, values));
            }
        }
        return rows.build();
    }

    private static <T> void getQueryFuture(ListenableFuture<T> future)
    {
        try {
            future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Thread interrupted", e);
        }
        catch (ExecutionException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Error processing query", e.getCause());
        }
    }

    record Result(QueryId queryId, MaterializedResult result)
    {
        Result
        {
            requireNonNull(queryId, "queryId is null");
            requireNonNull(result, "result is null");
        }
    }
}
