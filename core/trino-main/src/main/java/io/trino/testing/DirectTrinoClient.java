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
import io.trino.dispatcher.DispatchManager;
import io.trino.dispatcher.DispatchQuery;
import io.trino.exchange.DirectExchangeInput;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryState;
import io.trino.execution.buffer.PageDeserializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.DirectExchangeClient;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.server.SessionContext;
import io.trino.server.protocol.Slug;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.type.Type;
import org.intellij.lang.annotations.Language;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.FINISHING;
import static io.trino.execution.buffer.CompressionCodec.NONE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class DirectTrinoClient
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

    public DispatchQuery execute(SessionContext sessionContext, @Language("SQL") String sql, QueryResultsListener queryResultsListener)
    {
        // create the query and wait for it to be dispatched
        QueryId queryId = dispatchManager.createQueryId();
        getQueryFuture(dispatchManager.createQuery(queryId, Span.getInvalid(), Slug.createNew(), sessionContext, sql));
        getQueryFuture(dispatchManager.waitForDispatched(queryId));
        DispatchQuery dispatchQuery = dispatchManager.getQuery(queryId);
        if (dispatchQuery.getState().isDone()) {
            return dispatchQuery;
        }

        // read all output data
        try (DirectExchangeClient exchangeClient = createExchangeClient(dispatchQuery)) {
            queryManager.setOutputInfoListener(queryId, outputInfo -> {
                // the listener is executed concurrently, so the call back must be synchronized to avoid a race between adding locations and setting no more locations
                synchronized (this) {
                    queryResultsListener.setOutputColumns(outputInfo.getColumnNames(), outputInfo.getColumnTypes());

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
            for (QueryState state = queryManager.getQueryState(queryId);
                    (state != FAILED) &&
                            !exchangeClient.isFinished() &&
                            !(dispatchQuery.getState() == FINISHING && dispatchQuery.getFullQueryInfo().getOutputStage().isEmpty());
                    state = queryManager.getQueryState(queryId)) {
                for (Slice serializedPage = exchangeClient.pollPage(); serializedPage != null; serializedPage = exchangeClient.pollPage()) {
                    Page page = pageDeserializer.deserialize(serializedPage);
                    queryResultsListener.consumeOutputPage(page);
                }
                getQueryFuture(whenAnyComplete(ImmutableList.of(queryManager.getStateChange(queryId, state), exchangeClient.isBlocked())));
            }
        }

        // wait for the query to be totally finished
        queryManager.resultsConsumed(queryId);
        for (QueryState queryState = queryManager.getQueryState(queryId); !queryState.isDone(); queryState = queryManager.getQueryState(queryId)) {
            getQueryFuture(queryManager.getStateChange(queryId, queryState));
        }

        return dispatchQuery;
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

    public interface QueryResultsListener
    {
        void setOutputColumns(List<String> columnNames, List<Type> columnTypes);

        void consumeOutputPage(Page page);
    }
}
