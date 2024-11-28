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
package io.trino.server.protocol;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.client.Column;
import io.trino.client.FailureInfo;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.exchange.ExchangeDataSource;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.exchange.LazyExchangeDataSource;
import io.trino.execution.BasicStageInfo;
import io.trino.execution.QueryExecution;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.execution.QueryState;
import io.trino.execution.StageId;
import io.trino.execution.buffer.PageDeserializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.memory.context.SimpleLocalMemoryContext;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.server.ExternalUriInfo;
import io.trino.server.GoneException;
import io.trino.server.ResultQueryInfo;
import io.trino.server.protocol.spooling.QueryDataProducer;
import io.trino.spi.ErrorCode;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.MapBlock;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.exchange.ExchangeId;
import io.trino.spi.security.SelectedRole;
import io.trino.spi.type.Type;
import io.trino.transaction.TransactionId;
import io.trino.util.Ciphers;
import jakarta.ws.rs.NotFoundException;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.trino.SystemSessionProperties.getExchangeCompressionCodec;
import static io.trino.SystemSessionProperties.getRetryPolicy;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.FINISHING;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.server.protocol.ProtocolUtil.createColumn;
import static io.trino.server.protocol.ProtocolUtil.toStatementStats;
import static io.trino.server.protocol.QueryInfoUrlFactory.getQueryInfoUri;
import static io.trino.server.protocol.QueryResultRows.queryResultRowsBuilder;
import static io.trino.server.protocol.Slug.Context.EXECUTING_QUERY;
import static io.trino.spi.StandardErrorCode.SERIALIZATION_ERROR;
import static io.trino.util.Failures.toFailure;
import static io.trino.util.MoreLists.mappedCopy;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class Query
{
    private static final Logger log = Logger.get(Query.class);

    private final QueryManager queryManager;
    private final QueryId queryId;
    private final Session session;
    private final Slug slug;
    private final Optional<URI> queryInfoUrl;

    @GuardedBy("this")
    private final ExchangeDataSource exchangeDataSource;

    @GuardedBy("this")
    private final QueryDataProducer queryDataProducer;

    @GuardedBy("this")
    private ListenableFuture<Void> exchangeDataSourceBlocked;

    private final Executor resultsProcessorExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    @GuardedBy("this")
    private PageDeserializer deserializer;
    private final boolean supportsParametricDateTime;

    @GuardedBy("this")
    private OptionalLong nextToken = OptionalLong.of(0);

    @GuardedBy("this")
    private QueryResults lastResult;

    @GuardedBy("this")
    private long lastToken = -1;

    private volatile boolean resultsConsumed;

    @GuardedBy("this")
    private List<Column> columns;

    @GuardedBy("this")
    private List<Type> types;

    @GuardedBy("this")
    private Optional<String> setCatalog = Optional.empty();

    @GuardedBy("this")
    private Optional<String> setSchema = Optional.empty();

    @GuardedBy("this")
    private Optional<String> setPath = Optional.empty();

    @GuardedBy("this")
    private Optional<String> setAuthorizationUser = Optional.empty();

    @GuardedBy("this")
    private boolean resetAuthorizationUser;

    @GuardedBy("this")
    private Map<String, String> setSessionProperties = ImmutableMap.of();

    @GuardedBy("this")
    private Set<String> resetSessionProperties = ImmutableSet.of();

    @GuardedBy("this")
    private Map<String, SelectedRole> setRoles = ImmutableMap.of();

    @GuardedBy("this")
    private Map<String, String> addedPreparedStatements = ImmutableMap.of();

    @GuardedBy("this")
    private Set<String> deallocatedPreparedStatements = ImmutableSet.of();

    @GuardedBy("this")
    private Optional<TransactionId> startedTransactionId = Optional.empty();

    @GuardedBy("this")
    private boolean clearTransactionId;

    @GuardedBy("this")
    private Optional<Throwable> typeSerializationException = Optional.empty();

    @GuardedBy("this")
    private Long updateCount;

    public static Query create(
            Session session,
            Slug slug,
            QueryManager queryManager,
            QueryDataProducer queryDataProducer,
            Optional<URI> queryInfoUrl,
            DirectExchangeClientSupplier directExchangeClientSupplier,
            ExchangeManagerRegistry exchangeManagerRegistry,
            Executor dataProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        ExchangeDataSource exchangeDataSource = new LazyExchangeDataSource(
                session.getQueryId(),
                new ExchangeId("query-results-exchange-" + session.getQueryId()),
                session.getQuerySpan(),
                directExchangeClientSupplier,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), Query.class.getSimpleName()),
                queryManager::outputTaskFailed,
                getRetryPolicy(session),
                exchangeManagerRegistry);

        Query result = new Query(session, slug, queryManager, queryDataProducer, queryInfoUrl, exchangeDataSource, dataProcessorExecutor, timeoutExecutor, blockEncodingSerde);

        result.queryManager.setOutputInfoListener(result.getQueryId(), result::setQueryOutputInfo);

        result.queryManager.addStateChangeListener(result.getQueryId(), state -> {
            // Wait for the query info to become available and close the exchange client if there is no output stage for the query results to be pulled from.
            // This listener also makes sure the exchange client is always properly closed upon query failure.
            if (state.isDone() || state == FINISHING) {
                QueryInfo queryInfo = queryManager.getFullQueryInfo(result.getQueryId());
                result.closeExchangeIfNecessary(new ResultQueryInfo(queryInfo));
            }
        });

        return result;
    }

    private Query(
            Session session,
            Slug slug,
            QueryManager queryManager,
            QueryDataProducer queryDataProducer,
            Optional<URI> queryInfoUrl,
            ExchangeDataSource exchangeDataSource,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        requireNonNull(session, "session is null");
        requireNonNull(slug, "slug is null");
        requireNonNull(queryManager, "queryManager is null");
        requireNonNull(queryDataProducer, "queryDataProducer is null");
        requireNonNull(queryInfoUrl, "queryInfoUrl is null");
        requireNonNull(exchangeDataSource, "exchangeDataSource is null");
        requireNonNull(resultsProcessorExecutor, "resultsProcessorExecutor is null");
        requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");

        this.queryManager = queryManager;
        this.queryDataProducer = queryDataProducer;
        this.queryId = session.getQueryId();
        this.session = session;
        this.slug = slug;
        this.queryInfoUrl = queryInfoUrl;
        this.exchangeDataSource = exchangeDataSource;
        this.resultsProcessorExecutor = resultsProcessorExecutor;
        this.timeoutExecutor = timeoutExecutor;
        this.supportsParametricDateTime = session.getClientCapabilities().contains(ClientCapabilities.PARAMETRIC_DATETIME.toString());
        deserializer = new PagesSerdeFactory(blockEncodingSerde, getExchangeCompressionCodec(session))
                .createDeserializer(session.getExchangeEncryptionKey().map(Ciphers::deserializeAesEncryptionKey));
    }

    public void cancel()
    {
        queryManager.cancelQuery(queryId);
        dispose();
    }

    public void partialCancel(int id)
    {
        StageId stageId = new StageId(queryId, id);
        queryManager.cancelStage(stageId);
    }

    public void fail(Throwable throwable)
    {
        queryManager.failQuery(queryId, throwable);
    }

    public synchronized void dispose()
    {
        exchangeDataSource.close();
        lastResult = null;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public boolean isSlugValid(String slug, long token)
    {
        return this.slug.isValid(EXECUTING_QUERY, slug, token);
    }

    public QueryInfo getQueryInfo()
    {
        return queryManager.getFullQueryInfo(queryId);
    }

    public ListenableFuture<QueryResultsResponse> waitForResults(long token, ExternalUriInfo externalUriInfo, Duration wait, DataSize targetResultSize)
    {
        ListenableFuture<Void> futureStateChange;
        synchronized (this) {
            // before waiting, check if this request has already been processed and cached
            Optional<QueryResults> cachedResult = getCachedResult(token);
            if (cachedResult.isPresent()) {
                return immediateFuture(toResultsResponse(cachedResult.get()));
            }
            // release the lock eagerly after acquiring the future to avoid contending with callback threads
            futureStateChange = getFutureStateChange();
        }

        // wait for a results data or query to finish, up to the wait timeout
        if (!futureStateChange.isDone()) {
            futureStateChange = addTimeout(futureStateChange, () -> null, wait, timeoutExecutor);
        }
        // when state changes, fetch the next result
        return Futures.transform(futureStateChange, _ -> getNextResult(token, externalUriInfo, targetResultSize), resultsProcessorExecutor);
    }

    public void markResultsConsumedIfReady()
    {
        if (resultsConsumed) {
            return;
        }
        synchronized (this) {
            if (!resultsConsumed && exchangeDataSource.isFinished()) {
                queryManager.resultsConsumed(queryId);
            }
        }
    }

    private synchronized ListenableFuture<Void> getFutureStateChange()
    {
        // if the exchange client is open, wait for data
        if (!exchangeDataSource.isFinished()) {
            if (exchangeDataSourceBlocked != null && !exchangeDataSourceBlocked.isDone()) {
                return exchangeDataSourceBlocked;
            }
            ListenableFuture<Void> blocked = exchangeDataSource.isBlocked();
            if (blocked.isDone()) {
                // not blocked
                return immediateVoidFuture();
            }
            // cache future to avoid accumulation of callbacks on the underlying future
            exchangeDataSourceBlocked = ignoreCancellation(blocked);
            return exchangeDataSourceBlocked;
        }
        exchangeDataSourceBlocked = null;

        if (!resultsConsumed) {
            return immediateVoidFuture();
        }

        // otherwise, wait for the query to finish
        queryManager.recordHeartbeat(queryId);
        try {
            return queryDoneFuture(queryManager.getQueryState(queryId));
        }
        catch (NoSuchElementException e) {
            return immediateVoidFuture();
        }
    }

    /**
     * Contrary to {@link Futures#nonCancellationPropagating(ListenableFuture)} this method returns a future that cannot be cancelled
     * what allows it to be shared between multiple independent callers
     */
    private static ListenableFuture<Void> ignoreCancellation(ListenableFuture<Void> future)
    {
        return new AbstractFuture<Void>()
        {
            public AbstractFuture<Void> propagateFuture(ListenableFuture<? extends Void> future)
            {
                setFuture(future);
                return this;
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning)
            {
                // ignore
                return false;
            }
        }.propagateFuture(future);
    }

    private synchronized Optional<QueryResults> getCachedResult(long token)
    {
        // is this the first request?
        if (lastResult == null) {
            return Optional.empty();
        }

        // is this a repeated request for the last results?
        if (token == lastToken) {
            // tell query manager we are still interested in the query
            queryManager.recordHeartbeat(queryId);
            return Optional.of(lastResult);
        }

        // if this is a result before the lastResult, the data is gone
        if (token < lastToken) {
            throw new GoneException();
        }

        // if this is a request for a result after the end of the stream, return not found
        if (nextToken.isEmpty()) {
            throw new NotFoundException();
        }

        // if this is not a request for the next results, return not found
        if (token != nextToken.getAsLong()) {
            // unknown token
            throw new NotFoundException();
        }

        return Optional.empty();
    }

    private synchronized QueryResultsResponse getNextResult(long token, ExternalUriInfo externalUriInfo, DataSize targetResultSize)
    {
        // check if the result for the token have already been created
        Optional<QueryResults> cachedResult = getCachedResult(token);
        if (cachedResult.isPresent()) {
            return toResultsResponse(cachedResult.get());
        }

        verify(nextToken.isPresent(), "Cannot generate next result when next token is not present");
        verify(token == nextToken.getAsLong(), "Expected token to equal next token");

        // get the query info before returning
        // force update if query manager is closed
        ResultQueryInfo queryInfo = queryManager.getResultQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        boolean isStarted = queryInfo.state().ordinal() > QueryState.STARTING.ordinal();
        QueryResultRows resultRows;
        if (isStarted) {
            closeExchangeIfNecessary(queryInfo);
            // fetch result data from exchange
            resultRows = removePagesFromExchange(queryInfo, targetResultSize.toBytes());
        }
        else {
            resultRows = queryResultRowsBuilder(session).build();
        }

        if ((queryInfo.updateType() != null) && (updateCount == null)) {
            // grab the update count for non-queries
            Optional<Long> updatedRowsCount = resultRows.getUpdateCount();
            updateCount = updatedRowsCount.orElse(null);
        }

        if (isStarted && (queryInfo.outputStage().isEmpty() || exchangeDataSource.isFinished())) {
            queryManager.resultsConsumed(queryId);
            resultsConsumed = true;
            // update query since the query might have been transitioned to the FINISHED state
            queryInfo = queryManager.getResultQueryInfo(queryId);
        }

        // advance next token
        // only return a next if
        // (1) the query is not done AND the query state is not FAILED
        //   OR
        // (2) there is more data to send (due to buffering)
        //   OR
        // (3) cached query result needs client acknowledgement to discard
        if (queryInfo.state() != FAILED && (!queryInfo.finalQueryInfo() || !exchangeDataSource.isFinished() || (queryInfo.outputStage().isPresent() && !resultRows.isEmpty()))) {
            nextToken = OptionalLong.of(token + 1);
        }
        else {
            nextToken = OptionalLong.empty();
            // the client is not coming back, make sure the exchange is closed
            exchangeDataSource.close();
        }

        URI nextResultsUri = null;
        URI partialCancelUri = null;
        if (nextToken.isPresent()) {
            long nextToken = this.nextToken.getAsLong();
            nextResultsUri = createNextResultsUri(externalUriInfo, nextToken);
            partialCancelUri = findCancelableLeafStage(queryInfo)
                    .map(stage -> createPartialCancelUri(stage, externalUriInfo, nextToken))
                    .orElse(null);
        }

        // update catalog, schema, and path
        setCatalog = queryInfo.setCatalog();
        setSchema = queryInfo.setSchema();
        setPath = queryInfo.setPath();

        // update setAuthorizationUser
        setAuthorizationUser = queryInfo.setAuthorizationUser();
        resetAuthorizationUser = queryInfo.resetAuthorizationUser();

        // update setSessionProperties
        setSessionProperties = queryInfo.setSessionProperties();
        resetSessionProperties = queryInfo.resetSessionProperties();

        // update setRoles
        setRoles = queryInfo.setRoles();

        // update preparedStatements
        addedPreparedStatements = queryInfo.addedPreparedStatements();
        deallocatedPreparedStatements = queryInfo.deallocatedPreparedStatements();

        // update startedTransactionId
        startedTransactionId = queryInfo.startedTransactionId();
        clearTransactionId = queryInfo.clearTransactionId();

        // first time through, self is null
        QueryResults queryResults = new QueryResults(
                queryId.toString(),
                getQueryInfoUri(queryInfoUrl, queryId, externalUriInfo),
                partialCancelUri,
                nextResultsUri,
                resultRows.getColumns().orElse(null),
                queryDataProducer.produce(externalUriInfo, session, resultRows, this::handleSerializationException),
                toStatementStats(queryInfo),
                toQueryError(queryInfo, typeSerializationException),
                mappedCopy(queryInfo.warnings(), ProtocolUtil::toClientWarning),
                queryInfo.updateType(),
                updateCount);

        // cache the new result
        lastToken = token;
        lastResult = queryResults;

        return toResultsResponse(queryResults);
    }

    private synchronized QueryResultsResponse toResultsResponse(QueryResults queryResults)
    {
        return new QueryResultsResponse(
                setCatalog,
                setSchema,
                setPath,
                setAuthorizationUser,
                resetAuthorizationUser,
                setSessionProperties,
                resetSessionProperties,
                setRoles,
                addedPreparedStatements,
                deallocatedPreparedStatements,
                startedTransactionId,
                clearTransactionId,
                session.getProtocolHeaders(),
                queryResults);
    }

    private synchronized QueryResultRows removePagesFromExchange(ResultQueryInfo queryInfo, long targetResultBytes)
    {
        if (!resultsConsumed && queryInfo.outputStage().isEmpty()) {
            return queryResultRowsBuilder(session)
                    .withColumnsAndTypes(ImmutableList.of(), ImmutableList.of())
                    .build();
        }
        // Remove as many pages as possible from the exchange until just greater than DESIRED_RESULT_BYTES
        // NOTE: it is critical that query results are created for the pages removed from the exchange
        // client while holding the lock because the query may transition to the finished state when the
        // last page is removed.  If another thread observes this state before the response is cached
        // the pages will be lost.
        QueryResultRows.Builder resultBuilder = queryResultRowsBuilder(session)
                .withColumnsAndTypes(columns, types);

        try {
            long bytes = 0;
            while (bytes < targetResultBytes) {
                Slice serializedPage = exchangeDataSource.pollPage();
                if (serializedPage == null) {
                    break;
                }

                Page page = deserializer.deserialize(serializedPage);
                // page should already be loaded since it was just deserialized
                page = page.getLoadedPage();
                bytes += estimateJsonSize(page);
                resultBuilder.addPage(page);
            }
            if (exchangeDataSource.isFinished()) {
                exchangeDataSource.close();
                deserializer = null; // null to reclaim memory of PagesSerde which does not expose explicit lifecycle
            }
        }
        catch (Throwable cause) {
            queryManager.failQuery(queryId, cause);
        }

        return resultBuilder.build();
    }

    private static long estimateJsonSize(Page page)
    {
        long estimatedSize = 0;
        for (int i = 0; i < page.getChannelCount(); i++) {
            estimatedSize += estimateJsonSize(page.getBlock(i));
        }
        return estimatedSize;
    }

    private static long estimateJsonSize(Block block)
    {
        switch (block) {
            case RunLengthEncodedBlock rleBlock:
                return estimateJsonSize(rleBlock.getValue()) * rleBlock.getPositionCount();
            case DictionaryBlock dictionaryBlock:
                ValueBlock dictionary = dictionaryBlock.getDictionary();
                double averageSizePerEntry = (double) estimateJsonSize(dictionary) / dictionary.getPositionCount();
                return (long) (averageSizePerEntry * block.getPositionCount());
            case RowBlock rowBlock:
                return rowBlock.getFieldBlocks().stream()
                        .mapToLong(Query::estimateJsonSize)
                        .sum();
            case ArrayBlock arrayBlock:
                return estimateJsonSize(arrayBlock.getElementsBlock());
            case MapBlock mapBlock:
                return estimateJsonSize(mapBlock.getKeyBlock()) +
                        estimateJsonSize(mapBlock.getValueBlock());
            default:
                return block.getSizeInBytes();
        }
    }

    private void closeExchangeIfNecessary(ResultQueryInfo queryInfo)
    {
        if (queryInfo.state() != FAILED && queryInfo.outputStage().isPresent()) {
            return;
        }
        // Close the exchange client if the query has failed, or if the query
        // does not have an output stage. The latter happens
        // for data definition executions, as those do not have output.
        synchronized (this) {
            if (queryInfo.state() == FAILED || (!exchangeDataSource.isFinished() && queryInfo.outputStage().isEmpty())) {
                exchangeDataSource.close();
            }
        }
    }

    private synchronized void handleSerializationException(Throwable exception)
    {
        // failQuery can throw exception if query has already finished.
        try {
            queryManager.failQuery(queryId, exception);
        }
        catch (RuntimeException e) {
            log.debug(e, "Could not fail query");
        }

        if (typeSerializationException.isEmpty()) {
            typeSerializationException = Optional.of(exception);
        }
    }

    private synchronized void setQueryOutputInfo(QueryExecution.QueryOutputInfo outputInfo)
    {
        // if first callback, set column names
        if (columns == null) {
            List<String> columnNames = outputInfo.getColumnNames();
            List<Type> columnTypes = outputInfo.getColumnTypes();
            checkArgument(columnNames.size() == columnTypes.size(), "Column names and types size mismatch");

            ImmutableList.Builder<Column> list = ImmutableList.builder();
            for (int i = 0; i < columnNames.size(); i++) {
                list.add(createColumn(columnNames.get(i), columnTypes.get(i), supportsParametricDateTime));
            }
            columns = list.build();
            types = outputInfo.getColumnTypes();
        }

        outputInfo.drainInputs(exchangeDataSource::addInput);
        if (outputInfo.isNoMoreInputs()) {
            exchangeDataSource.noMoreInputs();
        }
    }

    private ListenableFuture<Void> queryDoneFuture(QueryState currentState)
    {
        if (currentState.isDone()) {
            return immediateVoidFuture();
        }
        return Futures.transformAsync(queryManager.getStateChange(queryId, currentState), this::queryDoneFuture, directExecutor());
    }

    private URI createNextResultsUri(ExternalUriInfo externalUriInfo, long nextToken)
    {
        return externalUriInfo.baseUriBuilder()
                .path("/v1/statement/executing")
                .path(queryId.toString())
                .path(slug.makeSlug(EXECUTING_QUERY, nextToken))
                .path(String.valueOf(nextToken))
                .build();
    }

    private URI createPartialCancelUri(int stage, ExternalUriInfo externalUriInfo, long nextToken)
    {
        return externalUriInfo.baseUriBuilder()
                .path("/v1/statement/executing/partialCancel")
                .path(queryId.toString())
                .path(String.valueOf(stage))
                .path(slug.makeSlug(EXECUTING_QUERY, nextToken))
                .path(String.valueOf(nextToken))
                .build();
    }

    private static Optional<Integer> findCancelableLeafStage(ResultQueryInfo queryInfo)
    {
        // if query is running, find the leaf-most running stage
        return queryInfo.outputStage().flatMap(Query::findCancelableLeafStage);
    }

    private static Optional<Integer> findCancelableLeafStage(BasicStageInfo stage)
    {
        // if this stage is already done, we can't cancel it
        if (stage.getState().isDone()) {
            return Optional.empty();
        }

        // attempt to find a cancelable sub stage
        // check in reverse order since build side of a join will be later in the list
        for (BasicStageInfo subStage : stage.getSubStages().reversed()) {
            Optional<Integer> leafStage = findCancelableLeafStage(subStage);
            if (leafStage.isPresent()) {
                return leafStage;
            }
        }

        // no matching sub stage, so return this stage
        return Optional.of(stage.getStageId().getId());
    }

    private static QueryError toQueryError(ResultQueryInfo queryInfo, Optional<Throwable> exception)
    {
        if (queryInfo.failureInfo() == null && exception.isPresent()) {
            ErrorCode errorCode = SERIALIZATION_ERROR.toErrorCode();
            FailureInfo failure = toFailure(exception.get()).toFailureInfo();
            return new QueryError(
                    firstNonNull(failure.getMessage(), "Internal error"),
                    null,
                    errorCode.getCode(),
                    errorCode.getName(),
                    errorCode.getType().toString(),
                    failure.getErrorLocation(),
                    failure);
        }

        return ProtocolUtil.toQueryError(queryInfo);
    }
}
