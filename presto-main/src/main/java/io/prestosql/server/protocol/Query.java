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
package io.prestosql.server.protocol;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.client.ClientTypeSignature;
import io.prestosql.client.ClientTypeSignatureParameter;
import io.prestosql.client.Column;
import io.prestosql.client.FailureInfo;
import io.prestosql.client.NamedClientTypeSignature;
import io.prestosql.client.QueryError;
import io.prestosql.client.QueryResults;
import io.prestosql.client.RowFieldName;
import io.prestosql.client.StageStats;
import io.prestosql.client.StatementStats;
import io.prestosql.client.Warning;
import io.prestosql.execution.QueryExecution;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStats;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.TaskInfo;
import io.prestosql.execution.buffer.PagesSerde;
import io.prestosql.execution.buffer.PagesSerdeFactory;
import io.prestosql.execution.buffer.SerializedPage;
import io.prestosql.operator.ExchangeClient;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.WarningCode;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.transaction.TransactionId;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.addTimeout;
import static io.prestosql.execution.QueryState.DISPATCHING;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.execution.QueryState.FINISHED;
import static io.prestosql.execution.QueryState.QUEUED;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.util.Failures.toFailure;
import static io.prestosql.util.MoreLists.mappedCopy;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class Query
{
    private static final Logger log = Logger.get(Query.class);

    private final QueryId queryId;
    private final String slug;
    private final long queryCreateNanoTime;
    private final Duration queryQueueDuration;
    private final QueryManager queryManager;

    @GuardedBy("this")
    private final ExchangeClient exchangeClient;

    private final Executor resultsProcessorExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final PagesSerde serde;

    private final AtomicLong resultId = new AtomicLong();

    @GuardedBy("this")
    private QueryResponse lastResult;

    @GuardedBy("this")
    private String lastResultPath;

    @GuardedBy("this")
    private List<Column> columns;

    @GuardedBy("this")
    private List<Type> types;

    @GuardedBy("this")
    private Long updateCount;

    private final FutureCreatedQuery futureCreatedQuery;

    public static Query createQuery(
            QueryId queryId,
            String slug,
            Duration queryElapsedTime,
            Duration queryQueueDuration,
            QueryManager queryManager,
            Supplier<QueryExecution> queryExecutionCallable,
            Function<Throwable, QueryExecution> failedQueryExecutionFactory,
            ExchangeClient exchangeClient,
            ExecutorService queryCreationExecutor,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        Query query = new Query(
                queryId,
                slug,
                queryElapsedTime,
                queryQueueDuration,
                queryManager,
                queryExecutionCallable,
                failedQueryExecutionFactory,
                exchangeClient,
                queryCreationExecutor,
                resultsProcessorExecutor,
                timeoutExecutor,
                blockEncodingSerde);
        query.start();
        return query;
    }

    private Query(
            QueryId queryId,
            String slug,
            Duration queryElapsedTime,
            Duration queryQueueDuration,
            QueryManager queryManager,
            Supplier<QueryExecution> queryExecutionCallable,
            Function<Throwable, QueryExecution> failedQueryExecutionFactory,
            ExchangeClient exchangeClient,
            ExecutorService queryCreationExecutor,
            Executor resultsProcessorExecutor,
            ScheduledExecutorService timeoutExecutor,
            BlockEncodingSerde blockEncodingSerde)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");

        this.queryId = requireNonNull(queryId, "queryId is null");
        this.slug = requireNonNull(slug, "slug is null");
        requireNonNull(queryElapsedTime, "queryElapsedTime is null");
        this.queryCreateNanoTime = System.nanoTime() - queryElapsedTime.roundTo(NANOSECONDS);
        this.queryQueueDuration = requireNonNull(queryQueueDuration, "queryQueueDuration is null");
        this.exchangeClient = requireNonNull(exchangeClient, "exchangeClient is null");
        this.resultsProcessorExecutor = requireNonNull(resultsProcessorExecutor, "resultsProcessorExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");

        serde = new PagesSerdeFactory(requireNonNull(blockEncodingSerde, "blockEncodingSerde is null"), false).createPagesSerde();
        futureCreatedQuery = new FutureCreatedQuery(
                queryId,
                requireNonNull(queryCreationExecutor, "queryCreationExecutor is null"),
                requireNonNull(queryExecutionCallable, "queryExecutionCallable is null"),
                requireNonNull(failedQueryExecutionFactory, "failedQueryExecutionFactory is null"));
    }

    private void start()
    {
        // when query creation finishes, register callbacks with the query manager
        addSuccessCallback(futureCreatedQuery.createNewListener(), () -> {
            // there may have been an error registering the query
            if (!queryManager.isQueryRegistered(queryId)) {
                return;
            }

            queryManager.addOutputInfoListener(queryId, this::setQueryOutputInfo);

            queryManager.addStateChangeListener(queryId, state -> {
                if (state.isDone()) {
                    QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
                    closeExchangeClientIfNecessary(queryInfo);
                }
            });
        });
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public boolean isQueryCreated()
    {
        return futureCreatedQuery.isQueryCreated();
    }

    public void failIfAbandoned()
    {
        futureCreatedQuery.failIfAbandoned();
    }

    public void cancel()
    {
        futureCreatedQuery.startQueryCreation();
        if (futureCreatedQuery.isQueryCreated()) {
            queryManager.cancelQuery(queryId);
        }
        else {
            addSuccessCallback(futureCreatedQuery.createNewListener(), () -> queryManager.cancelQuery(queryId));
        }
        synchronized (this) {
            exchangeClient.close();
        }
    }

    public boolean isSlugValid(String slug)
    {
        return this.slug.equals(slug);
    }

    public synchronized ListenableFuture<QueryResponse> waitForResults(long token, UriInfo uriInfo, String scheme, Duration wait, DataSize targetResultSize)
    {
        futureCreatedQuery.startQueryCreation();

        // before waiting, check if this request has already been processed and cached
        String requestedPath = uriInfo.getAbsolutePath().getPath();
        Optional<QueryResponse> cachedResult = getCachedResult(token, requestedPath);
        if (cachedResult.isPresent()) {
            return immediateFuture(cachedResult.get());
        }

        // wait for a results data or query to finish, up to the wait timeout
        ListenableFuture<?> futureStateChange = addTimeout(
                getFutureStateChange(),
                () -> null,
                wait,
                timeoutExecutor);

        // when state changes, fetch the next result
        return Futures.transform(futureStateChange, ignored -> getNextResponse(token, uriInfo, scheme, targetResultSize), resultsProcessorExecutor);
    }

    @VisibleForTesting
    public void startQueryCreation()
    {
        futureCreatedQuery.startQueryCreation();
    }

    private synchronized ListenableFuture<?> getFutureStateChange()
    {
        // if query has not been created, wait for the query to be created, and then call back into this method
        if (!futureCreatedQuery.isQueryCreated()) {
            return transformAsync(futureCreatedQuery.createNewListener(), ignored -> getFutureStateChange(), resultsProcessorExecutor);
        }

        // if the exchange client is open, wait for data
        if (!exchangeClient.isClosed()) {
            return exchangeClient.isBlocked();
        }

        // otherwise, wait for the query to finish
        queryManager.recordHeartbeat(queryId);
        try {
            return queryDoneFuture(queryManager.getQueryState(queryId));
        }
        catch (NoSuchElementException e) {
            return immediateFuture(null);
        }
    }

    private synchronized Optional<QueryResponse> getCachedResult(long token, String requestedPath)
    {
        // is this the first request?
        if (lastResult == null) {
            return Optional.empty();
        }

        // is the a repeated request for the last results?
        if (requestedPath.equals(lastResultPath)) {
            // tell query manager we are still interested in the query
            queryManager.recordHeartbeat(queryId);
            return Optional.of(lastResult);
        }

        if (token < resultId.get()) {
            throw new WebApplicationException(Response.Status.GONE);
        }

        // if this is not a request for the next results, return not found
        if (lastResult.getQueryResults().getNextUri() == null || !requestedPath.equals(lastResult.getQueryResults().getNextUri().getPath())) {
            // unknown token
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        return Optional.empty();
    }

    private synchronized QueryResponse getNextResponse(long token, UriInfo uriInfo, String scheme, DataSize targetResultSize)
    {
        // check if the result for the token have already been created
        String requestedPath = uriInfo.getAbsolutePath().getPath();
        Optional<QueryResponse> cachedResponse = getCachedResult(token, requestedPath);
        if (cachedResponse.isPresent()) {
            return cachedResponse.get();
        }

        URI queryHtmlUri = uriInfo.getRequestUriBuilder()
                .scheme(scheme)
                .replacePath("ui/query.html")
                .replaceQuery(queryId.toString())
                .build();

        if (!futureCreatedQuery.isQueryCreated()) {
            QueryResults queryResults = new QueryResults(
                    queryId.toString(),
                    queryHtmlUri,
                    null,
                    createNextResultsUri(scheme, uriInfo),
                    null,
                    null,
                    StatementStats.builder()
                            .setState(DISPATCHING.toString())
                            .setQueued(false)
                            .setElapsedTimeMillis(NANOSECONDS.toMillis(System.nanoTime() - queryCreateNanoTime))
                            .setQueuedTimeMillis(queryQueueDuration.toMillis())
                            .build(),
                    null,
                    ImmutableList.of(),
                    null,
                    null);

            QueryResponse queryResponse = new QueryResponse(
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of(),
                    ImmutableSet.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableSet.of(),
                    Optional.empty(),
                    false,
                    queryResults);

            cacheLastResponse(queryResponse, requestedPath);
            return queryResponse;
        }

        // Remove as many pages as possible from the exchange until just greater than DESIRED_RESULT_BYTES
        // NOTE: it is critical that query results are created for the pages removed from the exchange
        // client while holding the lock because the query may transition to the finished state when the
        // last page is removed.  If another thread observes this state before the response is cached
        // the pages will be lost.
        ConnectorSession session = queryManager.getQuerySession(queryId).toConnectorSession();
        Iterable<List<Object>> data = null;
        try {
            ImmutableList.Builder<RowIterable> pages = ImmutableList.builder();
            long bytes = 0;
            long rows = 0;
            long targetResultBytes = targetResultSize.toBytes();
            while (bytes < targetResultBytes) {
                SerializedPage serializedPage = exchangeClient.pollPage();
                if (serializedPage == null) {
                    break;
                }

                Page page = serde.deserialize(serializedPage);
                bytes += page.getLogicalSizeInBytes();
                rows += page.getPositionCount();
                pages.add(new RowIterable(session, types, page));
            }
            if (rows > 0) {
                // client implementations do not properly handle empty list of data
                data = Iterables.concat(pages.build());
            }
        }
        catch (Throwable cause) {
            queryManager.failQuery(queryId, cause);
        }

        // get the query info before returning
        // force update if query manager is closed
        QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
        queryManager.recordHeartbeat(queryId);

        // TODO: figure out a better way to do this
        // grab the update count for non-queries
        if ((data != null) && (queryInfo.getUpdateType() != null) && (updateCount == null) &&
                (columns.size() == 1) && (columns.get(0).getType().equals(BIGINT))) {
            Iterator<List<Object>> iterator = data.iterator();
            if (iterator.hasNext()) {
                Number number = (Number) iterator.next().get(0);
                if (number != null) {
                    updateCount = number.longValue();
                }
            }
        }

        closeExchangeClientIfNecessary(queryInfo);

        // for queries with no output, return a fake result for clients that require it
        if ((queryInfo.getState() == FINISHED) && !queryInfo.getOutputStage().isPresent()) {
            columns = ImmutableList.of(createColumn("result", BOOLEAN));
            data = ImmutableSet.of(ImmutableList.of(true));
        }

        // only return a next if
        // (1) the query is not done AND the query state is not FAILED
        //   OR
        // (2)there is more data to send (due to buffering)
        URI nextResultsUri = null;
        if (!queryInfo.isFinalQueryInfo() && queryInfo.getState() != FAILED
                || !exchangeClient.isClosed()) {
            nextResultsUri = createNextResultsUri(scheme, uriInfo);
        }

        // first time through, self is null
        QueryResults queryResults = new QueryResults(
                queryId.toString(),
                queryHtmlUri,
                findCancelableLeafStage(queryInfo),
                nextResultsUri,
                columns,
                data,
                toStatementStats(queryInfo),
                toQueryError(queryInfo),
                mappedCopy(queryInfo.getWarnings(), Query::toClientWarning),
                queryInfo.getUpdateType(),
                updateCount);

        QueryResponse queryResponse = new QueryResponse(
                queryInfo.getSetCatalog(),
                queryInfo.getSetSchema(),
                queryInfo.getSetPath(),
                queryInfo.getSetSessionProperties(),
                queryInfo.getResetSessionProperties(),
                queryInfo.getSetRoles(),
                queryInfo.getAddedPreparedStatements(),
                queryInfo.getDeallocatedPreparedStatements(),
                queryInfo.getStartedTransactionId(),
                queryInfo.isClearTransactionId(),
                queryResults);
        cacheLastResponse(queryResponse, requestedPath);
        return queryResponse;
    }

    private synchronized void cacheLastResponse(QueryResponse queryResponse, String requestedPath)
    {
        lastResultPath = requestedPath;
        lastResult = queryResponse;
    }

    private synchronized void closeExchangeClientIfNecessary(QueryInfo queryInfo)
    {
        // Close the exchange client if the query has failed, or if the query
        // is done and it does not have an output stage. The latter happens
        // for data definition executions, as those do not have output.
        if ((queryInfo.getState() == FAILED) ||
                (queryInfo.getState().isDone() && !queryInfo.getOutputStage().isPresent())) {
            exchangeClient.close();
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
                list.add(createColumn(columnNames.get(i), columnTypes.get(i)));
            }
            columns = list.build();
            types = outputInfo.getColumnTypes();
        }

        for (URI outputLocation : outputInfo.getBufferLocations()) {
            exchangeClient.addLocation(outputLocation);
        }
        if (outputInfo.isNoMoreBufferLocations()) {
            exchangeClient.noMoreLocations();
        }
    }

    private ListenableFuture<?> queryDoneFuture(QueryState currentState)
    {
        if (currentState.isDone()) {
            return immediateFuture(null);
        }
        return transformAsync(queryManager.getStateChange(queryId, currentState), this::queryDoneFuture, directExecutor());
    }

    private URI createNextResultsUri(String scheme, UriInfo uriInfo)
    {
        return uriInfo.getBaseUriBuilder()
                .scheme(scheme)
                .replacePath("/v1/statement/executing")
                .path(queryId.toString())
                .path(slug)
                .path(String.valueOf(resultId.incrementAndGet()))
                .replaceQuery("")
                .build();
    }

    private static Column createColumn(String name, Type type)
    {
        TypeSignature signature = type.getTypeSignature();
        return new Column(name, signature.toString(), toClientTypeSignature(signature));
    }

    private static ClientTypeSignature toClientTypeSignature(TypeSignature signature)
    {
        return new ClientTypeSignature(signature.getBase(), signature.getParameters().stream()
                .map(Query::toClientTypeSignatureParameter)
                .collect(toImmutableList()));
    }

    private static ClientTypeSignatureParameter toClientTypeSignatureParameter(TypeSignatureParameter parameter)
    {
        switch (parameter.getKind()) {
            case TYPE:
                return ClientTypeSignatureParameter.ofType(toClientTypeSignature(parameter.getTypeSignature()));
            case NAMED_TYPE:
                return ClientTypeSignatureParameter.ofNamedType(new NamedClientTypeSignature(
                        parameter.getNamedTypeSignature().getFieldName().map(value ->
                                new RowFieldName(value.getName(), value.isDelimited())),
                        toClientTypeSignature(parameter.getNamedTypeSignature().getTypeSignature())));
            case LONG:
                return ClientTypeSignatureParameter.ofLong(parameter.getLongLiteral());
        }
        throw new IllegalArgumentException("Unsupported kind: " + parameter.getKind());
    }

    private static StatementStats toStatementStats(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        StageInfo outputStage = queryInfo.getOutputStage().orElse(null);

        return StatementStats.builder()
                .setState(queryInfo.getState().toString())
                .setQueued(queryInfo.getState() == QUEUED)
                .setScheduled(queryInfo.isScheduled())
                .setNodes(globalUniqueNodes(outputStage).size())
                .setTotalSplits(queryStats.getTotalDrivers())
                .setQueuedSplits(queryStats.getQueuedDrivers())
                .setRunningSplits(queryStats.getRunningDrivers() + queryStats.getBlockedDrivers())
                .setCompletedSplits(queryStats.getCompletedDrivers())
                .setCpuTimeMillis(queryStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(queryStats.getTotalScheduledTime().toMillis())
                .setQueuedTimeMillis(queryStats.getQueuedTime().toMillis())
                .setElapsedTimeMillis(queryStats.getElapsedTime().toMillis())
                .setProcessedRows(queryStats.getRawInputPositions())
                .setProcessedBytes(queryStats.getRawInputDataSize().toBytes())
                .setPeakMemoryBytes(queryStats.getPeakUserMemoryReservation().toBytes())
                .setSpilledBytes(queryStats.getSpilledDataSize().toBytes())
                .setRootStage(toStageStats(outputStage))
                .build();
    }

    private static StageStats toStageStats(StageInfo stageInfo)
    {
        if (stageInfo == null) {
            return null;
        }

        io.prestosql.execution.StageStats stageStats = stageInfo.getStageStats();

        ImmutableList.Builder<StageStats> subStages = ImmutableList.builder();
        for (StageInfo subStage : stageInfo.getSubStages()) {
            subStages.add(toStageStats(subStage));
        }

        Set<String> uniqueNodes = new HashSet<>();
        for (TaskInfo task : stageInfo.getTasks()) {
            // todo add nodeId to TaskInfo
            URI uri = task.getTaskStatus().getSelf();
            uniqueNodes.add(uri.getHost() + ":" + uri.getPort());
        }

        return StageStats.builder()
                .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                .setState(stageInfo.getState().toString())
                .setDone(stageInfo.getState().isDone())
                .setNodes(uniqueNodes.size())
                .setTotalSplits(stageStats.getTotalDrivers())
                .setQueuedSplits(stageStats.getQueuedDrivers())
                .setRunningSplits(stageStats.getRunningDrivers() + stageStats.getBlockedDrivers())
                .setCompletedSplits(stageStats.getCompletedDrivers())
                .setCpuTimeMillis(stageStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(stageStats.getTotalScheduledTime().toMillis())
                .setProcessedRows(stageStats.getRawInputPositions())
                .setProcessedBytes(stageStats.getRawInputDataSize().toBytes())
                .setSubStages(subStages.build())
                .build();
    }

    private static Set<String> globalUniqueNodes(StageInfo stageInfo)
    {
        if (stageInfo == null) {
            return ImmutableSet.of();
        }
        ImmutableSet.Builder<String> nodes = ImmutableSet.builder();
        for (TaskInfo task : stageInfo.getTasks()) {
            // todo add nodeId to TaskInfo
            URI uri = task.getTaskStatus().getSelf();
            nodes.add(uri.getHost() + ":" + uri.getPort());
        }

        for (StageInfo subStage : stageInfo.getSubStages()) {
            nodes.addAll(globalUniqueNodes(subStage));
        }
        return nodes.build();
    }

    private static URI findCancelableLeafStage(QueryInfo queryInfo)
    {
        // if query is running, find the leaf-most running stage
        return queryInfo.getOutputStage().map(Query::findCancelableLeafStage).orElse(null);
    }

    private static URI findCancelableLeafStage(StageInfo stage)
    {
        // if this stage is already done, we can't cancel it
        if (stage.getState().isDone()) {
            return null;
        }

        // attempt to find a cancelable sub stage
        // check in reverse order since build side of a join will be later in the list
        for (StageInfo subStage : Lists.reverse(stage.getSubStages())) {
            URI leafStage = findCancelableLeafStage(subStage);
            if (leafStage != null) {
                return leafStage;
            }
        }

        // no matching sub stage, so return this stage
        return stage.getSelf();
    }

    private static QueryError toQueryError(QueryInfo queryInfo)
    {
        QueryState state = queryInfo.getState();
        if (state != FAILED) {
            return null;
        }

        FailureInfo failure;
        if (queryInfo.getFailureInfo() != null) {
            failure = queryInfo.getFailureInfo().toFailureInfo();
        }
        else {
            log.warn("Query %s in state %s has no failure info", queryInfo.getQueryId(), state);
            failure = toFailure(new RuntimeException(format("Query is %s (reason unknown)", state))).toFailureInfo();
        }

        ErrorCode errorCode;
        if (queryInfo.getErrorCode() != null) {
            errorCode = queryInfo.getErrorCode();
        }
        else {
            errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
            log.warn("Failed query %s has no error code", queryInfo.getQueryId());
        }
        return new QueryError(
                firstNonNull(failure.getMessage(), "Internal error"),
                null,
                errorCode.getCode(),
                errorCode.getName(),
                errorCode.getType().toString(),
                failure.getErrorLocation(),
                failure);
    }

    private static Warning toClientWarning(PrestoWarning warning)
    {
        WarningCode code = warning.getWarningCode();
        return new Warning(new Warning.Code(code.getCode(), code.getName()), warning.getMessage());
    }

    public static final class QueryResponse
    {
        private final Optional<String> setCatalog;
        private final Optional<String> setSchema;
        private final Optional<String> setPath;
        private final Map<String, String> setSessionProperties;
        private final Set<String> resetSessionProperties;
        private final Map<String, SelectedRole> setRoles;
        private final Map<String, String> addedPreparedStatements;
        private final Set<String> deallocatedPreparedStatements;
        private final Optional<TransactionId> startedTransactionId;
        private final boolean clearTransactionId;
        private final QueryResults queryResults;

        public QueryResponse(
                Optional<String> setCatalog,
                Optional<String> setSchema,
                Optional<String> setPath,
                Map<String, String> setSessionProperties,
                Set<String> resetSessionProperties,
                Map<String, SelectedRole> setRoles,
                Map<String, String> addedPreparedStatements,
                Set<String> deallocatedPreparedStatements,
                Optional<TransactionId> startedTransactionId,
                boolean clearTransactionId,
                QueryResults queryResults)
        {
            this.setCatalog = setCatalog;
            this.setSchema = setSchema;
            this.setPath = setPath;
            this.setSessionProperties = setSessionProperties;
            this.resetSessionProperties = resetSessionProperties;
            this.setRoles = setRoles;
            this.addedPreparedStatements = addedPreparedStatements;
            this.deallocatedPreparedStatements = deallocatedPreparedStatements;
            this.startedTransactionId = startedTransactionId;
            this.clearTransactionId = clearTransactionId;
            this.queryResults = queryResults;
        }

        public Optional<String> getSetCatalog()
        {
            return setCatalog;
        }

        public Optional<String> getSetSchema()
        {
            return setSchema;
        }

        public Optional<String> getSetPath()
        {
            return setPath;
        }

        public Map<String, String> getSetSessionProperties()
        {
            return setSessionProperties;
        }

        public Set<String> getResetSessionProperties()
        {
            return resetSessionProperties;
        }

        public Map<String, SelectedRole> getSetRoles()
        {
            return setRoles;
        }

        public Map<String, String> getAddedPreparedStatements()
        {
            return addedPreparedStatements;
        }

        public Set<String> getDeallocatedPreparedStatements()
        {
            return deallocatedPreparedStatements;
        }

        public Optional<TransactionId> getStartedTransactionId()
        {
            return startedTransactionId;
        }

        public boolean isClearTransactionId()
        {
            return clearTransactionId;
        }

        public QueryResults getQueryResults()
        {
            return queryResults;
        }
    }
}
