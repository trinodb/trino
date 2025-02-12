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
package io.trino.dispatcher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.trino.client.QueryError;
import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import io.trino.client.TypedQueryData;
import io.trino.execution.ExecutionFailureInfo;
import io.trino.execution.QueryManagerConfig;
import io.trino.execution.QueryState;
import io.trino.server.ExternalUriInfo;
import io.trino.server.GoneException;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.ServerConfig;
import io.trino.server.SessionContext;
import io.trino.server.protocol.QueryInfoUrlFactory;
import io.trino.server.protocol.Slug;
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.ErrorCode;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.tracing.TrinoAttributes;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.execution.QueryState.FAILED;
import static io.trino.execution.QueryState.QUEUED;
import static io.trino.server.ServletSecurityUtils.authenticatedIdentity;
import static io.trino.server.ServletSecurityUtils.clearAuthenticatedIdentity;
import static io.trino.server.protocol.QueryInfoUrlFactory.getQueryInfoUri;
import static io.trino.server.protocol.Slug.Context.EXECUTING_QUERY;
import static io.trino.server.protocol.Slug.Context.QUEUED_QUERY;
import static io.trino.server.security.ResourceSecurity.AccessType.AUTHENTICATED_USER;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/v1/statement")
public class QueuedStatementResource
{
    private static final Logger log = Logger.get(QueuedStatementResource.class);
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final Duration NO_DURATION = new Duration(0, MILLISECONDS);

    private final HttpRequestSessionContextFactory sessionContextFactory;
    private final DispatchManager dispatchManager;
    private final Tracer tracer;

    private final QueryInfoUrlFactory queryInfoUrlFactory;

    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final boolean compressionEnabled;
    private final QueryManager queryManager;

    @Inject
    public QueuedStatementResource(
            HttpRequestSessionContextFactory sessionContextFactory,
            DispatchManager dispatchManager,
            Tracer tracer,
            DispatchExecutor executor,
            QueryInfoUrlFactory queryInfoUrlTemplate,
            ServerConfig serverConfig,
            QueryManagerConfig queryManagerConfig)
    {
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.responseExecutor = executor.getExecutor();
        this.timeoutExecutor = executor.getScheduledExecutor();
        this.queryInfoUrlFactory = requireNonNull(queryInfoUrlTemplate, "queryInfoUrlTemplate is null");
        this.compressionEnabled = serverConfig.isQueryResultsCompressionEnabled();
        queryManager = new QueryManager(queryManagerConfig.getClientTimeout());
    }

    @PostConstruct
    public void start()
    {
        queryManager.initialize(dispatchManager);
    }

    @PreDestroy
    public void stop()
    {
        queryManager.destroy();
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @HEAD
    @Produces(APPLICATION_JSON)
    public Response validateConnection()
    {
        return Response.ok().build();
    }

    @ResourceSecurity(AUTHENTICATED_USER)
    @POST
    @Produces(APPLICATION_JSON)
    public Response postStatement(
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders,
            @BeanParam ExternalUriInfo externalUriInfo)
    {
        if (isNullOrEmpty(statement)) {
            throw new BadRequestException("SQL statement is empty");
        }

        Query query = registerQuery(statement, servletRequest, httpHeaders);
        return createQueryResultsResponse(query.getQueryResults(query.getLastToken(), externalUriInfo), query.sessionContext.getQueryDataEncoding());
    }

    private Query registerQuery(String statement, HttpServletRequest servletRequest, HttpHeaders httpHeaders)
    {
        Optional<String> remoteAddress = Optional.ofNullable(servletRequest.getRemoteAddr());
        Optional<Identity> identity = authenticatedIdentity(servletRequest);
        if (identity.flatMap(Identity::getPrincipal).map(InternalPrincipal.class::isInstance).orElse(false)) {
            throw new ForbiddenException("Internal communication can not be used to start a query");
        }

        MultivaluedMap<String, String> headers = httpHeaders.getRequestHeaders();

        SessionContext sessionContext = sessionContextFactory.createSessionContext(headers, remoteAddress, identity);
        Query query = new Query(statement, sessionContext, dispatchManager, queryInfoUrlFactory, tracer);
        queryManager.registerQuery(query);

        // let authentication filter know that identity lifecycle has been handed off
        clearAuthenticatedIdentity(servletRequest);

        return query;
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("queued/{queryId}/{slug}/{token}")
    @Produces(APPLICATION_JSON)
    public void getStatus(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token,
            @QueryParam("maxWait") Duration maxWait,
            @BeanParam ExternalUriInfo externalUriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        Query query = getQuery(queryId, slug, token);

        ListenableFuture<Response> future = getStatus(query, token, maxWait, externalUriInfo);
        bindAsyncResponse(asyncResponse, future, responseExecutor);
    }

    private ListenableFuture<Response> getStatus(Query query, long token, Duration maxWait, ExternalUriInfo externalUriInfo)
    {
        long waitMillis = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait).toMillis();

        return FluentFuture.from(query.waitForDispatched())
                // wait for query to be dispatched, up to the wait timeout
                .withTimeout(waitMillis, MILLISECONDS, timeoutExecutor)
                .catching(TimeoutException.class, _ -> null, directExecutor())
                // when state changes, fetch the next result
                .transform(_ -> query.getQueryResults(token, externalUriInfo), responseExecutor)
                .transform(results -> createQueryResultsResponse(results, query.sessionContext.getQueryDataEncoding()), directExecutor());
    }

    @ResourceSecurity(PUBLIC)
    @DELETE
    @Path("queued/{queryId}/{slug}/{token}")
    @Produces(APPLICATION_JSON)
    public Response cancelQuery(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token)
    {
        getQuery(queryId, slug, token)
                .cancel();
        return Response.noContent().build();
    }

    private Query getQuery(QueryId queryId, String slug, long token)
    {
        Query query = queryManager.getQuery(queryId);
        if (query == null || !query.getSlug().isValid(QUEUED_QUERY, slug, token)) {
            throw new NotFoundException("Query not found");
        }
        return query;
    }

    private Response createQueryResultsResponse(QueryResults results, Optional<String> queryDataEncoding)
    {
        Response.ResponseBuilder builder = Response.ok(results);
        if (!compressionEnabled) {
            builder.encoding("identity");
        }
        queryDataEncoding.ifPresent(encoding -> builder.header(TRINO_HEADERS.responseQueryDataEncoding(), encoding));
        return builder.build();
    }

    private static URI getQueuedUri(QueryId queryId, Slug slug, long token, ExternalUriInfo externalUriInfo)
    {
        return externalUriInfo.baseUriBuilder()
                .path("/v1/statement/queued/")
                .path(queryId.toString())
                .path(slug.makeSlug(QUEUED_QUERY, token))
                .path(String.valueOf(token))
                .build();
    }

    private static QueryResults createQueryResults(
            QueryId queryId,
            URI nextUri,
            Optional<QueryError> queryError,
            ExternalUriInfo externalUriInfo,
            Optional<URI> queryInfoUrl,
            Duration elapsedTime,
            Duration queuedTime)
    {
        QueryState state = queryError.map(error -> FAILED).orElse(QUEUED);
        return new QueryResults(
                queryId.toString(),
                getQueryInfoUri(queryInfoUrl, queryId, externalUriInfo),
                null,
                nextUri,
                null,
                TypedQueryData.of(null),
                StatementStats.builder()
                        .setState(state.toString())
                        .setQueued(state == QUEUED)
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .setElapsedTimeMillis(elapsedTime.toMillis())
                        .setQueuedTimeMillis(queuedTime.toMillis())
                        .build(),
                queryError.orElse(null),
                ImmutableList.of(),
                null,
                null);
    }

    private static final class Query
    {
        private final String query;
        private final SessionContext sessionContext;
        private final DispatchManager dispatchManager;
        private final QueryId queryId;
        private final Optional<URI> queryInfoUrl;
        private final Span querySpan;
        private final Slug slug = Slug.createNew();
        private final AtomicLong lastToken = new AtomicLong();

        private final long initTime = System.nanoTime();
        private final AtomicReference<Boolean> submissionGate = new AtomicReference<>();
        private final SettableFuture<Void> creationFuture = SettableFuture.create();

        public Query(String query, SessionContext sessionContext, DispatchManager dispatchManager, QueryInfoUrlFactory queryInfoUrlFactory, Tracer tracer)
        {
            this.query = requireNonNull(query, "query is null");
            this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
            this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
            this.queryId = dispatchManager.createQueryId();
            requireNonNull(queryInfoUrlFactory, "queryInfoUrlFactory is null");
            this.queryInfoUrl = queryInfoUrlFactory.getQueryInfoUrl(queryId);
            requireNonNull(tracer, "tracer is null");
            this.querySpan = tracer.spanBuilder("query")
                    .setAttribute(TrinoAttributes.QUERY_ID, queryId.toString())
                    .startSpan();
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public Slug getSlug()
        {
            return slug;
        }

        public long getLastToken()
        {
            return lastToken.get();
        }

        public boolean tryAbandonSubmissionWithTimeout(Duration querySubmissionTimeout)
        {
            return Duration.nanosSince(initTime).compareTo(querySubmissionTimeout) >= 0 && submissionGate.compareAndSet(null, false);
        }

        public boolean isSubmissionAbandoned()
        {
            return Boolean.FALSE.equals(submissionGate.get());
        }

        public boolean isCreated()
        {
            return creationFuture.isDone();
        }

        private ListenableFuture<Void> waitForDispatched()
        {
            submitIfNeeded();
            if (!creationFuture.isDone()) {
                return nonCancellationPropagating(creationFuture);
            }
            // otherwise, wait for the query to finish
            return dispatchManager.waitForDispatched(queryId);
        }

        private void submitIfNeeded()
        {
            if (submissionGate.compareAndSet(null, true)) {
                querySpan.addEvent("submit");
                creationFuture.setFuture(dispatchManager.createQuery(queryId, querySpan, slug, sessionContext, query));
            }
        }

        public QueryResults getQueryResults(long token, ExternalUriInfo externalUriInfo)
        {
            long lastToken = this.lastToken.get();
            // token should be the last token or the next token
            if (token != lastToken && token != lastToken + 1) {
                throw new GoneException("Invalid token");
            }
            // advance (or stay at) the token
            this.lastToken.compareAndSet(lastToken, token);

            // if query submission has not finished, return simple empty result
            if (!creationFuture.isDone()) {
                return createQueryResults(
                        token + 1,
                        externalUriInfo,
                        DispatchInfo.queued(NO_DURATION, NO_DURATION));
            }

            DispatchInfo dispatchInfo = dispatchManager.getDispatchInfo(queryId)
                    // query should always be found, but it may have just been determined to be abandoned
                    .orElseThrow(NotFoundException::new);

            return createQueryResults(token + 1, externalUriInfo, dispatchInfo);
        }

        public void cancel()
        {
            creationFuture.addListener(() -> dispatchManager.cancelQuery(queryId), directExecutor());
        }

        public void destroy()
        {
            querySpan.setStatus(StatusCode.ERROR).end();
            sessionContext.getIdentity().destroy();
        }

        private QueryResults createQueryResults(long token, ExternalUriInfo externalUriInfo, DispatchInfo dispatchInfo)
        {
            URI nextUri = getNextUri(token, externalUriInfo, dispatchInfo);

            Optional<QueryError> queryError = dispatchInfo.getFailureInfo()
                    .map(this::toQueryError);

            return QueuedStatementResource.createQueryResults(
                    queryId,
                    nextUri,
                    queryError,
                    externalUriInfo,
                    queryInfoUrl,
                    dispatchInfo.getElapsedTime(),
                    dispatchInfo.getQueuedTime());
        }

        private URI getNextUri(long token, ExternalUriInfo externalUriInfo, DispatchInfo dispatchInfo)
        {
            // if failed, query is complete
            if (dispatchInfo.getFailureInfo().isPresent()) {
                return null;
            }
            // if dispatched, redirect to new uri
            return dispatchInfo.getCoordinatorLocation()
                    .map(coordinatorLocation -> getRedirectUri(coordinatorLocation, externalUriInfo))
                    .orElseGet(() -> getQueuedUri(queryId, slug, token, externalUriInfo));
        }

        private URI getRedirectUri(CoordinatorLocation coordinatorLocation, ExternalUriInfo externalUriInfo)
        {
            return coordinatorLocation.getUri(externalUriInfo)
                    .path("/v1/statement/executing")
                    .path(queryId.toString())
                    .path(slug.makeSlug(EXECUTING_QUERY, 0))
                    .path("0")
                    .build();
        }

        private QueryError toQueryError(ExecutionFailureInfo executionFailureInfo)
        {
            ErrorCode errorCode;
            if (executionFailureInfo.getErrorCode() != null) {
                errorCode = executionFailureInfo.getErrorCode();
            }
            else {
                errorCode = GENERIC_INTERNAL_ERROR.toErrorCode();
                log.warn("Failed query %s has no error code", queryId);
            }

            return new QueryError(
                    firstNonNull(executionFailureInfo.getMessage(), "Internal error"),
                    null,
                    errorCode.getCode(),
                    errorCode.getName(),
                    errorCode.getType().toString(),
                    executionFailureInfo.getErrorLocation(),
                    executionFailureInfo.toFailureInfo());
        }
    }

    @ThreadSafe
    private static class QueryManager
    {
        private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
        private final ScheduledExecutorService scheduledExecutorService = newSingleThreadScheduledExecutor(daemonThreadsNamed("drain-state-query-manager"));

        private final Duration querySubmissionTimeout;

        public QueryManager(Duration querySubmissionTimeout)
        {
            this.querySubmissionTimeout = requireNonNull(querySubmissionTimeout, "querySubmissionTimeout is null");
        }

        public void initialize(DispatchManager dispatchManager)
        {
            scheduledExecutorService.scheduleWithFixedDelay(() -> {
                try {
                    syncWith(dispatchManager);
                }
                catch (Throwable e) {
                    // ignore to avoid getting unscheduled
                    log.error(e, "Unexpected error synchronizing with dispatch manager");
                }
            }, 200, 200, MILLISECONDS);
        }

        public void destroy()
        {
            scheduledExecutorService.shutdownNow();
        }

        private void syncWith(DispatchManager dispatchManager)
        {
            queries.forEach((queryId, query) -> {
                if (shouldBePurged(dispatchManager, query)) {
                    removeQuery(queryId);
                }
            });
        }

        private boolean shouldBePurged(DispatchManager dispatchManager, Query query)
        {
            if (query.isSubmissionAbandoned()) {
                // Query submission was explicitly abandoned
                return true;
            }
            if (query.tryAbandonSubmissionWithTimeout(querySubmissionTimeout)) {
                // Query took too long to be submitted by the client
                return true;
            }
            // Query was created in the DispatchManager, and DispatchManager has already purged the query
            return query.isCreated() && !dispatchManager.isQueryRegistered(query.getQueryId());
        }

        private void removeQuery(QueryId queryId)
        {
            Optional.ofNullable(queries.remove(queryId))
                    .ifPresent(QueryManager::destroyQuietly);
        }

        private static void destroyQuietly(Query query)
        {
            try {
                query.destroy();
            }
            catch (Throwable t) {
                log.error(t, "Error destroying query");
            }
        }

        public void registerQuery(Query query)
        {
            Query existingQuery = queries.putIfAbsent(query.getQueryId(), query);
            checkState(existingQuery == null, "Query already registered");
        }

        @Nullable
        public Query getQuery(QueryId queryId)
        {
            return queries.get(queryId);
        }
    }
}
