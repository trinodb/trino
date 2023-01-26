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

import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.client.ProtocolHeaders;
import io.trino.exchange.ExchangeManagerRegistry;
import io.trino.execution.QueryManager;
import io.trino.operator.DirectExchangeClientSupplier;
import io.trino.server.ForStatementResource;
import io.trino.server.ServerConfig;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import io.trino.spi.block.BlockEncodingSerde;
import jakarta.annotation.PreDestroy;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.ResponseBuilder;
import jakarta.ws.rs.core.UriInfo;

import java.net.URLEncoder;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.server.protocol.Slug.Context.EXECUTING_QUERY;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Path("/v1/statement/executing")
public class ExecutingStatementResource
{
    private static final Logger log = Logger.get(ExecutingStatementResource.class);
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();

    private static final DataSize DEFAULT_TARGET_RESULT_SIZE = DataSize.of(1, MEGABYTE);
    private static final DataSize MAX_TARGET_RESULT_SIZE = DataSize.of(128, MEGABYTE);

    private final QueryManager queryManager;
    private final DirectExchangeClientSupplier directExchangeClientSupplier;
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final BlockEncodingSerde blockEncodingSerde;
    private final QueryInfoUrlFactory queryInfoUrlFactory;
    private final BoundedExecutor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;

    private final ConcurrentMap<QueryId, Query> queries = new ConcurrentHashMap<>();
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(threadsNamed("execution-query-purger"));
    private final PreparedStatementEncoder preparedStatementEncoder;
    private final boolean compressionEnabled;

    @Inject
    public ExecutingStatementResource(
            QueryManager queryManager,
            DirectExchangeClientSupplier directExchangeClientSupplier,
            ExchangeManagerRegistry exchangeManagerRegistry,
            BlockEncodingSerde blockEncodingSerde,
            QueryInfoUrlFactory queryInfoUrlTemplate,
            @ForStatementResource BoundedExecutor responseExecutor,
            @ForStatementResource ScheduledExecutorService timeoutExecutor,
            PreparedStatementEncoder preparedStatementEncoder,
            ServerConfig serverConfig)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.directExchangeClientSupplier = requireNonNull(directExchangeClientSupplier, "directExchangeClientSupplier is null");
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.queryInfoUrlFactory = requireNonNull(queryInfoUrlTemplate, "queryInfoUrlTemplate is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.preparedStatementEncoder = requireNonNull(preparedStatementEncoder, "preparedStatementEncoder is null");
        this.compressionEnabled = serverConfig.isQueryResultsCompressionEnabled();

        queryPurger.scheduleWithFixedDelay(
                () -> {
                    try {
                        for (QueryId queryId : queries.keySet()) {
                            // forget about this query if the query manager is no longer tracking it
                            if (!queryManager.hasQuery(queryId)) {
                                Query query = queries.remove(queryId);
                                if (query != null) {
                                    query.dispose();
                                }
                            }
                        }
                    }
                    catch (Throwable e) {
                        log.warn(e, "Error removing old queries");
                    }

                    try {
                        for (Query query : queries.values()) {
                            query.markResultsConsumedIfReady();
                        }
                    }
                    catch (Throwable e) {
                        log.warn(e, "Error marking results consumed");
                    }
                },
                200,
                200,
                MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        queryPurger.shutdownNow();
    }

    @ResourceSecurity(PUBLIC)
    @GET
    @Path("{queryId}/{slug}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getQueryResults(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token,
            @QueryParam("maxWait") Duration maxWait,
            @QueryParam("targetResultSize") DataSize targetResultSize,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        Query query = getQuery(queryId, slug, token);
        asyncQueryResults(query, token, maxWait, targetResultSize, uriInfo, asyncResponse);
    }

    protected Query getQuery(QueryId queryId, String slug, long token)
    {
        Query query = queries.get(queryId);
        if (query != null) {
            if (!query.isSlugValid(slug, token)) {
                throw queryNotFound();
            }
            return query;
        }

        // this is the first time the query has been accessed on this coordinator
        Session session;
        Slug querySlug;
        try {
            session = queryManager.getQuerySession(queryId);
            querySlug = queryManager.getQuerySlug(queryId);
            if (!querySlug.isValid(EXECUTING_QUERY, slug, token)) {
                throw queryNotFound();
            }
        }
        catch (NoSuchElementException e) {
            throw queryNotFound();
        }

        query = queries.computeIfAbsent(queryId, id -> Query.create(
                session,
                querySlug,
                queryManager,
                queryInfoUrlFactory.getQueryInfoUrl(queryId),
                directExchangeClientSupplier,
                exchangeManagerRegistry,
                responseExecutor,
                timeoutExecutor,
                blockEncodingSerde));
        return query;
    }

    private void asyncQueryResults(
            Query query,
            long token,
            Duration maxWait,
            DataSize targetResultSize,
            UriInfo uriInfo,
            AsyncResponse asyncResponse)
    {
        Duration wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        if (targetResultSize == null) {
            targetResultSize = DEFAULT_TARGET_RESULT_SIZE;
        }
        else {
            targetResultSize = Ordering.natural().min(targetResultSize, MAX_TARGET_RESULT_SIZE);
        }
        ListenableFuture<QueryResultsResponse> queryResultsFuture = query.waitForResults(token, uriInfo, wait, targetResultSize);

        ListenableFuture<Response> response = Futures.transform(queryResultsFuture, this::toResponse, directExecutor());

        bindAsyncResponse(asyncResponse, response, responseExecutor);
    }

    private Response toResponse(QueryResultsResponse resultsResponse)
    {
        ResponseBuilder response = Response.ok(resultsResponse.queryResults());

        ProtocolHeaders protocolHeaders = resultsResponse.protocolHeaders();
        resultsResponse.setCatalog().ifPresent(catalog -> response.header(protocolHeaders.responseSetCatalog(), catalog));
        resultsResponse.setSchema().ifPresent(schema -> response.header(protocolHeaders.responseSetSchema(), schema));
        resultsResponse.setPath().ifPresent(path -> response.header(protocolHeaders.responseSetPath(), path));
        resultsResponse.setAuthorizationUser().ifPresent(authorizationUser -> response.header(protocolHeaders.responseSetAuthorizationUser(), authorizationUser));
        if (resultsResponse.resetAuthorizationUser()) {
            response.header(protocolHeaders.responseResetAuthorizationUser(), true);
        }

        // add set session properties
        resultsResponse.setSessionProperties()
                .forEach((key, value) -> response.header(protocolHeaders.responseSetSession(), key + '=' + urlEncode(value)));

        // add clear session properties
        resultsResponse.resetSessionProperties()
                .forEach(name -> response.header(protocolHeaders.responseClearSession(), name));

        // add set roles
        resultsResponse.setRoles()
                .forEach((key, value) -> response.header(protocolHeaders.responseSetRole(), key + '=' + urlEncode(value.toString())));

        // add added prepare statements
        for (Entry<String, String> entry : resultsResponse.addedPreparedStatements().entrySet()) {
            String encodedKey = urlEncode(entry.getKey());
            String encodedValue = urlEncode(preparedStatementEncoder.encodePreparedStatementForHeader(entry.getValue()));
            response.header(protocolHeaders.responseAddedPrepare(), encodedKey + '=' + encodedValue);
        }

        // add deallocated prepare statements
        for (String name : resultsResponse.deallocatedPreparedStatements()) {
            response.header(protocolHeaders.responseDeallocatedPrepare(), urlEncode(name));
        }

        // add new transaction ID
        resultsResponse.startedTransactionId()
                .ifPresent(transactionId -> response.header(protocolHeaders.responseStartedTransactionId(), transactionId));

        // add clear transaction ID directive
        if (resultsResponse.clearTransactionId()) {
            response.header(protocolHeaders.responseClearTransactionId(), true);
        }

        if (!compressionEnabled) {
            response.encoding("identity");
        }

        return response.build();
    }

    @ResourceSecurity(PUBLIC)
    @DELETE
    @Path("{queryId}/{slug}/{token}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response cancelQuery(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token)
    {
        Query query = queries.get(queryId);
        if (query != null) {
            if (!query.isSlugValid(slug, token)) {
                throw queryNotFound();
            }
            query.cancel();
            return Response.noContent().build();
        }

        // cancel the query execution directly instead of creating the statement client
        try {
            if (!queryManager.getQuerySlug(queryId).isValid(EXECUTING_QUERY, slug, token)) {
                throw queryNotFound();
            }
            queryManager.cancelQuery(queryId);
            return Response.noContent().build();
        }
        catch (NoSuchElementException e) {
            throw queryNotFound();
        }
    }

    @ResourceSecurity(PUBLIC)
    @DELETE
    @Path("partialCancel/{queryId}/{stage}/{slug}/{token}")
    public void partialCancel(
            @PathParam("queryId") QueryId queryId,
            @PathParam("stage") int stage,
            @PathParam("slug") String slug,
            @PathParam("token") long token)
    {
        Query query = getQuery(queryId, slug, token);
        query.partialCancel(stage);
    }

    private static WebApplicationException queryNotFound()
    {
        throw new WebApplicationException(
                Response.status(NOT_FOUND)
                        .type(TEXT_PLAIN_TYPE)
                        .entity("Query not found")
                        .build());
    }

    private static String urlEncode(String value)
    {
        return URLEncoder.encode(value, UTF_8);
    }
}
