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
package io.prestosql.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.dispatcher.DispatchQuery;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManager;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStats;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.connector.system.KillQueryProcedure.createKillQueryException;
import static io.prestosql.connector.system.KillQueryProcedure.createPreemptQueryException;
import static java.util.Objects.requireNonNull;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/query")
public class QueryResource
{
    private static final DataSize ZERO_BYTES = new DataSize(0, DataSize.Unit.BYTE);
    private static final Duration ZERO_MILLIS = new Duration(0, TimeUnit.MILLISECONDS);

    // TODO There should be a combined interface for this
    private final DispatchManager dispatchManager;
    private final QueryManager queryManager;

    @Inject
    public QueryResource(DispatchManager dispatchManager, QueryManager queryManager)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @GET
    public List<BasicQueryInfo> getAllQueryInfo(@QueryParam("state") String stateFilter)
    {
        QueryState expectedState = stateFilter == null ? null : QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));
        ImmutableList.Builder<BasicQueryInfo> builder = new ImmutableList.Builder<>();
        for (BasicQueryInfo queryInfo : dispatchManager.getQueries()) {
            if (stateFilter == null || queryInfo.getState() == expectedState) {
                builder.add(queryInfo);
            }
        }
        return builder.build();
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");

        try {
            QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
            return Response.ok(queryInfo).build();
        }
        catch (NoSuchElementException ignored) {
        }

        try {
            DispatchQuery query = dispatchManager.getQuery(queryId);
            if (query.isDone()) {
                return Response.ok(toFullQueryInfo(query)).build();
            }
        }
        catch (NoSuchElementException ignored) {
        }

        return Response.status(Status.GONE).build();
    }

    @DELETE
    @Path("{queryId}")
    public void cancelQuery(@PathParam("queryId") QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        dispatchManager.cancelQuery(queryId);
    }

    @PUT
    @Path("{queryId}/killed")
    public Response killQuery(@PathParam("queryId") QueryId queryId, String message)
    {
        return failQuery(queryId, createKillQueryException(message));
    }

    @PUT
    @Path("{queryId}/preempted")
    public Response preemptQuery(@PathParam("queryId") QueryId queryId, String message)
    {
        return failQuery(queryId, createPreemptQueryException(message));
    }

    private Response failQuery(QueryId queryId, PrestoException queryException)
    {
        requireNonNull(queryId, "queryId is null");

        try {
            QueryState state = queryManager.getQueryState(queryId);

            // check before killing to provide the proper error code (this is racy)
            if (state.isDone()) {
                return Response.status(Status.CONFLICT).build();
            }

            queryManager.failQuery(queryId, queryException);

            // verify if the query was failed (if not, we lost the race)
            if (!queryException.getErrorCode().equals(queryManager.getQueryInfo(queryId).getErrorCode())) {
                return Response.status(Status.CONFLICT).build();
            }

            return Response.status(Status.OK).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    private static QueryInfo toFullQueryInfo(DispatchQuery query)
    {
        checkArgument(query.isDone(), "query is not done");
        BasicQueryInfo info = query.getBasicQueryInfo();
        BasicQueryStats stats = info.getQueryStats();

        QueryStats queryStats = new QueryStats(
                query.getCreateTime(),
                query.getExecutionStartTime().orElse(null),
                query.getLastHeartbeat(),
                query.getEndTime().orElse(null),
                stats.getElapsedTime(),
                stats.getQueuedTime(),
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                ZERO_BYTES,
                info.isScheduled(),
                ZERO_MILLIS,
                ZERO_MILLIS,
                ZERO_MILLIS,
                false,
                ImmutableSet.of(),
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                0,
                ZERO_BYTES,
                ImmutableList.of(),
                ImmutableList.of());

        return new QueryInfo(
                info.getQueryId(),
                info.getSession(),
                info.getState(),
                info.getMemoryPool(),
                info.isScheduled(),
                info.getSelf(),
                ImmutableList.of(),
                info.getQuery(),
                info.getPreparedQuery(),
                queryStats,
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
                null,
                Optional.empty(),
                query.getDispatchInfo().getFailureInfo().orElse(null),
                info.getErrorCode(),
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                true,
                info.getResourceGroupId());
    }
}
