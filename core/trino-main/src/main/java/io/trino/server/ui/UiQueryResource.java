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
package io.trino.server.ui;

import com.google.common.collect.ImmutableList;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.security.AccessControl;
import io.trino.server.BasicQueryInfo;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.ProtocolConfig;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.security.AccessDeniedException;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Optional;

import static io.trino.connector.system.KillQueryProcedure.createKillQueryException;
import static io.trino.connector.system.KillQueryProcedure.createPreemptQueryException;
import static io.trino.security.AccessControlUtil.checkCanKillQueryOwnedBy;
import static io.trino.security.AccessControlUtil.checkCanViewQueryOwnedBy;
import static io.trino.security.AccessControlUtil.filterQueries;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static java.util.Objects.requireNonNull;

@Path("/ui/api/query")
public class UiQueryResource
{
    private final DispatchManager dispatchManager;
    private final AccessControl accessControl;
    private final HttpRequestSessionContextFactory sessionContextFactory;
    private final Optional<String> alternateHeaderName;

    @Inject
    public UiQueryResource(DispatchManager dispatchManager, AccessControl accessControl, HttpRequestSessionContextFactory sessionContextFactory, ProtocolConfig protocolConfig)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
        this.alternateHeaderName = protocolConfig.getAlternateHeaderName();
    }

    @ResourceSecurity(WEB_UI)
    @GET
    public List<BasicQueryInfo> getAllQueryInfo(@QueryParam("state") String stateFilter, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        QueryState expectedState = stateFilter == null ? null : QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));

        List<BasicQueryInfo> queries = dispatchManager.getQueries();
        queries = filterQueries(sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders, alternateHeaderName), queries, accessControl);

        ImmutableList.Builder<BasicQueryInfo> builder = new ImmutableList.Builder<>();
        for (BasicQueryInfo queryInfo : queries) {
            if (stateFilter == null || queryInfo.getState() == expectedState) {
                builder.add(queryInfo);
            }
        }
        return builder.build();
    }

    @ResourceSecurity(WEB_UI)
    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") QueryId queryId, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        requireNonNull(queryId, "queryId is null");

        Optional<QueryInfo> queryInfo = dispatchManager.getFullQueryInfo(queryId);
        if (queryInfo.isPresent()) {
            try {
                checkCanViewQueryOwnedBy(sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders, alternateHeaderName), queryInfo.get().getSession().getUser(), accessControl);
                return Response.ok(queryInfo.get()).build();
            }
            catch (AccessDeniedException e) {
                throw new ForbiddenException();
            }
        }
        return Response.status(Status.GONE).build();
    }

    @ResourceSecurity(WEB_UI)
    @PUT
    @Path("{queryId}/killed")
    public Response killQuery(@PathParam("queryId") QueryId queryId, String message, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        return failQuery(queryId, createKillQueryException(message), servletRequest, httpHeaders);
    }

    @ResourceSecurity(WEB_UI)
    @PUT
    @Path("{queryId}/preempted")
    public Response preemptQuery(@PathParam("queryId") QueryId queryId, String message, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        return failQuery(queryId, createPreemptQueryException(message), servletRequest, httpHeaders);
    }

    private Response failQuery(QueryId queryId, TrinoException queryException, HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        requireNonNull(queryId, "queryId is null");

        try {
            BasicQueryInfo queryInfo = dispatchManager.getQueryInfo(queryId);

            checkCanKillQueryOwnedBy(sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders, alternateHeaderName), queryInfo.getSession().getUser(), accessControl);

            // check before killing to provide the proper error code (this is racy)
            if (queryInfo.getState().isDone()) {
                return Response.status(Status.CONFLICT).build();
            }

            dispatchManager.failQuery(queryId, queryException);

            return Response.status(Status.ACCEPTED).build();
        }
        catch (AccessDeniedException e) {
            throw new ForbiddenException();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }
}
