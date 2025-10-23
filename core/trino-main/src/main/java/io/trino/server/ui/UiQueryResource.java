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

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.operator.OperatorInfo;
import io.trino.plugin.base.metrics.TDigestHistogram;
import io.trino.security.AccessControl;
import io.trino.server.BasicQueryInfo;
import io.trino.server.DisableHttpCache;
import io.trino.server.GoneException;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.metrics.Metric;
import io.trino.spi.security.AccessDeniedException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.fasterxml.jackson.annotation.JsonIgnoreProperties.Value.forIgnoredProperties;
import static io.trino.connector.system.KillQueryProcedure.createKillQueryException;
import static io.trino.connector.system.KillQueryProcedure.createPreemptQueryException;
import static io.trino.plugin.base.metrics.TDigestHistogram.DIGEST_PROPERTY;
import static io.trino.security.AccessControlUtil.checkCanKillQueryOwnedBy;
import static io.trino.security.AccessControlUtil.checkCanViewQueryOwnedBy;
import static io.trino.security.AccessControlUtil.filterQueries;
import static io.trino.server.DataSizeSerializer.SUCCINCT_DATA_SIZE_ENABLED;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static java.util.Objects.requireNonNull;

@Path("/ui/api/query")
@ResourceSecurity(WEB_UI)
@DisableHttpCache
public class UiQueryResource
{
    private final JsonCodec<QueryInfo> queryInfoCodec;
    private final JsonCodec<QueryInfo> prettyQueryInfoCodec;
    private final DispatchManager dispatchManager;
    private final AccessControl accessControl;
    private final HttpRequestSessionContextFactory sessionContextFactory;

    @Inject
    public UiQueryResource(ObjectMapper objectMapper, DispatchManager dispatchManager, AccessControl accessControl, HttpRequestSessionContextFactory sessionContextFactory)
    {
        this.queryInfoCodec = buildQueryInfoCodec(objectMapper, false);
        this.prettyQueryInfoCodec = buildQueryInfoCodec(objectMapper, true);
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
    }

    @GET
    public List<TrimmedBasicQueryInfo> getAllQueryInfo(@QueryParam("state") String stateFilter, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        QueryState expectedState = stateFilter == null ? null : QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));

        List<BasicQueryInfo> queries = dispatchManager.getQueries();
        queries = filterQueries(sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders), queries, accessControl);

        ImmutableList.Builder<TrimmedBasicQueryInfo> builder = ImmutableList.builder();
        for (BasicQueryInfo queryInfo : queries) {
            if (stateFilter == null || queryInfo.getState() == expectedState) {
                builder.add(new TrimmedBasicQueryInfo(queryInfo));
            }
        }
        return builder.build();
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") QueryId queryId, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        requireNonNull(queryId, "queryId is null");

        Optional<QueryInfo> queryInfo = dispatchManager.getFullQueryInfo(queryId);
        if (queryInfo.isPresent()) {
            try {
                checkCanViewQueryOwnedBy(sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders), queryInfo.get().getSession().toIdentity(), accessControl);

                String queryString = servletRequest.getQueryString();
                if (queryString != null && queryString.contains("pretty")) {
                    // Use pretty JSON codec that reduces noise
                    return Response.ok(prettyQueryInfoCodec.toJson(queryInfo.get()), APPLICATION_JSON_TYPE).build();
                }
                return Response.ok(queryInfoCodec.toJson(queryInfo.get()), APPLICATION_JSON_TYPE).build();
            }
            catch (AccessDeniedException e) {
                throw new ForbiddenException();
            }
        }
        throw new GoneException();
    }

    @PUT
    @Path("{queryId}/killed")
    public Response killQuery(@PathParam("queryId") QueryId queryId, String message, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        return failQuery(queryId, createKillQueryException(message), servletRequest, httpHeaders);
    }

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

            checkCanKillQueryOwnedBy(sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders), queryInfo.getSession().toIdentity(), accessControl);

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
            throw new GoneException();
        }
    }

    private JsonCodec<QueryInfo> buildQueryInfoCodec(ObjectMapper objectMapper, boolean pretty)
    {
        JsonCodecFactory jsonCodecFactory = new JsonCodecFactory(() -> {
            // Enable succinct DataSize serialization for QueryInfo to make it more human friendly
            ContextAttributes attrs = ContextAttributes.getEmpty();
            if (pretty) {
                attrs = attrs.withSharedAttribute(SUCCINCT_DATA_SIZE_ENABLED, Boolean.TRUE);
            }

            ObjectMapper mapper = objectMapper
                    .copy()
                    .setDefaultAttributes(attrs);
            // Don't serialize TDigestHistogram.digest which isn't useful and human readable
            mapper.configOverride(TDigestHistogram.class).setIgnorals(forIgnoredProperties(DIGEST_PROPERTY));

            // Do not output @class property for metric types
            mapper.addMixIn(Metric.class, DropTypeInfo.class);
            // Do not output @type property for OperatorInfo
            mapper.addMixIn(OperatorInfo.class, DropTypeInfo.class);
            return mapper;
        });

        if (pretty) {
            jsonCodecFactory = jsonCodecFactory.prettyPrint();
        }

        return jsonCodecFactory.jsonCodec(QueryInfo.class);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
    public interface DropTypeInfo {}
}
