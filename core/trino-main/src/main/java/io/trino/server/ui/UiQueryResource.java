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
import com.fasterxml.jackson.databind.cfg.ContextAttributes;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryState;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.StagesInfo;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.SessionPropertyManager;
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
import io.trino.spi.security.Identity;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.planprinter.JsonRenderer.JsonRenderedNode;
import io.trino.sql.planner.planprinter.NoOpAnonymizer;
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
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

import static com.fasterxml.jackson.annotation.JsonIgnoreProperties.Value.forIgnoredProperties;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.connector.system.KillQueryProcedure.createKillQueryException;
import static io.trino.connector.system.KillQueryProcedure.createPreemptQueryException;
import static io.trino.plugin.base.metrics.TDigestHistogram.DIGEST_PROPERTY;
import static io.trino.security.AccessControlUtil.checkCanKillQueryOwnedBy;
import static io.trino.security.AccessControlUtil.checkCanViewQueryOwnedBy;
import static io.trino.security.AccessControlUtil.filterQueries;
import static io.trino.server.DataSizeSerializer.SUCCINCT_DATA_SIZE_ENABLED;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static io.trino.sql.planner.planprinter.PlanPrinter.renderDistributedPlan;
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
    private final Metadata metadata;
    private final FunctionManager functionManager;
    private final SessionPropertyManager sessionPropertyManager;

    @Inject
    public UiQueryResource(
            JsonMapper jsonMapper,
            DispatchManager dispatchManager,
            AccessControl accessControl,
            HttpRequestSessionContextFactory sessionContextFactory,
            Metadata metadata,
            FunctionManager functionManager,
            SessionPropertyManager sessionPropertyManager)
    {
        this.queryInfoCodec = buildQueryInfoCodec(jsonMapper, false);
        this.prettyQueryInfoCodec = buildQueryInfoCodec(jsonMapper, true);
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
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
        QueryInfo queryInfo = fetchViewableQueryInfo(queryId, servletRequest, httpHeaders);

        String queryString = servletRequest.getQueryString();
        if (queryString != null && queryString.contains("pretty")) {
            // Use pretty JSON codec that reduces noise
            return Response.ok(prettyQueryInfoCodec.toJson(queryInfo), APPLICATION_JSON_TYPE).build();
        }
        return Response.ok(queryInfoCodec.toJson(queryInfo), APPLICATION_JSON_TYPE).build();
    }

    @GET
    @Path("{queryId}/plan")
    public Map<StageId, List<FlattenedPlanNode>> getQueryPlan(@PathParam("queryId") QueryId queryId, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        QueryInfo queryInfo = fetchViewableQueryInfo(queryId, servletRequest, httpHeaders);
        if (queryInfo.getStages().isEmpty()) {
            return ImmutableMap.of();
        }

        // The plan is rendered on request, so that the rendered form is not retained with every tracked query
        StagesInfo stages = queryInfo.getStages().get();
        Map<PlanFragmentId, JsonRenderedNode> plan = renderDistributedPlan(
                stages,
                queryInfo.getSession().toSession(sessionPropertyManager),
                metadata,
                functionManager,
                new NoOpAnonymizer());
        return stages.getStages().stream()
                // pruned query info has no plan
                .filter(stage -> stage.plan() != null)
                .collect(toImmutableMap(StageInfo::stageId, stage -> FlattenedPlanNode.flatten(plan.get(stage.plan().getId()))));
    }

    @PUT
    @Path("{queryId}/killed")
    public Response killQuery(@PathParam("queryId") QueryId queryId, String message, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        return failQuery(queryId, user -> createKillQueryException(message, user), servletRequest, httpHeaders);
    }

    @PUT
    @Path("{queryId}/preempted")
    public Response preemptQuery(@PathParam("queryId") QueryId queryId, String message, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        return failQuery(queryId, user -> createPreemptQueryException(message, user), servletRequest, httpHeaders);
    }

    private QueryInfo fetchViewableQueryInfo(QueryId queryId, HttpServletRequest servletRequest, HttpHeaders httpHeaders)
    {
        requireNonNull(queryId, "queryId is null");

        QueryInfo queryInfo = dispatchManager.getFullQueryInfo(queryId).orElseThrow(GoneException::new);
        try {
            checkCanViewQueryOwnedBy(sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders), queryInfo.getSession().toIdentity(), accessControl);
        }
        catch (AccessDeniedException e) {
            throw new ForbiddenException();
        }
        return queryInfo;
    }

    private Response failQuery(QueryId queryId, Function<String, TrinoException> queryExceptionFactory, HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        requireNonNull(queryId, "queryId is null");

        try {
            BasicQueryInfo queryInfo = dispatchManager.getQueryInfo(queryId);

            Identity identity = sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders);
            checkCanKillQueryOwnedBy(identity, queryInfo.getSession().toIdentity(), accessControl);

            // check before killing to provide the proper error code (this is racy)
            if (queryInfo.getState().isDone()) {
                return Response.status(Status.CONFLICT).build();
            }

            dispatchManager.failQuery(queryId, queryExceptionFactory.apply(identity.getUser()));

            return Response.status(Status.ACCEPTED).build();
        }
        catch (AccessDeniedException e) {
            throw new ForbiddenException();
        }
        catch (NoSuchElementException e) {
            throw new GoneException();
        }
    }

    private JsonCodec<QueryInfo> buildQueryInfoCodec(JsonMapper jsonMapper, boolean pretty)
    {
        // Enable succinct DataSize serialization for QueryInfo to make it more human friendly
        ContextAttributes attrs = ContextAttributes.getEmpty();
        if (pretty) {
            attrs = attrs.withSharedAttribute(SUCCINCT_DATA_SIZE_ENABLED, Boolean.TRUE);
        }

        JsonMapper mapper = jsonMapper
                .rebuild()
                .defaultAttributes(attrs)
                .build();

        // Don't serialize TDigestHistogram.digest which isn't useful and human readable
        mapper.configOverride(TDigestHistogram.class).setIgnorals(forIgnoredProperties(DIGEST_PROPERTY));

        // Do not output @class property for metric types
        mapper.addMixIn(Metric.class, DropTypeInfo.class);
        // Do not output @type property for OperatorInfo
        mapper.addMixIn(OperatorInfo.class, DropTypeInfo.class);
        JsonCodecFactory jsonCodecFactory = new JsonCodecFactory(mapper);

        if (pretty) {
            jsonCodecFactory = jsonCodecFactory.prettyPrint();
        }

        return jsonCodecFactory.jsonCodec(QueryInfo.class);
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
    public interface DropTypeInfo {}
}
