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
package io.prestosql.server.ui;

import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryState;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.GroupProvider;

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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.io.ByteStreams.toByteArray;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.prestosql.connector.system.KillQueryProcedure.createKillQueryException;
import static io.prestosql.connector.system.KillQueryProcedure.createPreemptQueryException;
import static io.prestosql.security.AccessControlUtil.checkCanKillQueryOwnedBy;
import static io.prestosql.security.AccessControlUtil.checkCanViewQueryOwnedBy;
import static io.prestosql.security.AccessControlUtil.filterQueries;
import static io.prestosql.server.HttpRequestSessionContext.extractAuthorizedIdentity;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;

@Path("/ui/api/query")
public class UiQueryResource
{
    private static final Logger logger = Logger.get(UiQueryResource.class);
    private static final File CONFIG_FILE = new File("etc/event-listener.properties");
    private static final String eventLogUri = "/ui/api/eventlog";

    private final DispatchManager dispatchManager;
    private final AccessControl accessControl;
    private final GroupProvider groupProvider;
    private final HttpClient httpClient;
    private final JsonCodec<QueryInfo> codec;

    @Inject
    public UiQueryResource(DispatchManager dispatchManager,
            AccessControl accessControl,
            GroupProvider groupProvider,
            @ForWebUi HttpClient httpClient,
            JsonCodec<QueryInfo> codec)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.groupProvider = requireNonNull(groupProvider, "groupProvider is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.codec = requireNonNull(codec, "queryInfoCodec is null");
    }

    @GET
    public List<BasicQueryInfo> getAllQueryInfo(@QueryParam("state") String stateFilter, @Context HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        QueryState expectedState = stateFilter == null ? null : QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));

        List<BasicQueryInfo> queries = dispatchManager.getQueries();
        queries = filterQueries(extractAuthorizedIdentity(servletRequest, httpHeaders, accessControl, groupProvider), queries, accessControl);

        ImmutableList.Builder<BasicQueryInfo> builder = new ImmutableList.Builder<>();
        for (BasicQueryInfo queryInfo : queries) {
            if (stateFilter == null || queryInfo.getState() == expectedState) {
                builder.add(queryInfo);
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

        if (!queryInfo.isPresent() ||
                !queryInfo.get().getOutputStage().isPresent() ||
                queryInfo.get().getOutputStage().get().getPlan() == null) {
            try {
                QueryInfo queryInfoFromEventLog = redirectToEventLogApi(queryId);
                queryInfo = queryInfoFromEventLog == null ? Optional.empty() : Optional.of(queryInfoFromEventLog);
            }
            catch (Exception e) {
                logger.error(e, e.getMessage());
            }
        }

        if (queryInfo.isPresent()) {
            try {
                checkCanViewQueryOwnedBy(extractAuthorizedIdentity(servletRequest, httpHeaders, accessControl, groupProvider), queryInfo.get().getSession().getUser(), accessControl);
                return Response.ok(queryInfo.get()).build();
            }
            catch (AccessDeniedException e) {
                throw new ForbiddenException();
            }
        }
        return Response.status(Status.GONE).build();
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

    private Response failQuery(QueryId queryId, PrestoException queryException, HttpServletRequest servletRequest, @Context HttpHeaders httpHeaders)
    {
        requireNonNull(queryId, "queryId is null");

        try {
            BasicQueryInfo queryInfo = dispatchManager.getQueryInfo(queryId);

            checkCanKillQueryOwnedBy(extractAuthorizedIdentity(servletRequest, httpHeaders, accessControl, groupProvider), queryInfo.getSession().getUser(), accessControl);

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

    private QueryInfo redirectToEventLogApi(QueryId queryId) throws Exception
    {
        Map<String, String> properties = loadEventListenerProperties(CONFIG_FILE.getAbsoluteFile());
        String address = properties.getOrDefault("node.internal-address", "localhost");
        String port = properties.get("http-server.http.port");
        if (isNullOrEmpty(port)) {
            throw new Exception("Missing node.internal-address in etc/event-listener.properties");
        }
        String redirectUri = String.format("http://%s:%s%s", address, port, eventLogUri);
        URI uri = uriBuilderFrom(new URI(redirectUri)).appendPath(queryId.getId()).build();
        Request request = prepareGet()
                .setUri(uri)
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .build();
        QueryInfo queryInfoFromEventLog = httpClient.execute(request, new ResponseHandler<QueryInfo, Exception>()
        {
            @Override
            public QueryInfo handleException(Request request, Exception exception)
                    throws Exception
            {
                return null;
            }

            @Override
            public QueryInfo handle(Request request, io.airlift.http.client.Response response)
                    throws Exception
            {
                if (response.getStatusCode() == Response.Status.GONE.getStatusCode()) {
                    return null;
                }
                byte[] bytes = toByteArray(response.getInputStream());
                return codec.fromJson(bytes);
            }
        });
        return queryInfoFromEventLog;
    }

    private Map<String, String> loadEventListenerProperties(File configFile)
    {
        try {
            return new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read configuration file: " + configFile, e);
        }
    }
}
