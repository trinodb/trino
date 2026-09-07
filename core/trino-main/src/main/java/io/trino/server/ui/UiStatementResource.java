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

import com.google.inject.Inject;
import io.trino.client.QueryResults;
import io.trino.dispatcher.QueuedStatementResource;
import io.trino.server.ExternalUriInfo;
import io.trino.server.protocol.ExecutingStatementResource;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.List;

import static io.trino.client.ProtocolHeaders.TRINO_HEADERS;
import static io.trino.server.ServletSecurityUtils.authenticatedIdentity;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static java.util.Objects.requireNonNull;

@Path("/ui/api/statement")
@ResourceSecurity(WEB_UI)
@Produces(APPLICATION_JSON)
public class UiStatementResource
{
    private static final String UI_REQUEST_HEADER = "X-Trino-UI-Request";

    private final QueuedStatementResource queuedStatementResource;
    private final ExecutingStatementResource executingStatementResource;

    @Inject
    public UiStatementResource(QueuedStatementResource queuedStatementResource, ExecutingStatementResource executingStatementResource)
    {
        this.queuedStatementResource = requireNonNull(queuedStatementResource, "queuedStatementResource is null");
        this.executingStatementResource = requireNonNull(executingStatementResource, "executingStatementResource is null");
    }

    @POST
    public Response postStatement(
            String statement,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders headers,
            @BeanParam ExternalUriInfo externalUriInfo)
    {
        String user = checkRequest(servletRequest, headers);
        checkUserHeaders(headers, user);
        // Keep the X-Trino-UI-Request header so protocol detection rejects configured alternate headers.
        return queuedStatementResource.postStatement(statement, servletRequest, headers, externalUriInfo);
    }

    @GET
    @Path("queued/{queryId}/{slug}/{token}")
    public void getQueuedResults(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders headers,
            @BeanParam ExternalUriInfo externalUriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        String user = checkRequest(servletRequest, headers);
        queuedStatementResource.checkQueryOwner(queryId, slug, token, user);
        queuedStatementResource.getStatus(queryId, slug, token, externalUriInfo, asyncResponse);
    }

    @GET
    @Path("executing/{queryId}/{slug}/{token}")
    public void getExecutingResults(
            @PathParam("queryId") QueryId queryId,
            @PathParam("slug") String slug,
            @PathParam("token") long token,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders headers,
            @BeanParam ExternalUriInfo externalUriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        String user = checkRequest(servletRequest, headers);
        executingStatementResource.checkQueryOwner(queryId, slug, token, user);
        executingStatementResource.getQueryResults(queryId, slug, token, externalUriInfo, asyncResponse);
    }

    private static String checkRequest(HttpServletRequest request, HttpHeaders headers)
    {
        if (!List.of("true").equals(headers.getRequestHeader(UI_REQUEST_HEADER))) {
            throw new ForbiddenException("Missing or invalid " + UI_REQUEST_HEADER);
        }
        return authenticatedIdentity(request).orElseThrow(ForbiddenException::new).getUser();
    }

    private static void checkUserHeaders(HttpHeaders headers, String user)
    {
        for (String header : List.of(TRINO_HEADERS.requestUser(), TRINO_HEADERS.requestOriginalUser())) {
            List<String> values = headers.getRequestHeader(header);
            if (values != null && values.stream().anyMatch(value -> !value.trim().equals(user))) {
                throw new ForbiddenException("Statement user must match the authenticated Web UI user");
            }
        }
    }

    // Adapt only the outgoing response: executing queries cache their canonical QueryResults.
    public static class ResponseFilter
            implements ContainerResponseFilter
    {
        @Context
        private ResourceInfo resourceInfo;

        @Override
        public void filter(ContainerRequestContext request, ContainerResponseContext response)
        {
            if (resourceInfo.getResourceClass() == UiStatementResource.class && response.getEntity() instanceof QueryResults results) {
                String statementPath = ExternalUriInfo.from(request).absolutePath("/v1/statement").getRawPath();
                response.setEntity(withUiNextUri(results, statementPath));
            }
        }
    }

    static QueryResults withUiNextUri(QueryResults results, String statementPath)
    {
        URI nextUri = results.getNextUri();
        if (nextUri == null) {
            return results;
        }
        String path = nextUri.getRawPath();
        if (!path.startsWith(statementPath + "/")) {
            throw new IllegalArgumentException("Unexpected statement continuation path: " + path);
        }
        String uiPath = statementPath.substring(0, statementPath.length() - "/v1/statement".length()) + "/ui/api/statement";
        URI uiNextUri = UriBuilder.fromUri(nextUri).replacePath(uiPath + path.substring(statementPath.length())).build();
        return new QueryResults(
                results.getId(),
                results.getInfoUri(),
                results.getPartialCancelUri(),
                uiNextUri,
                results.getColumns(),
                results.getRawData(),
                results.getStats(),
                results.getError(),
                results.getWarnings(),
                results.getUpdateType(),
                results.getUpdateCount());
    }
}
