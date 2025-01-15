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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.TaskId;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeState;
import io.trino.security.AccessControl;
import io.trino.server.ForWorkerInfo;
import io.trino.server.GoneException;
import io.trino.server.HttpRequestSessionContextFactory;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.Node;
import io.trino.spi.QueryId;
import io.trino.spi.security.AccessDeniedException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.INACTIVE;
import static io.trino.security.AccessControlUtil.checkCanViewQueryOwnedBy;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON;
import static jakarta.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static java.util.Objects.requireNonNull;

@Path("/ui/api/worker")
@ResourceSecurity(WEB_UI)
public class WorkerResource
{
    private final DispatchManager dispatchManager;
    private final InternalNodeManager nodeManager;
    private final AccessControl accessControl;
    private final HttpClient httpClient;
    private final HttpRequestSessionContextFactory sessionContextFactory;

    @Inject
    public WorkerResource(
            DispatchManager dispatchManager,
            InternalNodeManager nodeManager,
            AccessControl accessControl,
            @ForWorkerInfo HttpClient httpClient,
            HttpRequestSessionContextFactory sessionContextFactory)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.sessionContextFactory = requireNonNull(sessionContextFactory, "sessionContextFactory is null");
    }

    @GET
    @Path("{nodeId}/status")
    public Response getStatus(@PathParam("nodeId") String nodeId)
    {
        return proxyJsonResponse(nodeId, "v1/status");
    }

    @GET
    @Path("{nodeId}/thread")
    public Response getThreads(@PathParam("nodeId") String nodeId)
    {
        return proxyJsonResponse(nodeId, "v1/thread");
    }

    @GET
    @Path("{nodeId}/task/{taskId}")
    public Response getThreads(
            @PathParam("taskId") TaskId task,
            @PathParam("nodeId") String nodeId,
            @Context HttpServletRequest servletRequest,
            @Context HttpHeaders httpHeaders)
    {
        QueryId queryId = task.getQueryId();
        Optional<QueryInfo> queryInfo = dispatchManager.getFullQueryInfo(queryId);
        if (queryInfo.isPresent()) {
            try {
                checkCanViewQueryOwnedBy(sessionContextFactory.extractAuthorizedIdentity(servletRequest, httpHeaders), queryInfo.get().getSession().toIdentity(), accessControl);
                return proxyJsonResponse(nodeId, "v1/task/" + task);
            }
            catch (AccessDeniedException e) {
                throw new ForbiddenException();
            }
        }
        throw new GoneException();
    }

    @GET
    public Response getWorkerList()
    {
        Set<InternalNode> activeNodes = nodeManager.getAllNodes().getActiveNodes();
        Set<InternalNode> inactiveNodes = nodeManager.getAllNodes().getInactiveNodes();
        Set<JsonNodeInfo> jsonNodes = new HashSet<>();
        for (Node node : activeNodes) {
            JsonNodeInfo jsonNode = new JsonNodeInfo(node.getNodeIdentifier(), node.getHostAndPort().getHostText(), node.getVersion(), node.isCoordinator(), ACTIVE.toString().toLowerCase(Locale.ENGLISH));
            jsonNodes.add(jsonNode);
        }
        for (Node node : inactiveNodes) {
            JsonNodeInfo jsonNode = new JsonNodeInfo(node.getNodeIdentifier(), node.getHostAndPort().getHostText(), node.getVersion(), node.isCoordinator(), INACTIVE.toString().toLowerCase(Locale.ENGLISH));
            jsonNodes.add(jsonNode);
        }
        return Response.ok().entity(jsonNodes).build();
    }

    public static class JsonNodeInfo
    {
        private final String nodeId;
        private final String nodeIp;
        private final String nodeVersion;
        private final boolean coordinator;
        private final String state;

        @JsonCreator
        public JsonNodeInfo(@JsonProperty("nodeId") String nodeId,
                @JsonProperty("nodeIp") String nodeIp,
                @JsonProperty("nodeVersion") String nodeVersion,
                @JsonProperty("coordinator") boolean coordinator,
                @JsonProperty("state") String state)
        {
            this.nodeId = requireNonNull(nodeId, "nodeId is null");
            this.nodeIp = requireNonNull(nodeIp, "nodeIp is null");
            this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
            this.coordinator = coordinator;
            this.state = requireNonNull(state, "state is null");
        }

        @JsonProperty
        public String getNodeId()
        {
            return nodeId;
        }

        @JsonProperty
        public String getNodeIp()
        {
            return nodeIp;
        }

        @JsonProperty
        public String getNodeVersion()
        {
            return nodeVersion;
        }

        @JsonProperty
        public boolean getCoordinator()
        {
            return coordinator;
        }

        @JsonProperty
        public String getState()
        {
            return state;
        }
    }

    private Response proxyJsonResponse(String nodeId, String workerPath)
    {
        Set<InternalNode> nodes = nodeManager.getNodes(NodeState.ACTIVE);
        InternalNode node = nodes.stream()
                .filter(n -> n.getNodeIdentifier().equals(nodeId))
                .findFirst()
                .orElseThrow(NotFoundException::new);

        Request request = prepareGet()
                .setUri(uriBuilderFrom(node.getInternalUri())
                        .appendPath(workerPath)
                        .build())
                .build();

        byte[] responseStream = httpClient.execute(request, new StreamingJsonResponseHandler());
        return Response.ok(responseStream, APPLICATION_JSON_TYPE).build();
    }

    private static class StreamingJsonResponseHandler
            implements ResponseHandler<byte[], RuntimeException>
    {
        @Override
        public byte[] handleException(Request request, Exception exception)
        {
            throw new RuntimeException("Request to worker failed", exception);
        }

        @Override
        public byte[] handle(Request request, io.airlift.http.client.Response response)
        {
            try {
                if (!APPLICATION_JSON.equals(response.getHeader(CONTENT_TYPE))) {
                    throw new RuntimeException("Response received was not of type " + APPLICATION_JSON);
                }
                return response.getInputStream().readAllBytes();
            }
            catch (IOException e) {
                throw new RuntimeException("Unable to read response from worker", e);
            }
        }
    }
}
