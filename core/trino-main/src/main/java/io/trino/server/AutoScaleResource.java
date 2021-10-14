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
package io.trino.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.log.Logger;
import io.trino.dispatcher.DispatchManager;
import io.trino.metadata.AllNodes;
import io.trino.metadata.DiscoveryNodeManager;
import io.trino.metadata.InternalNode;
import io.trino.metadata.NodeState;
import io.trino.server.security.ResourceSecurity;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.trino.metadata.NodeState.ACTIVE;
import static io.trino.metadata.NodeState.DECOMMISSIONED;
import static io.trino.metadata.NodeState.DECOMMISSIONING;
import static io.trino.metadata.NodeState.INACTIVE;
import static io.trino.metadata.NodeState.SHUTTING_DOWN;
import static io.trino.server.security.ResourceSecurity.AccessType.PUBLIC;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;

// AutoScaleResource expose coordinator endpoints to facilitate the auto-scaling of cluster.
// These endpoints include:
//   1. /v1/autoscale/nodes --- list of all alive nodes with NodeState and NodeStatus;
//   2. /v1/autoscale/queries/{maxEndAgeSec} --- list of BasicQueryInfo of pending and recently ended queries;
//   3. /v1/autoscale/refreshnodes --- refresh with list of nodes to exclude (decommission);
//   4. /v1/autoscale/stats --- basic statistics of AutoScaleResource itself (number of calls etc)
@Path("/v1/autoscale")
public class AutoScaleResource
{
    private static Logger log = Logger.get(AutoScaleResource.class);

    private final DiscoveryNodeManager nodeManager;
    private final HttpClient httpClient;
    private final DispatchManager dispatchManager;

    // Set of worker nodes to exclude (decommission).
    Set<String> nodesToExclude = new HashSet<>();

    // Executor to periodically and asynchronously poll NodeStatus of all workers.
    private final ScheduledExecutorService nodeStatusExecutor;

    // Poll worker status once every 15 seconds, a balance between freshness and cost.
    private static final int POLL_NODESTATUS_SEC = 15;

    // Map from NodeId to RemoteNodeStatus.
    private final ConcurrentHashMap<String, RemoteNodeStatus> nodeStatuses = new ConcurrentHashMap<>();

    // Statistics maintained and exposed in AutoScaleStats.
    private final long startTime = System.currentTimeMillis();
    private int numRefreshNodes;
    private int numUpdateStateOk;
    private int numUpdateStateFailed;
    private int numGetBasicQueryInfos;

    @Inject
    public AutoScaleResource(DiscoveryNodeManager nodeManager,
            @ForAutoScale HttpClient httpClient,
            DispatchManager dispatchManager)
    {
        log.info("Construct AutoScaleResource");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = httpClient;
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.nodeStatusExecutor = newSingleThreadScheduledExecutor(threadsNamed("autoscale-executor-%s"));
    }

    @PostConstruct
    public void startPollingNodeStatus()
    {
        nodeStatusExecutor.scheduleWithFixedDelay(() -> {
            try {
                pollWorkers();
            }
            catch (Exception e) {
                log.error(e, "Error polling state of nodes");
            }
        }, 5, POLL_NODESTATUS_SEC, TimeUnit.SECONDS);
        pollWorkers();
    }

    // Poll /v1/status of all alive workers.
    private void pollWorkers()
    {
        AllNodes allNodes = nodeManager.getAllNodes();

        Set<InternalNode> aliveNodes = allNodes.getAliveNodes();

        ImmutableSet<String> aliveNodeIds = aliveNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        Set<String> deadNodes = difference(nodeStatuses.keySet(), aliveNodeIds).immutableCopy();
        nodeStatuses.keySet().removeAll(deadNodes);

        // Add new nodes
        for (InternalNode node : aliveNodes) {
            URI statusUri = uriBuilderFrom(node.getInternalUri()).appendPath("/v1/status").build();
            nodeStatuses.putIfAbsent(node.getNodeIdentifier(), new RemoteNodeStatus(httpClient, statusUri));
        }

        // Schedule refresh
        nodeStatuses.values().forEach(RemoteNodeStatus::asyncRefresh);
    }

    @PreDestroy
    public void stop()
    {
        nodeStatusExecutor.shutdownNow();
    }

    // Get basic statistics of AutoScaleResource itself.
    @ResourceSecurity(PUBLIC)
    @GET
    @Path("stats")
    @Produces(APPLICATION_JSON)
    public AutoScaleStats getAutoScaleStats()
    {
        int[] remoteCounts = getRemoteNodeStatusCount();
        return new AutoScaleStats(startTime, numRefreshNodes,
                numUpdateStateOk, numUpdateStateFailed,
                remoteCounts[0], remoteCounts[1], numGetBasicQueryInfos);
    }

    // Get [#success, #failure] among the latest polls of all RemoteNodeStatus.
    private int[] getRemoteNodeStatusCount()
    {
        int[] counts = new int[2];
        for (RemoteNodeStatus r : nodeStatuses.values()) {
            if (r.isLastUpdateSuccess()) {
                counts[0]++;
            }
            else {
                counts[1]++;
            }
        }
        return counts;
    }

    // AutoScaleStats models the response to v1/autoscale/stats
    public static class AutoScaleStats
    {
        // The start time in epoch of AutoScaleResource
        private final long startTime;

        // Number of times refreshNodes() was called since start
        private final int numRefreshNodes;

        // Total number of success in update worker state
        private final int numUpdateStateOk;

        // Total number of failure in update worker state
        private final int numUpdateStateFailed;

        // Number of success in latest refresh of RemoteNodeStatus
        private final int numRemoteNodeStatusOk;

        // Number of failures in latest refresh of RemoteNodeStatus
        private final int numRemoteNodeStatusFailed;

        // Number of times getBasicQueryInfos is called
        private final int numGetBasicQueryInfos;

        @JsonCreator
        public AutoScaleStats(
                @JsonProperty("startTime") long startTime,
                @JsonProperty("numRefreshNodes") int numRefreshNodes,
                @JsonProperty("numUpdateStateOk") int numUpdateStateOk,
                @JsonProperty("numUpdateStateFailed") int numUpdateStateFailed,
                @JsonProperty("numRemoteNodeStatusOk") int numRemoteNodeStatusOk,
                @JsonProperty("numRemoteNodeStatusFailed") int numRemoteNodeStatusFailed,
                @JsonProperty("numGetBasicQueryInfos") int numGetBasicQueryInfos)
        {
            this.startTime = startTime;
            this.numRefreshNodes = numRefreshNodes;
            this.numUpdateStateOk = numUpdateStateOk;
            this.numUpdateStateFailed = numUpdateStateFailed;
            this.numRemoteNodeStatusOk = numRemoteNodeStatusOk;
            this.numRemoteNodeStatusFailed = numRemoteNodeStatusFailed;
            this.numGetBasicQueryInfos = numGetBasicQueryInfos;
        }

        @JsonProperty
        public long getStartTime()
        {
            return startTime;
        }

        @JsonProperty
        public long getNumRefreshNodes()
        {
            return numRefreshNodes;
        }

        @JsonProperty
        public long getNumUpdateStateOk()
        {
            return numUpdateStateOk;
        }

        @JsonProperty
        public long getNumUpdateStateFailed()
        {
            return numUpdateStateFailed;
        }

        @JsonProperty
        public long getNumRemoteNodeStatusOk()
        {
            return numRemoteNodeStatusOk;
        }

        @JsonProperty
        public long getNumRemoteNodeStatusFailed()
        {
            return numRemoteNodeStatusFailed;
        }

        @JsonProperty
        public long getNumGetBasicQueryInfos()
        {
            return numGetBasicQueryInfos;
        }
    }

    // Gets list of all nodes where each is modeled as AutoScaleNode.
    @ResourceSecurity(PUBLIC)
    @GET
    @Path("nodes")
    @Produces(APPLICATION_JSON)
    public List<AutoScaleNode> getAutoScaleNodes()
    {
        final AllNodes nodes = nodeManager.getAllNodes();
        TreeMap<String, AutoScaleNode> asmp = new TreeMap<>();
        addToAsnMap(nodes.getActiveNodes(), ACTIVE, asmp);
        addToAsnMap(nodes.getShuttingDownNodes(), SHUTTING_DOWN, asmp);
        addToAsnMap(nodes.getInactiveNodes(), INACTIVE, asmp);
        addToAsnMap(nodes.getDecommissioningNodes(), DECOMMISSIONING, asmp);
        addToAsnMap(nodes.getDecommissionedNodes(), DECOMMISSIONED, asmp);
        return new ArrayList<>(asmp.values());
    }

    // AutoScaleNode is a bundle of (InternalNode, NodeState, NodeStatus)
    // where NodeStatus is polled from v1/status of the node.
    public static class AutoScaleNode
    {
        private final String nodeId;
        private final String uri;
        private final boolean coordinator;
        private final NodeState state;
        private final NodeStatus status;
        private final long statusTime;

        @JsonCreator
        public AutoScaleNode(
                @JsonProperty("nodeId") String nodeId,
                @JsonProperty("uri") String uri,
                @JsonProperty("coordinator") boolean coordinator,
                @JsonProperty("state") NodeState state,
                @JsonProperty("status") NodeStatus status,
                @JsonProperty("statusTime") long statusTime)
        {
            this.nodeId = requireNonNull(nodeId, "nodeId is null");
            this.uri = uri;
            this.coordinator = coordinator;
            this.state = state;
            this.status = status;
            this.statusTime = statusTime;
        }

        @JsonProperty
        public String getNodeId()
        {
            return nodeId;
        }

        @JsonProperty
        public String getUri()
        {
            return uri;
        }

        @JsonProperty
        public boolean getCoordinator()
        {
            return coordinator;
        }

        @JsonProperty
        public NodeState getState()
        {
            return state;
        }

        @JsonProperty
        public NodeStatus getStatus()
        {
            return status;
        }

        @JsonProperty
        public long getStatusTime()
        {
            return statusTime;
        }
    }

    // Get BasicQueryInfo of all pending and recently ended queries.
    // Here recently is defined as ended on or after maxEndAgeSec seconds ago.
    @ResourceSecurity(PUBLIC)
    @GET
    @Path("queries/{maxEndAgeSec}")
    @Produces(APPLICATION_JSON)
    public List<BasicQueryInfo> getBasicQueryInfos(
            @PathParam("maxEndAgeSec") int maxEndAgeSec)
    {
        numGetBasicQueryInfos++;
        // Return queries not ended or ended within maxEndAgeSec seconds.
        if (maxEndAgeSec < 0) {
            return dispatchManager.getQueries();
        }
        else {
            long endCutoff = System.currentTimeMillis() - 1000L * maxEndAgeSec;
            return dispatchManager.getQueries().stream()
                    .filter(v -> v.getQueryStats() == null
                        || v.getQueryStats().getEndTime() == null
                        || v.getQueryStats().getEndTime().getMillis() >= endCutoff)
                    .collect(toImmutableList());
        }
    }

    // Given an absolute list of nodes to exclude (a.k.a. decommission), which means:
    //   1. The desired state for worker appear in the exclude list is DECOMMISSIONED;
    //   2. The desired state for worker that does not appear in the exclude list is ACTIVE;
    // Initiate decommission/recommission actions as appropriate to have all worker nodes
    // move toward their desired state. Specifically:
    //   1. A worker to exclude will be honored within seconds with no new task dispatch.
    //      asyncUpdateState will be called for the worker to wait for pending tasks and
    //      later report as DECOMMISSIONED upon completion.
    //   3. A worker that was previously excluded but no longer will qualify within seconds
    //      for new task dispatch. asyncUpdateState will be called for the worker to be
    //      back to ACTIVE.
    @ResourceSecurity(PUBLIC)
    @PUT
    @Path("refreshnodes")
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
    public Response refreshNodes(List<String> exclude)
    {
        numRefreshNodes++;
        log.info(numRefreshNodes + " refreshNodes " + Joiner.on(',').join(exclude));

        TreeMap<String, AutoScaleNode> asnm = getId2AutoScaleNodeMap();
        // Assume nodesToExclude are comma separated list of nodeIds
        Set<String> nodesToExclude = parseNodesToExclude(exclude, asnm.keySet());
        if (!nodesToExclude.equals(this.nodesToExclude)) {
            this.nodesToExclude = nodesToExclude;
            nodeManager.setNodesToExclude(nodesToExclude);
        }

        for (AutoScaleNode node : asnm.values()) {
            if (node.coordinator) {
                continue;
            }
            // Decommission ACTIVE nodes that appear in nodesToExclude.
            // Note that for now we update state during each refresh:
            //   1. worker handle decommission efficiently if it is already DN or DD state.
            //   2. we didn't track whether the previous update was successful
            //   3. ensure DECOMMISSIONING state on worker just in case.
            if (nodesToExclude.contains(node.nodeId)) {
                asyncUpdateState(node, DECOMMISSIONING);
            }

            // Recommission DN/DD nodes that do not appear in nodesToExclude
            if ((node.state == DECOMMISSIONING || node.state == DECOMMISSIONED)
                    && !nodesToExclude.contains(node.nodeId)) {
                asyncUpdateState(node, ACTIVE);
            }
        }

        return Response.ok().type(TEXT_PLAIN)
                .entity(String.format("refreshNodes [%s] OK", Joiner.on(',').join(exclude)))
                .build();
    }

    // Parse given list of node to exclude into a set and log unknown ones.
    private static Set<String> parseNodesToExclude(List<String> exclude, Set<String> nodes)
    {
        ImmutableSet.Builder<String> nodesToExclude = ImmutableSet.builder();
        for (String node : exclude) {
            if (!nodes.contains(node)) {
                log.info("parseNodesToExclude unknown node " + node);
            }
            nodesToExclude.add(node);
        }
        return nodesToExclude.build();
    }

    // Get map from nodeId to AutoScaleNode for all nodes.
    private TreeMap<String, AutoScaleNode> getId2AutoScaleNodeMap()
    {
        final AllNodes nodes = nodeManager.getAllNodes();
        TreeMap<String, AutoScaleNode> asnm = new TreeMap<>();
        addToAsnMap(nodes.getActiveNodes(), ACTIVE, asnm);
        addToAsnMap(nodes.getShuttingDownNodes(), SHUTTING_DOWN, asnm);
        addToAsnMap(nodes.getInactiveNodes(), INACTIVE, asnm);
        addToAsnMap(nodes.getDecommissioningNodes(), DECOMMISSIONING, asnm);
        addToAsnMap(nodes.getDecommissionedNodes(), DECOMMISSIONED, asnm);
        return asnm;
    }

    // Add all nodes with a specific NodeState into nmap.
    private void addToAsnMap(
            Set<InternalNode> nodes, NodeState state, TreeMap<String, AutoScaleNode> nmap)
    {
        for (InternalNode node : nodes) {
            String nodeId = node.getNodeIdentifier();
            String uri = node.getInternalUri().toString();
            RemoteNodeStatus rns = nodeStatuses.get(nodeId);
            NodeStatus status = rns != null && rns.getNodeStatus().isPresent()
                    ? rns.getNodeStatus().get() : null;
            nmap.put(nodeId, new AutoScaleNode(
                    nodeId, uri, node.isCoordinator(), state, status,
                    rns == null ? 0 : rns.getLastUpdateTime()));
        }
    }

    // Asynchronously update state of a specific worker, basically execute HTTP put
    // request against /v1/info/state endpoint on the remote worker.
    private synchronized void asyncUpdateState(AutoScaleNode node, NodeState state)
    {
        log.info(String.format("asyncUpdateState %s %s", node.nodeId, state));
        Request request = getUpdateStateRequest(node, state);
        HttpResponseFuture<StatusResponse> responseFuture = httpClient.executeAsync(
                request, StatusResponseHandler.createStatusResponseHandler());

        Futures.addCallback(responseFuture, new FutureCallback<StatusResponse>()
        {
            @Override
            public void onSuccess(@Nullable StatusResponse result)
            {
                numUpdateStateOk++;
                log.info(String.format("OK async updated %s %s", request.getUri(), state));
            }

            @Override
            public void onFailure(Throwable t)
            {
                numUpdateStateFailed++;
                log.info(String.format("Error async updated %s %s %s",
                        request.getUri(), state, t.getMessage()));
            }
        }, directExecutor());
    }

    private synchronized Request getUpdateStateRequest(AutoScaleNode node, NodeState state)
    {
        // http://10.43.31.106:8081 -> http://10.43.31.106:8081/v1/info/state
        URI infoStateUri = uriBuilderFrom(getUri(node.getUri())).appendPath("/v1/info/state").build();

        // Note that the quote in "<state>" is needed as otherwise
        // Unrecognized token 'DECOMMISSION': was expecting ('true', 'false' or 'null')
        BodyGenerator bodyGenerator = StaticBodyGenerator.createStaticBodyGenerator(
                "\"" + state + "\"", Charset.defaultCharset());
        return preparePut()
                .setUri(infoStateUri)
                .setHeader(CONTENT_TYPE, "application/json")
                .setBodyGenerator(bodyGenerator)
                .build();
    }

    private static URI getUri(String uri)
    {
        try {
            return new URI(uri);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
