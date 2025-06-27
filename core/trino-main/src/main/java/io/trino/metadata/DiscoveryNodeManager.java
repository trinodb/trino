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
package io.trino.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.http.client.HttpClient;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.trino.client.NodeVersion;
import io.trino.failuredetector.FailureDetector;
import io.trino.server.InternalCommunicationConfig;
import io.trino.server.NodeStateManager.CurrentNodeState;
import io.trino.server.ServerConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

@ThreadSafe
public final class DiscoveryNodeManager
        implements InternalNodeManager
{
    private static final Logger log = Logger.get(DiscoveryNodeManager.class);

    private final Supplier<NodeState> currentNodeState;
    private final ServiceSelector serviceSelector;
    private final FailureDetector failureDetector;
    private final NodeVersion expectedNodeVersion;
    private final ConcurrentHashMap<URI, RemoteNodeState> nodeStates = new ConcurrentHashMap<>();
    private final HttpClient httpClient;
    private final ScheduledExecutorService nodeStateUpdateExecutor;
    private final ExecutorService nodeStateEventExecutor;
    private final boolean httpsRequired;
    private final InternalNode currentNode;
    private final Ticker ticker;

    @GuardedBy("this")
    private AllNodes allNodes;

    @GuardedBy("this")
    private Set<InternalNode> coordinators;

    @GuardedBy("this")
    private final List<Consumer<AllNodes>> listeners = new ArrayList<>();

    @Inject
    public DiscoveryNodeManager(
            @ServiceType("trino") ServiceSelector serviceSelector,
            NodeInfo nodeInfo,
            HttpServerInfo httpServerInfo,
            CurrentNodeState currentNodeState,
            FailureDetector failureDetector,
            NodeVersion expectedNodeVersion,
            @ForNodeManager HttpClient httpClient,
            ServerConfig serverConfig,
            InternalCommunicationConfig internalCommunicationConfig)
    {
        this(
                serviceSelector,
                new InternalNode(
                                nodeInfo.getNodeId(),
                                internalCommunicationConfig.isHttpsRequired() ? httpServerInfo.getHttpsUri() : httpServerInfo.getHttpUri(),
                                expectedNodeVersion,
                                serverConfig.isCoordinator()),
                currentNodeState,
                failureDetector,
                expectedNodeVersion,
                httpClient,
                internalCommunicationConfig.isHttpsRequired(),
                Ticker.systemTicker());
    }

    @VisibleForTesting
    DiscoveryNodeManager(
            ServiceSelector serviceSelector,
            InternalNode currentNode,
            Supplier<NodeState> currentNodeState,
            FailureDetector failureDetector,
            NodeVersion expectedNodeVersion,
            HttpClient httpClient,
            boolean httpsRequired,
            Ticker ticker)
    {
        this.serviceSelector = requireNonNull(serviceSelector, "serviceSelector is null");
        this.currentNode = requireNonNull(currentNode, "currentNode is null");
        this.currentNodeState = requireNonNull(currentNodeState, "currentNodeState is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.expectedNodeVersion = requireNonNull(expectedNodeVersion, "expectedNodeVersion is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeStateUpdateExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("node-state-poller-%s"));
        this.nodeStateEventExecutor = newCachedThreadPool(daemonThreadsNamed("node-state-events-%s"));
        this.httpsRequired = httpsRequired;
        this.ticker = requireNonNull(ticker, "ticker is null");

        refreshNodes();
    }

    @PostConstruct
    public void startPollingNodeStates()
    {
        nodeStateUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                refreshNodes();
            }
            catch (Exception e) {
                log.error(e, "Error polling state of nodes");
            }
        }, 5, 5, TimeUnit.SECONDS);
        refreshNodes();
    }

    @PreDestroy
    public void destroy()
    {
        nodeStateUpdateExecutor.shutdown();
        nodeStateEventExecutor.shutdown();
    }

    @PreDestroy
    public void stop()
    {
        nodeStateUpdateExecutor.shutdownNow();
    }

    @Override
    public void refreshNodes()
    {
        // This is a deny-list.
        Set<ServiceDescriptor> failed = failureDetector.getFailed();
        Set<ServiceDescriptor> services = serviceSelector.selectAllServices().stream()
                .filter(service -> !failed.contains(service))
                .collect(toImmutableSet());

        // Add new nodes
        for (ServiceDescriptor service : services) {
            URI uri = getHttpUri(service, httpsRequired);
            if (uri == null) {
                continue;
            }
            NodeVersion nodeVersion = getNodeVersion(service);
            if (!expectedNodeVersion.equals(nodeVersion)) {
                // version is currently invalid so, remove this node if it had been previously tracked
                nodeStates.remove(uri);
                continue;
            }

            // Mark the node as seen, and get the current state
            RemoteNodeState remoteNodeState = nodeStates.computeIfAbsent(
                    uri,
                    _ -> new RemoteNodeState(
                            new InternalNode(service.getNodeId(), uri, nodeVersion, isCoordinator(service)),
                            httpClient,
                            ticker));
            remoteNodeState.setSeen();
        }

        // Remove nodes that are no longer present
        for (var entry : nodeStates.entrySet()) {
            RemoteNodeState remoteNodeState = entry.getValue();
            if (remoteNodeState.isMissing()) {
                if (remoteNodeState.hasBeenActive() && remoteNodeState.getNodeState() != NodeState.SHUTTING_DOWN) {
                    InternalNode node = remoteNodeState.getNode();
                    log.info("Previously active node is missing: %s (last seen at %s)", node.getNodeIdentifier(), node.getHost());
                }
                nodeStates.remove(entry.getKey());
            }
        }

        // Schedule refresh
        nodeStates.values().forEach(RemoteNodeState::asyncRefresh);

        // update indexes
        refreshNodesInternal();
    }

    private synchronized void refreshNodesInternal()
    {
        ImmutableSet.Builder<InternalNode> activeNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> inactiveNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> drainingNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> drainedNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> shuttingDownNodesBuilder = ImmutableSet.builder();

        switch (currentNodeState.get()) {
            case ACTIVE -> activeNodesBuilder.add(currentNode);
            case INACTIVE -> inactiveNodesBuilder.add(currentNode);
            case DRAINING -> drainingNodesBuilder.add(currentNode);
            case DRAINED -> drainedNodesBuilder.add(currentNode);
            case SHUTTING_DOWN -> shuttingDownNodesBuilder.add(currentNode);
        }

        for (RemoteNodeState remoteNodeState : nodeStates.values()) {
            InternalNode node = remoteNodeState.getNode();
            NodeState nodeState = remoteNodeState.getNodeState();
            switch (nodeState) {
                case ACTIVE -> activeNodesBuilder.add(node);
                case INACTIVE -> inactiveNodesBuilder.add(node);
                case DRAINING -> drainingNodesBuilder.add(node);
                case DRAINED -> drainedNodesBuilder.add(node);
                case SHUTTING_DOWN -> shuttingDownNodesBuilder.add(node);
            }
        }

        Set<InternalNode> activeNodes = activeNodesBuilder.build();
        Set<InternalNode> drainingNodes = drainingNodesBuilder.build();
        Set<InternalNode> drainedNodes = drainedNodesBuilder.build();
        Set<InternalNode> inactiveNodes = inactiveNodesBuilder.build();
        Set<InternalNode> shuttingDownNodes = shuttingDownNodesBuilder.build();

        Set<InternalNode> coordinators = activeNodes.stream()
                .filter(InternalNode::isCoordinator)
                .collect(toImmutableSet());

        AllNodes allNodes = new AllNodes(activeNodes, inactiveNodes, drainingNodes, drainedNodes, shuttingDownNodes, coordinators);
        // only update if all nodes actually changed (note: this does not include the connectors registered with the nodes)
        if (!allNodes.equals(this.allNodes)) {
            // assign allNodes to a local variable for use in the callback below
            this.allNodes = allNodes;
            this.coordinators = coordinators;

            // notify listeners
            List<Consumer<AllNodes>> listeners = ImmutableList.copyOf(this.listeners);
            nodeStateEventExecutor.submit(() -> listeners.forEach(listener -> listener.accept(allNodes)));
        }
    }

    @Override
    public synchronized AllNodes getAllNodes()
    {
        return allNodes;
    }

    @Managed
    public int getActiveNodeCount()
    {
        return getAllNodes().getActiveNodes().size();
    }

    @Managed
    public int getInactiveNodeCount()
    {
        return getAllNodes().getInactiveNodes().size();
    }

    @Managed
    public int getDrainingNodeCount()
    {
        return getAllNodes().getDrainingNodes().size();
    }

    @Managed
    public int getDrainedNodeCount()
    {
        return getAllNodes().getDrainedNodes().size();
    }

    @Managed
    public int getShuttingDownNodeCount()
    {
        return getAllNodes().getShuttingDownNodes().size();
    }

    @Override
    public Set<InternalNode> getNodes(NodeState state)
    {
        return switch (state) {
            case ACTIVE -> getAllNodes().getActiveNodes();
            case INACTIVE -> getAllNodes().getInactiveNodes();
            case DRAINING -> getAllNodes().getDrainingNodes();
            case DRAINED -> getAllNodes().getDrainedNodes();
            case SHUTTING_DOWN -> getAllNodes().getShuttingDownNodes();
        };
    }

    @Override
    public synchronized NodesSnapshot getActiveNodesSnapshot()
    {
        return new NodesSnapshot(allNodes.getActiveNodes());
    }

    @Override
    public InternalNode getCurrentNode()
    {
        return currentNode;
    }

    @Override
    public synchronized Set<InternalNode> getCoordinators()
    {
        return coordinators;
    }

    @Override
    public synchronized void addNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.add(requireNonNull(listener, "listener is null"));
        AllNodes allNodes = this.allNodes;
        nodeStateEventExecutor.submit(() -> listener.accept(allNodes));
    }

    @Override
    public synchronized void removeNodeChangeListener(Consumer<AllNodes> listener)
    {
        listeners.remove(requireNonNull(listener, "listener is null"));
    }

    private static URI getHttpUri(ServiceDescriptor descriptor, boolean httpsRequired)
    {
        String url = descriptor.getProperties().get(httpsRequired ? "https" : "http");
        if (url != null) {
            return URI.create(url);
        }
        return null;
    }

    private static NodeVersion getNodeVersion(ServiceDescriptor descriptor)
    {
        String nodeVersion = descriptor.getProperties().get("node_version");
        return nodeVersion == null ? null : new NodeVersion(nodeVersion);
    }

    private static boolean isCoordinator(ServiceDescriptor service)
    {
        return Boolean.parseBoolean(service.getProperties().get("coordinator"));
    }
}
