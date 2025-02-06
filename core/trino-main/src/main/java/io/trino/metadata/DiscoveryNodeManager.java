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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets.SetView;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.client.ServiceType;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogManagerConfig;
import io.trino.connector.CatalogManagerConfig.CatalogMangerKind;
import io.trino.failuredetector.FailureDetector;
import io.trino.server.InternalCommunicationConfig;
import io.trino.spi.connector.CatalogHandle;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.trino.connector.system.GlobalSystemConnector.CATALOG_HANDLE;
import static io.trino.metadata.NodeState.INACTIVE;
import static io.trino.metadata.NodeState.SHUTTING_DOWN;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

@ThreadSafe
public final class DiscoveryNodeManager
        implements InternalNodeManager
{
    private static final Logger log = Logger.get(DiscoveryNodeManager.class);

    private static final Splitter CATALOG_HANDLE_ID_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private final ServiceSelector serviceSelector;
    private final FailureDetector failureDetector;
    private final NodeVersion expectedNodeVersion;
    private final ConcurrentHashMap<String, RemoteNodeState> nodeStates = new ConcurrentHashMap<>();
    private final HttpClient httpClient;
    private final ScheduledExecutorService nodeStateUpdateExecutor;
    private final ExecutorService nodeStateEventExecutor;
    private final boolean httpsRequired;
    private final InternalNode currentNode;
    private final boolean allCatalogsOnAllNodes;

    @GuardedBy("this")
    private Optional<SetMultimap<CatalogHandle, InternalNode>> activeNodesByCatalogHandle = Optional.empty();

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
            FailureDetector failureDetector,
            NodeVersion expectedNodeVersion,
            @ForNodeManager HttpClient httpClient,
            InternalCommunicationConfig internalCommunicationConfig,
            CatalogManagerConfig catalogManagerConfig)
    {
        this.serviceSelector = requireNonNull(serviceSelector, "serviceSelector is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
        this.expectedNodeVersion = requireNonNull(expectedNodeVersion, "expectedNodeVersion is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeStateUpdateExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("node-state-poller-%s"));
        this.nodeStateEventExecutor = newCachedThreadPool(daemonThreadsNamed("node-state-events-%s"));
        this.httpsRequired = internalCommunicationConfig.isHttpsRequired();
        this.allCatalogsOnAllNodes = catalogManagerConfig.getCatalogMangerKind() != CatalogMangerKind.STATIC;

        this.currentNode = findCurrentNode(
                serviceSelector.selectAllServices(),
                nodeInfo.getNodeId(),
                expectedNodeVersion,
                httpsRequired);

        refreshNodesInternal();
    }

    private static InternalNode findCurrentNode(List<ServiceDescriptor> allServices, String currentNodeId, NodeVersion expectedNodeVersion, boolean httpsRequired)
    {
        for (ServiceDescriptor service : allServices) {
            URI uri = getHttpUri(service, httpsRequired);
            NodeVersion nodeVersion = getNodeVersion(service);
            if (uri != null && nodeVersion != null) {
                InternalNode node = new InternalNode(service.getNodeId(), uri, nodeVersion, isCoordinator(service));

                if (node.getNodeIdentifier().equals(currentNodeId)) {
                    checkState(
                            node.getNodeVersion().equals(expectedNodeVersion),
                            "INVARIANT: current node version (%s) should be equal to %s",
                            node.getNodeVersion(),
                            expectedNodeVersion);
                    return node;
                }
            }
        }
        throw new IllegalStateException("INVARIANT: current node not returned from service selector");
    }

    @PostConstruct
    public void startPollingNodeStates()
    {
        nodeStateUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                pollWorkers();
            }
            catch (Exception e) {
                log.error(e, "Error polling state of nodes");
            }
        }, 5, 5, TimeUnit.SECONDS);
        pollWorkers();
    }

    @PreDestroy
    public void destroy()
    {
        nodeStateUpdateExecutor.shutdown();
        nodeStateEventExecutor.shutdown();
    }

    private void pollWorkers()
    {
        AllNodes allNodes = getAllNodes();
        Set<InternalNode> aliveNodes = ImmutableSet.<InternalNode>builder()
                .addAll(allNodes.getActiveNodes())
                .addAll(allNodes.getDrainingNodes())
                .addAll(allNodes.getDrainedNodes())
                .addAll(allNodes.getShuttingDownNodes())
                .build();

        Set<String> aliveNodeIds = aliveNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        // Remove nodes that don't exist anymore
        // Make a copy to materialize the set difference
        Set<String> deadNodes = difference(nodeStates.keySet(), aliveNodeIds).immutableCopy();
        nodeStates.keySet().removeAll(deadNodes);

        // Add new nodes
        for (InternalNode node : aliveNodes) {
            nodeStates.putIfAbsent(node.getNodeIdentifier(),
                    new RemoteNodeState(httpClient, uriBuilderFrom(node.getInternalUri()).appendPath("/v1/info/state").build()));
        }

        // Schedule refresh
        nodeStates.values().forEach(RemoteNodeState::asyncRefresh);

        // update indexes
        refreshNodesInternal();
    }

    @PreDestroy
    public void stop()
    {
        nodeStateUpdateExecutor.shutdownNow();
    }

    @Override
    public void refreshNodes()
    {
        refreshNodesInternal();
    }

    private synchronized void refreshNodesInternal()
    {
        // This is a deny-list.
        Set<ServiceDescriptor> failed = failureDetector.getFailed();
        Set<ServiceDescriptor> services = serviceSelector.selectAllServices().stream()
                .filter(service -> !failed.contains(service))
                .collect(toImmutableSet());

        ImmutableSet.Builder<InternalNode> activeNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> inactiveNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> drainingNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> drainedNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> shuttingDownNodesBuilder = ImmutableSet.builder();
        ImmutableSet.Builder<InternalNode> coordinatorsBuilder = ImmutableSet.builder();
        ImmutableSetMultimap.Builder<CatalogHandle, InternalNode> byCatalogHandleBuilder = ImmutableSetMultimap.builder();

        for (ServiceDescriptor service : services) {
            URI uri = getHttpUri(service, httpsRequired);
            NodeVersion nodeVersion = getNodeVersion(service);
            boolean coordinator = isCoordinator(service);
            if (uri != null && nodeVersion != null) {
                InternalNode node = new InternalNode(service.getNodeId(), uri, nodeVersion, coordinator);
                NodeState nodeState = getNodeState(node);

                switch (nodeState) {
                    case ACTIVE:
                        activeNodesBuilder.add(node);
                        if (coordinator) {
                            coordinatorsBuilder.add(node);
                        }

                        // record available active nodes organized by catalog handle
                        String catalogHandleIds = service.getProperties().get("catalogHandleIds");
                        if (catalogHandleIds != null) {
                            catalogHandleIds = catalogHandleIds.toLowerCase(ENGLISH);
                            for (String catalogHandleId : CATALOG_HANDLE_ID_SPLITTER.split(catalogHandleIds)) {
                                byCatalogHandleBuilder.put(CatalogHandle.fromId(catalogHandleId), node);
                            }
                        }

                        // always add system connector
                        byCatalogHandleBuilder.put(CATALOG_HANDLE, node);
                        break;
                    case INACTIVE:
                        inactiveNodesBuilder.add(node);
                        break;
                    case DRAINING:
                        drainingNodesBuilder.add(node);
                        break;
                    case DRAINED:
                        drainedNodesBuilder.add(node);
                        break;
                    case SHUTTING_DOWN:
                        shuttingDownNodesBuilder.add(node);
                        break;
                    default:
                        log.error("Unknown state %s for node %s", nodeState, node);
                }
            }
        }

        Set<InternalNode> activeNodes = activeNodesBuilder.build();
        Set<InternalNode> drainingNodes = drainingNodesBuilder.build();
        Set<InternalNode> drainedNodes = drainedNodesBuilder.build();
        Set<InternalNode> inactiveNodes = inactiveNodesBuilder.build();
        Set<InternalNode> coordinators = coordinatorsBuilder.build();
        Set<InternalNode> shuttingDownNodes = shuttingDownNodesBuilder.build();
        if (allNodes != null) {
            // log node that are no longer active (but not shutting down)
            Set<InternalNode> aliveNodes = ImmutableSet.<InternalNode>builder()
                    .addAll(activeNodes)
                    .addAll(drainingNodes)
                    .addAll(drainedNodes)
                    .addAll(shuttingDownNodes)
                    .build();
            SetView<InternalNode> missingNodes = difference(allNodes.getActiveNodes(), aliveNodes);
            for (InternalNode missingNode : missingNodes) {
                log.info("Previously active node is missing: %s (last seen at %s)", missingNode.getNodeIdentifier(), missingNode.getHost());
            }
        }

        // nodes by catalog handle changes anytime a node adds or removes a catalog (note: this is not part of the listener system)
        if (!allCatalogsOnAllNodes) {
            activeNodesByCatalogHandle = Optional.of(byCatalogHandleBuilder.build());
        }

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

    private NodeState getNodeState(InternalNode node)
    {
        if (expectedNodeVersion.equals(node.getNodeVersion())) {
            String nodeId = node.getNodeIdentifier();
            // The empty case that is being set to a default value of ACTIVE is limited to the case where a node
            // has announced itself but no state has yet been successfully retrieved. RemoteNodeState will retain
            // the previously known state if any has been reported.
            return Optional.ofNullable(nodeStates.get(nodeId))
                    .flatMap(RemoteNodeState::getNodeState)
                    .orElse(NodeState.ACTIVE);
        }
        return INACTIVE;
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
    public synchronized Set<InternalNode> getActiveCatalogNodes(CatalogHandle catalogHandle)
    {
        // activeNodesByCatalogName is immutable
        return activeNodesByCatalogHandle
                .map(map -> map.get(catalogHandle))
                .orElseGet(() -> allNodes.getActiveNodes());
    }

    @Override
    public synchronized NodesSnapshot getActiveNodesSnapshot()
    {
        return new NodesSnapshot(allNodes.getActiveNodes(), activeNodesByCatalogHandle);
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
