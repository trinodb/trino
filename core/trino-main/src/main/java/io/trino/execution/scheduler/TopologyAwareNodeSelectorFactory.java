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
package io.prestosql.execution.scheduler;

import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.spi.HostAddress;

import javax.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.metadata.NodeState.ACTIVE;
import static java.util.Objects.requireNonNull;

public class TopologyAwareNodeSelectorFactory
        implements NodeSelectorFactory
{
    private static final Logger LOG = Logger.get(TopologyAwareNodeSelectorFactory.class);

    private final Cache<InternalNode, Boolean> inaccessibleNodeLogCache = CacheBuilder.newBuilder()
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();

    private final NetworkTopology networkTopology;
    private final InternalNodeManager nodeManager;
    private final int minCandidates;
    private final boolean includeCoordinator;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private final NodeTaskMap nodeTaskMap;

    private final List<CounterStat> placementCounters;
    private final Map<String, CounterStat> placementCountersByName;

    @Inject
    public TopologyAwareNodeSelectorFactory(
            NetworkTopology networkTopology,
            InternalNodeManager nodeManager,
            NodeSchedulerConfig schedulerConfig,
            NodeTaskMap nodeTaskMap,
            TopologyAwareNodeSelectorConfig topologyConfig)
    {
        requireNonNull(networkTopology, "networkTopology is null");
        requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(schedulerConfig, "schedulerConfig is null");
        requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        requireNonNull(topologyConfig, "topologyConfig is null");

        this.networkTopology = networkTopology;
        this.nodeManager = nodeManager;
        this.minCandidates = schedulerConfig.getMinCandidates();
        this.includeCoordinator = schedulerConfig.isIncludeCoordinator();
        this.maxSplitsPerNode = schedulerConfig.getMaxSplitsPerNode();
        this.maxPendingSplitsPerTask = schedulerConfig.getMaxPendingSplitsPerTask();
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        checkArgument(maxSplitsPerNode >= maxPendingSplitsPerTask, "maxSplitsPerNode must be > maxPendingSplitsPerTask");

        Builder<CounterStat> placementCounters = ImmutableList.builder();
        ImmutableMap.Builder<String, CounterStat> placementCountersByName = ImmutableMap.builder();

        // always add a counter for "all" locations, which is the default global segment
        CounterStat allCounter = new CounterStat();
        placementCounters.add(allCounter);
        placementCountersByName.put("all", allCounter);

        for (String segmentName : ImmutableList.copyOf(topologyConfig.getLocationSegmentNames())) {
            CounterStat segmentCounter = new CounterStat();
            placementCounters.add(segmentCounter);
            placementCountersByName.put(segmentName, segmentCounter);
        }

        this.placementCounters = placementCounters.build();
        this.placementCountersByName = placementCountersByName.build();
    }

    public Map<String, CounterStat> getPlacementCountersByName()
    {
        return placementCountersByName;
    }

    @Override
    public NodeSelector createNodeSelector(Optional<CatalogName> catalogName)
    {
        requireNonNull(catalogName, "catalogName is null");

        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the the split is about to be scheduled
        Supplier<NodeMap> nodeMap = Suppliers.memoizeWithExpiration(
                () -> createNodeMap(catalogName),
                5, TimeUnit.SECONDS);

        return new TopologyAwareNodeSelector(
                nodeManager,
                nodeTaskMap,
                includeCoordinator,
                nodeMap,
                minCandidates,
                maxSplitsPerNode,
                maxPendingSplitsPerTask,
                placementCounters,
                networkTopology);
    }

    private NodeMap createNodeMap(Optional<CatalogName> catalogName)
    {
        Set<InternalNode> nodes = catalogName
                .map(nodeManager::getActiveConnectorNodes)
                .orElseGet(() -> nodeManager.getNodes(ACTIVE));

        Set<String> coordinatorNodeIds = nodeManager.getCoordinators().stream()
                .map(InternalNode::getNodeIdentifier)
                .collect(toImmutableSet());

        ImmutableSetMultimap.Builder<HostAddress, InternalNode> byHostAndPort = ImmutableSetMultimap.builder();
        ImmutableSetMultimap.Builder<InetAddress, InternalNode> byHost = ImmutableSetMultimap.builder();
        ImmutableSetMultimap.Builder<NetworkLocation, InternalNode> workersByNetworkPath = ImmutableSetMultimap.builder();
        for (InternalNode node : nodes) {
            if (includeCoordinator || !coordinatorNodeIds.contains(node.getNodeIdentifier())) {
                NetworkLocation location = networkTopology.locate(node.getHostAndPort());
                for (int i = 0; i <= location.getSegments().size(); i++) {
                    workersByNetworkPath.put(location.subLocation(0, i), node);
                }
            }
            try {
                byHostAndPort.put(node.getHostAndPort(), node);

                InetAddress host = InetAddress.getByName(node.getInternalUri().getHost());
                byHost.put(host, node);
            }
            catch (UnknownHostException e) {
                if (inaccessibleNodeLogCache.getIfPresent(node) == null) {
                    inaccessibleNodeLogCache.put(node, true);
                    LOG.warn(e, "Unable to resolve host name for node: %s", node);
                }
            }
        }

        return new NodeMap(byHostAndPort.build(), byHost.build(), workersByNetworkPath.build(), coordinatorNodeIds);
    }
}
