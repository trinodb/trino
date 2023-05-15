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
package io.trino.execution.scheduler;

import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.trino.Session;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.execution.NodeTaskMap;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.CatalogHandle;

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
import static io.trino.SystemSessionProperties.getMaxUnacknowledgedSplitsPerTask;
import static io.trino.collect.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.metadata.NodeState.ACTIVE;
import static java.util.Objects.requireNonNull;

public class TopologyAwareNodeSelectorFactory
        implements NodeSelectorFactory
{
    private static final Logger LOG = Logger.get(TopologyAwareNodeSelectorFactory.class);

    private final NonEvictableCache<InternalNode, Object> inaccessibleNodeLogCache = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    .expireAfterWrite(30, TimeUnit.SECONDS));

    private final NetworkTopology networkTopology;
    private final InternalNodeManager nodeManager;
    private final int minCandidates;
    private final boolean includeCoordinator;
    private final long maxSplitsWeightPerNode;
    private final long minPendingSplitsWeightPerTask;
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
        requireNonNull(nodeTaskMap, "nodeTaskMap is null");

        this.networkTopology = networkTopology;
        this.nodeManager = nodeManager;
        this.minCandidates = schedulerConfig.getMinCandidates();
        this.includeCoordinator = schedulerConfig.isIncludeCoordinator();
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        int maxSplitsPerNode = schedulerConfig.getMaxSplitsPerNode();
        int minPendingSplitsPerTask = schedulerConfig.getMinPendingSplitsPerTask();
        checkArgument(maxSplitsPerNode >= minPendingSplitsPerTask, "maxSplitsPerNode must be > minPendingSplitsPerTask");
        this.maxSplitsWeightPerNode = SplitWeight.rawValueForStandardSplitCount(maxSplitsPerNode);
        this.minPendingSplitsWeightPerTask = SplitWeight.rawValueForStandardSplitCount(minPendingSplitsPerTask);

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
        this.placementCountersByName = placementCountersByName.buildOrThrow();
    }

    public Map<String, CounterStat> getPlacementCountersByName()
    {
        return placementCountersByName;
    }

    @Override
    public NodeSelector createNodeSelector(Session session, Optional<CatalogHandle> catalogHandle)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");

        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the split is about to be scheduled
        Supplier<NodeMap> nodeMap = Suppliers.memoizeWithExpiration(
                () -> createNodeMap(catalogHandle),
                5, TimeUnit.SECONDS);

        return new TopologyAwareNodeSelector(
                nodeManager,
                nodeTaskMap,
                includeCoordinator,
                nodeMap,
                minCandidates,
                maxSplitsWeightPerNode,
                minPendingSplitsWeightPerTask,
                getMaxUnacknowledgedSplitsPerTask(session),
                placementCounters,
                networkTopology);
    }

    private NodeMap createNodeMap(Optional<CatalogHandle> catalogHandle)
    {
        Set<InternalNode> nodes = catalogHandle
                .map(nodeManager::getActiveCatalogNodes)
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
                byHost.put(node.getInternalAddress(), node);
            }
            catch (UnknownHostException e) {
                if (markInaccessibleNode(node)) {
                    LOG.warn(e, "Unable to resolve host name for node: %s", node);
                }
            }
        }

        return new NodeMap(byHostAndPort.build(), byHost.build(), workersByNetworkPath.build(), coordinatorNodeIds);
    }

    /**
     * Returns true if node has been marked as inaccessible, or false if it was known to be inaccessible.
     */
    private boolean markInaccessibleNode(InternalNode node)
    {
        Object marker = new Object();
        return uncheckedCacheGet(inaccessibleNodeLogCache, node, () -> marker) == marker;
    }
}
