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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.collect.cache.NonEvictableCache;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.NodeSchedulerConfig.SplitsBalancingPolicy;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.CatalogHandle;

import java.net.InetAddress;
import java.net.UnknownHostException;
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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class UniformNodeSelectorFactory
        implements NodeSelectorFactory
{
    private static final Logger LOG = Logger.get(UniformNodeSelectorFactory.class);

    private final NonEvictableCache<InternalNode, Object> inaccessibleNodeLogCache = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    .expireAfterWrite(30, TimeUnit.SECONDS));

    private final InternalNodeManager nodeManager;
    private final int minCandidates;
    private final boolean includeCoordinator;
    private final long maxSplitsWeightPerNode;
    private final long minPendingSplitsWeightPerTask;
    private final long maxAdjustedPendingSplitsWeightPerTask;
    private final SplitsBalancingPolicy splitsBalancingPolicy;
    private final boolean optimizedLocalScheduling;
    private final NodeTaskMap nodeTaskMap;
    private final Duration nodeMapMemoizationDuration;

    @Inject
    public UniformNodeSelectorFactory(
            InternalNodeManager nodeManager,
            NodeSchedulerConfig config,
            NodeTaskMap nodeTaskMap)
    {
        this(nodeManager, config, nodeTaskMap, new Duration(5, SECONDS));
    }

    @VisibleForTesting
    UniformNodeSelectorFactory(
            InternalNodeManager nodeManager,
            NodeSchedulerConfig config,
            NodeTaskMap nodeTaskMap,
            Duration nodeMapMemoizationDuration)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.minCandidates = config.getMinCandidates();
        this.includeCoordinator = config.isIncludeCoordinator();
        this.splitsBalancingPolicy = config.getSplitsBalancingPolicy();
        this.optimizedLocalScheduling = config.getOptimizedLocalScheduling();
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        int maxSplitsPerNode = config.getMaxSplitsPerNode();
        int minPendingSplitsPerTask = config.getMinPendingSplitsPerTask();
        int maxAdjustedPendingSplitsWeightPerTask = config.getMaxAdjustedPendingSplitsWeightPerTask();
        checkArgument(maxSplitsPerNode >= minPendingSplitsPerTask, "maxSplitsPerNode must be > minPendingSplitsPerTask");
        checkArgument(maxAdjustedPendingSplitsWeightPerTask >= minPendingSplitsPerTask, "maxPendingSplitsPerTask must be >= minPendingSplitsPerTask");
        this.maxSplitsWeightPerNode = SplitWeight.rawValueForStandardSplitCount(maxSplitsPerNode);
        this.minPendingSplitsWeightPerTask = SplitWeight.rawValueForStandardSplitCount(minPendingSplitsPerTask);
        this.maxAdjustedPendingSplitsWeightPerTask = SplitWeight.rawValueForStandardSplitCount(maxAdjustedPendingSplitsWeightPerTask);
        this.nodeMapMemoizationDuration = nodeMapMemoizationDuration;
    }

    @Override
    public NodeSelector createNodeSelector(Session session, Optional<CatalogHandle> catalogHandle)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");

        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the split is about to be scheduled
        Supplier<NodeMap> nodeMap;
        if (nodeMapMemoizationDuration.toMillis() > 0) {
            nodeMap = Suppliers.memoizeWithExpiration(
                    () -> createNodeMap(catalogHandle),
                    nodeMapMemoizationDuration.toMillis(), MILLISECONDS);
        }
        else {
            nodeMap = () -> createNodeMap(catalogHandle);
        }

        return new UniformNodeSelector(
                nodeManager,
                nodeTaskMap,
                includeCoordinator,
                nodeMap,
                minCandidates,
                maxSplitsWeightPerNode,
                minPendingSplitsWeightPerTask,
                maxAdjustedPendingSplitsWeightPerTask,
                getMaxUnacknowledgedSplitsPerTask(session),
                splitsBalancingPolicy,
                optimizedLocalScheduling);
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
        for (InternalNode node : nodes) {
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

        return new NodeMap(byHostAndPort.build(), byHost.build(), ImmutableSetMultimap.of(), coordinatorNodeIds);
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
