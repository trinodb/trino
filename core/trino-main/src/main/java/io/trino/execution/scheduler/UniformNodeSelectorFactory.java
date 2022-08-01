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

import io.trino.Session;
import io.trino.connector.CatalogHandle;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.scheduler.NodeSchedulerConfig.SplitsBalancingPolicy;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeMap;
import io.trino.spi.SplitWeight;

import javax.inject.Inject;

import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.SystemSessionProperties.getMaxUnacknowledgedSplitsPerTask;
import static java.util.Objects.requireNonNull;

public class UniformNodeSelectorFactory
        implements NodeSelectorFactory
{
    private final InternalNodeManager nodeManager;
    private final int minCandidates;
    private final boolean includeCoordinator;
    private final long maxSplitsWeightPerNode;
    private final long maxPendingSplitsWeightPerTask;
    private final SplitsBalancingPolicy splitsBalancingPolicy;
    private final boolean optimizedLocalScheduling;
    private final NodeTaskMap nodeTaskMap;

    @Inject
    public UniformNodeSelectorFactory(
            InternalNodeManager nodeManager,
            NodeSchedulerConfig config,
            NodeTaskMap nodeTaskMap)
    {
        requireNonNull(nodeManager, "nodeManager is null");
        requireNonNull(config, "config is null");
        requireNonNull(nodeTaskMap, "nodeTaskMap is null");

        this.nodeManager = nodeManager;
        this.minCandidates = config.getMinCandidates();
        this.includeCoordinator = config.isIncludeCoordinator();
        this.splitsBalancingPolicy = config.getSplitsBalancingPolicy();
        this.optimizedLocalScheduling = config.getOptimizedLocalScheduling();
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        int maxSplitsPerNode = config.getMaxSplitsPerNode();
        int maxPendingSplitsPerTask = config.getMaxPendingSplitsPerTask();
        checkArgument(maxSplitsPerNode >= maxPendingSplitsPerTask, "maxSplitsPerNode must be > maxPendingSplitsPerTask");
        this.maxSplitsWeightPerNode = SplitWeight.rawValueForStandardSplitCount(maxSplitsPerNode);
        this.maxPendingSplitsWeightPerTask = SplitWeight.rawValueForStandardSplitCount(maxPendingSplitsPerTask);
    }

    @Override
    public NodeSelector createNodeSelector(Session session, Optional<CatalogHandle> catalogHandle)
    {
        requireNonNull(catalogHandle, "catalogName is null");
        Supplier<NodeMap> nodeMap = () -> nodeManager.createNodeMap(catalogHandle);

        return new UniformNodeSelector(
                nodeManager,
                nodeTaskMap,
                includeCoordinator,
                nodeMap,
                minCandidates,
                maxSplitsWeightPerNode,
                maxPendingSplitsWeightPerTask,
                getMaxUnacknowledgedSplitsPerTask(session),
                splitsBalancingPolicy,
                optimizedLocalScheduling);
    }
}
