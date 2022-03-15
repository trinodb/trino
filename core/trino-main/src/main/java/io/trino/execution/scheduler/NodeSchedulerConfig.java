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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static java.util.Locale.ENGLISH;

@DefunctConfig({"node-scheduler.location-aware-scheduling-enabled", "node-scheduler.multiple-tasks-per-node-enabled"})
public class NodeSchedulerConfig
{
    public enum NodeSchedulerPolicy
    {
        UNIFORM, TOPOLOGY
    }

    public enum SplitsBalancingPolicy
    {
        NODE, STAGE
    }

    private int minCandidates = 10;
    private boolean includeCoordinator = true;
    private int maxSplitsPerNode = 100;
    private int maxPendingSplitsPerTask = 10;
    private NodeSchedulerPolicy nodeSchedulerPolicy = NodeSchedulerPolicy.UNIFORM;
    private boolean optimizedLocalScheduling = true;
    private SplitsBalancingPolicy splitsBalancingPolicy = SplitsBalancingPolicy.STAGE;
    private int maxUnacknowledgedSplitsPerTask = 500;
    private int maxAbsoluteFullNodesPerQuery = Integer.MAX_VALUE;
    private double maxFractionFullNodesPerQuery = 0.5;
    private NodeAllocatorType nodeAllocatorType = NodeAllocatorType.BIN_PACKING;

    @NotNull
    public NodeSchedulerPolicy getNodeSchedulerPolicy()
    {
        return nodeSchedulerPolicy;
    }

    @LegacyConfig("node-scheduler.network-topology")
    @Config("node-scheduler.policy")
    public NodeSchedulerConfig setNodeSchedulerPolicy(String nodeSchedulerPolicy)
    {
        this.nodeSchedulerPolicy = toNodeSchedulerPolicy(nodeSchedulerPolicy);
        return this;
    }

    private static NodeSchedulerPolicy toNodeSchedulerPolicy(String nodeSchedulerPolicy)
    {
        // "legacy" and "flat" are here for backward compatibility
        switch (nodeSchedulerPolicy.toLowerCase(ENGLISH)) {
            case "legacy":
            case "uniform":
                return NodeSchedulerPolicy.UNIFORM;
            case "flat":
            case "topology":
                return NodeSchedulerPolicy.TOPOLOGY;
            default:
                throw new IllegalArgumentException("Unknown node scheduler policy: " + nodeSchedulerPolicy);
        }
    }

    @Min(1)
    public int getMinCandidates()
    {
        return minCandidates;
    }

    @Config("node-scheduler.min-candidates")
    public NodeSchedulerConfig setMinCandidates(int candidates)
    {
        this.minCandidates = candidates;
        return this;
    }

    public boolean isIncludeCoordinator()
    {
        return includeCoordinator;
    }

    @Config("node-scheduler.include-coordinator")
    public NodeSchedulerConfig setIncludeCoordinator(boolean includeCoordinator)
    {
        this.includeCoordinator = includeCoordinator;
        return this;
    }

    @Config("node-scheduler.max-pending-splits-per-task")
    @LegacyConfig({"node-scheduler.max-pending-splits-per-node-per-task", "node-scheduler.max-pending-splits-per-node-per-stage"})
    public NodeSchedulerConfig setMaxPendingSplitsPerTask(int maxPendingSplitsPerTask)
    {
        this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        return this;
    }

    public int getMaxPendingSplitsPerTask()
    {
        return maxPendingSplitsPerTask;
    }

    public int getMaxSplitsPerNode()
    {
        return maxSplitsPerNode;
    }

    @Config("node-scheduler.max-splits-per-node")
    public NodeSchedulerConfig setMaxSplitsPerNode(int maxSplitsPerNode)
    {
        this.maxSplitsPerNode = maxSplitsPerNode;
        return this;
    }

    @Min(1)
    public int getMaxUnacknowledgedSplitsPerTask()
    {
        return maxUnacknowledgedSplitsPerTask;
    }

    @Config("node-scheduler.max-unacknowledged-splits-per-task")
    @ConfigDescription("Maximum number of leaf splits not yet delivered to a given task")
    public NodeSchedulerConfig setMaxUnacknowledgedSplitsPerTask(int maxUnacknowledgedSplitsPerTask)
    {
        this.maxUnacknowledgedSplitsPerTask = maxUnacknowledgedSplitsPerTask;
        return this;
    }

    @NotNull
    public SplitsBalancingPolicy getSplitsBalancingPolicy()
    {
        return splitsBalancingPolicy;
    }

    @Config("node-scheduler.splits-balancing-policy")
    @ConfigDescription("Strategy for balancing new splits on worker nodes")
    public NodeSchedulerConfig setSplitsBalancingPolicy(SplitsBalancingPolicy splitsBalancingPolicy)
    {
        this.splitsBalancingPolicy = splitsBalancingPolicy;
        return this;
    }

    public boolean getOptimizedLocalScheduling()
    {
        return optimizedLocalScheduling;
    }

    @Config("node-scheduler.optimized-local-scheduling")
    public NodeSchedulerConfig setOptimizedLocalScheduling(boolean optimizedLocalScheduling)
    {
        this.optimizedLocalScheduling = optimizedLocalScheduling;
        return this;
    }

    @Config("node-scheduler.max-absolute-full-nodes-per-query")
    public NodeSchedulerConfig setMaxAbsoluteFullNodesPerQuery(int maxAbsoluteFullNodesPerQuery)
    {
        this.maxAbsoluteFullNodesPerQuery = maxAbsoluteFullNodesPerQuery;
        return this;
    }

    public int getMaxAbsoluteFullNodesPerQuery()
    {
        return maxAbsoluteFullNodesPerQuery;
    }

    @Config("node-scheduler.max-fraction-full-nodes-per-query")
    public NodeSchedulerConfig setMaxFractionFullNodesPerQuery(double maxFractionFullNodesPerQuery)
    {
        this.maxFractionFullNodesPerQuery = maxFractionFullNodesPerQuery;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getMaxFractionFullNodesPerQuery()
    {
        return maxFractionFullNodesPerQuery;
    }

    public enum NodeAllocatorType
    {
        FIXED_COUNT,
        FULL_NODE_CAPABLE,
        BIN_PACKING
    }

    @NotNull
    public NodeAllocatorType getNodeAllocatorType()
    {
        return nodeAllocatorType;
    }

    @Config("node-scheduler.allocator-type")
    public NodeSchedulerConfig setNodeAllocatorType(String nodeAllocatorType)
    {
        this.nodeAllocatorType = toNodeAllocatorType(nodeAllocatorType);
        return this;
    }

    private static NodeAllocatorType toNodeAllocatorType(String nodeAllocatorType)
    {
        switch (nodeAllocatorType.toLowerCase(ENGLISH)) {
            case "fixed_count":
                return NodeAllocatorType.FIXED_COUNT;
            case "full_node_capable":
                return NodeAllocatorType.FULL_NODE_CAPABLE;
            case "bin_packing":
                return NodeAllocatorType.BIN_PACKING;
        }
        throw new IllegalArgumentException("Unknown node allocator type: " + nodeAllocatorType);
    }
}
