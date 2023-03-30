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
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

import static java.util.Locale.ENGLISH;

@DefunctConfig({
        "node-scheduler.location-aware-scheduling-enabled",
        "node-scheduler.multiple-tasks-per-node-enabled",
        "node-scheduler.max-fraction-full-nodes-per-query",
        "node-scheduler.max-absolute-full-nodes-per-query"})
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
    private int minPendingSplitsPerTask = 10;
    private int maxAdjustedPendingSplitsWeightPerTask = 2000;
    private NodeSchedulerPolicy nodeSchedulerPolicy = NodeSchedulerPolicy.UNIFORM;
    private boolean optimizedLocalScheduling = true;
    private SplitsBalancingPolicy splitsBalancingPolicy = SplitsBalancingPolicy.STAGE;
    private int maxUnacknowledgedSplitsPerTask = 2000;
    private Duration allowedNoMatchingNodePeriod = new Duration(2, TimeUnit.MINUTES);
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

    @Config("node-scheduler.min-pending-splits-per-task")
    @LegacyConfig({"node-scheduler.max-pending-splits-per-task", "node-scheduler.max-pending-splits-per-node-per-task", "node-scheduler.max-pending-splits-per-node-per-stage"})
    public NodeSchedulerConfig setMinPendingSplitsPerTask(int minPendingSplitsPerTask)
    {
        this.minPendingSplitsPerTask = minPendingSplitsPerTask;
        return this;
    }

    public int getMinPendingSplitsPerTask()
    {
        return minPendingSplitsPerTask;
    }

    @Config("node-scheduler.max-adjusted-pending-splits-per-task")
    public NodeSchedulerConfig setMaxAdjustedPendingSplitsWeightPerTask(int maxAdjustedPendingSplitsWeightPerTask)
    {
        this.maxAdjustedPendingSplitsWeightPerTask = maxAdjustedPendingSplitsWeightPerTask;
        return this;
    }

    @Min(0)
    public int getMaxAdjustedPendingSplitsWeightPerTask()
    {
        return maxAdjustedPendingSplitsWeightPerTask;
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

    @Config("node-scheduler.allowed-no-matching-node-period")
    @ConfigDescription("How long scheduler should wait before failing a query for which hard task requirements (e.g. node exposing specific catalog) cannot be satisfied")
    public NodeSchedulerConfig setAllowedNoMatchingNodePeriod(Duration allowedNoMatchingNodePeriod)
    {
        this.allowedNoMatchingNodePeriod = allowedNoMatchingNodePeriod;
        return this;
    }

    public Duration getAllowedNoMatchingNodePeriod()
    {
        return allowedNoMatchingNodePeriod;
    }

    public enum NodeAllocatorType
    {
        FIXED_COUNT,
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
            case "bin_packing":
                return NodeAllocatorType.BIN_PACKING;
        }
        throw new IllegalArgumentException("Unknown node allocator type: " + nodeAllocatorType);
    }
}
