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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.RemoteTask;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.execution.scheduler.NetworkLocation.ROOT_LOCATION;
import static io.trino.execution.scheduler.NodeScheduler.calculateLowWatermark;
import static io.trino.execution.scheduler.NodeScheduler.canAssignSplitBasedOnWeight;
import static io.trino.execution.scheduler.NodeScheduler.getAllNodes;
import static io.trino.execution.scheduler.NodeScheduler.randomizedNodes;
import static io.trino.execution.scheduler.NodeScheduler.selectDistributionNodes;
import static io.trino.execution.scheduler.NodeScheduler.selectExactNodes;
import static io.trino.execution.scheduler.NodeScheduler.selectNodes;
import static io.trino.execution.scheduler.NodeScheduler.toWhenHasSplitQueueSpaceFuture;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Objects.requireNonNull;

public class TopologyAwareNodeSelector
        implements NodeSelector
{
    private static final Logger log = Logger.get(TopologyAwareNodeSelector.class);

    private final InternalNodeManager nodeManager;
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final long maxSplitsWeightPerNode;
    private final long maxPendingSplitsWeightPerTask;
    private final int maxUnacknowledgedSplitsPerTask;
    private final List<CounterStat> topologicalSplitCounters;
    private final NetworkTopology networkTopology;

    public TopologyAwareNodeSelector(
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            long maxSplitsWeightPerNode,
            long maxPendingSplitsWeightPerTask,
            int maxUnacknowledgedSplitsPerTask,
            List<CounterStat> topologicalSplitCounters,
            NetworkTopology networkTopology)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxSplitsWeightPerNode = maxSplitsWeightPerNode;
        this.maxPendingSplitsWeightPerTask = maxPendingSplitsWeightPerTask;
        this.maxUnacknowledgedSplitsPerTask = maxUnacknowledgedSplitsPerTask;
        checkArgument(maxUnacknowledgedSplitsPerTask > 0, "maxUnacknowledgedSplitsPerTask must be > 0, found: %s", maxUnacknowledgedSplitsPerTask);
        this.topologicalSplitCounters = requireNonNull(topologicalSplitCounters, "topologicalSplitCounters is null");
        this.networkTopology = requireNonNull(networkTopology, "networkTopology is null");
    }

    @Override
    public void lockDownNodes()
    {
        nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
    }

    @Override
    public List<InternalNode> allNodes()
    {
        return getAllNodes(nodeMap.get().get(), includeCoordinator);
    }

    @Override
    public InternalNode selectCurrentNode()
    {
        // TODO: this is a hack to force scheduling on the coordinator
        return nodeManager.getCurrentNode();
    }

    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        return selectNodes(limit, randomizedNodes(nodeMap.get().get(), includeCoordinator, excludedNodes));
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
    {
        NodeMap nodeMap = this.nodeMap.get().get();
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        int[] topologicCounters = new int[topologicalSplitCounters.size()];
        Set<NetworkLocation> filledLocations = new HashSet<>();
        Set<InternalNode> blockedExactNodes = new HashSet<>();
        boolean splitWaitingForAnyNode = false;
        for (Split split : splits) {
            SplitWeight splitWeight = split.getSplitWeight();
            if (!split.isRemotelyAccessible()) {
                List<InternalNode> candidateNodes = selectExactNodes(nodeMap, split.getAddresses(), includeCoordinator);
                if (candidateNodes.isEmpty()) {
                    log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getNodesByHost().keys());
                    throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                }
                InternalNode chosenNode = bestNodeSplitCount(splitWeight, candidateNodes.iterator(), minCandidates, maxPendingSplitsWeightPerTask, assignmentStats);
                if (chosenNode != null) {
                    assignment.put(chosenNode, split);
                    assignmentStats.addAssignedSplit(chosenNode, splitWeight);
                }
                // Exact node set won't matter, if a split is waiting for any node
                else if (!splitWaitingForAnyNode) {
                    blockedExactNodes.addAll(candidateNodes);
                }
                continue;
            }

            InternalNode chosenNode = null;
            int depth = topologicalSplitCounters.size() - 1;
            int chosenDepth = 0;
            Set<NetworkLocation> locations = new HashSet<>();
            for (HostAddress host : split.getAddresses()) {
                locations.add(networkTopology.locate(host));
            }
            if (locations.isEmpty()) {
                // Add the root location
                locations.add(ROOT_LOCATION);
                depth = 0;
            }
            // Try each address at progressively shallower network locations
            for (int i = depth; i >= 0 && chosenNode == null; i--) {
                for (NetworkLocation location : locations) {
                    // Skip locations which are only shallower than this level
                    // For example, locations which couldn't be located will be at the "root" location
                    if (location.getSegments().size() < i) {
                        continue;
                    }
                    location = location.subLocation(0, i);
                    if (filledLocations.contains(location)) {
                        continue;
                    }
                    Set<InternalNode> nodes = nodeMap.getWorkersByNetworkPath().get(location);
                    chosenNode = bestNodeSplitCount(splitWeight, new ResettableRandomizedIterator<>(nodes), minCandidates, calculateMinPendingSplitsWeightPerTask(i, depth), assignmentStats);
                    if (chosenNode != null) {
                        chosenDepth = i;
                        break;
                    }
                    filledLocations.add(location);
                }
            }
            if (chosenNode != null) {
                assignment.put(chosenNode, split);
                assignmentStats.addAssignedSplit(chosenNode, splitWeight);
                topologicCounters[chosenDepth]++;
            }
            else {
                splitWaitingForAnyNode = true;
            }
        }
        for (int i = 0; i < topologicCounters.length; i++) {
            if (topologicCounters[i] > 0) {
                topologicalSplitCounters.get(i).update(topologicCounters[i]);
            }
        }

        ListenableFuture<Void> blocked;
        long minPendingForWildcardNetworkAffinity = calculateMinPendingSplitsWeightPerTask(0, topologicalSplitCounters.size() - 1);
        if (splitWaitingForAnyNode) {
            blocked = toWhenHasSplitQueueSpaceFuture(existingTasks, calculateLowWatermark(minPendingForWildcardNetworkAffinity));
        }
        else {
            blocked = toWhenHasSplitQueueSpaceFuture(blockedExactNodes, existingTasks, calculateLowWatermark(minPendingForWildcardNetworkAffinity));
        }
        return new SplitPlacementResult(blocked, assignment);
    }

    /**
     * Computes how much of the queue can be filled by splits with the network topology distance to a node given by
     * splitAffinity. A split with zero affinity can only fill half the queue, whereas one that matches
     * exactly can fill the entire queue.
     */
    private long calculateMinPendingSplitsWeightPerTask(int splitAffinity, int totalDepth)
    {
        if (totalDepth == 0) {
            return maxPendingSplitsWeightPerTask;
        }
        // Use half the queue for any split
        // Reserve the other half for splits that have some amount of network affinity
        double queueFraction = 0.5 * (1.0 + splitAffinity / (double) totalDepth);
        return (long) Math.ceil(maxPendingSplitsWeightPerTask * queueFraction);
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
    {
        return selectDistributionNodes(nodeMap.get().get(), nodeTaskMap, maxSplitsWeightPerNode, maxPendingSplitsWeightPerTask, maxUnacknowledgedSplitsPerTask, splits, existingTasks, bucketNodeMap);
    }

    @Nullable
    private InternalNode bestNodeSplitCount(SplitWeight splitWeight, Iterator<InternalNode> candidates, int minCandidatesWhenFull, long minPendingSplitsWeightPerTask, NodeAssignmentStats assignmentStats)
    {
        InternalNode bestQueueNotFull = null;
        long minWeight = Long.MAX_VALUE;
        int fullCandidatesConsidered = 0;

        while (candidates.hasNext() && (fullCandidatesConsidered < minCandidatesWhenFull || bestQueueNotFull == null)) {
            InternalNode node = candidates.next();
            if (assignmentStats.getUnacknowledgedSplitCountForStage(node) >= maxUnacknowledgedSplitsPerTask) {
                fullCandidatesConsidered++;
                continue;
            }
            if (canAssignSplitBasedOnWeight(assignmentStats.getTotalSplitsWeight(node), maxSplitsWeightPerNode, splitWeight)) {
                return node;
            }
            fullCandidatesConsidered++;
            long taskQueuedWeight = assignmentStats.getQueuedSplitsWeightForStage(node);
            if (taskQueuedWeight < minWeight && canAssignSplitBasedOnWeight(taskQueuedWeight, minPendingSplitsWeightPerTask, splitWeight)) {
                minWeight = taskQueuedWeight;
                bestQueueNotFull = node;
            }
        }
        return bestQueueNotFull;
    }
}
