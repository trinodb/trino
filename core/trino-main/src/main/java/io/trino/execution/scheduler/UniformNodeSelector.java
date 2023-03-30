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
import com.google.common.base.Ticker;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.RemoteTask;
import io.trino.execution.resourcegroups.IndexedPriorityQueue;
import io.trino.execution.scheduler.NodeSchedulerConfig.SplitsBalancingPolicy;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.TrinoException;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.scheduler.NodeScheduler.calculateLowWatermark;
import static io.trino.execution.scheduler.NodeScheduler.filterNodes;
import static io.trino.execution.scheduler.NodeScheduler.getAllNodes;
import static io.trino.execution.scheduler.NodeScheduler.randomizedNodes;
import static io.trino.execution.scheduler.NodeScheduler.selectDistributionNodes;
import static io.trino.execution.scheduler.NodeScheduler.selectExactNodes;
import static io.trino.execution.scheduler.NodeScheduler.selectNodes;
import static io.trino.execution.scheduler.NodeScheduler.toWhenHasSplitQueueSpaceFuture;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

public class UniformNodeSelector
        implements NodeSelector
{
    private static final Logger log = Logger.get(UniformNodeSelector.class);

    private final InternalNodeManager nodeManager;
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final long maxSplitsWeightPerNode;
    private final long minPendingSplitsWeightPerTask;
    private final int maxUnacknowledgedSplitsPerTask;
    private final SplitsBalancingPolicy splitsBalancingPolicy;
    private final boolean optimizedLocalScheduling;
    private final QueueSizeAdjuster queueSizeAdjuster;

    public UniformNodeSelector(
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            long maxSplitsWeightPerNode,
            long minPendingSplitsWeightPerTask,
            long maxAdjustedPendingSplitsWeightPerTask,
            int maxUnacknowledgedSplitsPerTask,
            SplitsBalancingPolicy splitsBalancingPolicy,
            boolean optimizedLocalScheduling)
    {
        this(nodeManager,
                nodeTaskMap,
                includeCoordinator,
                nodeMap,
                minCandidates,
                maxSplitsWeightPerNode,
                minPendingSplitsWeightPerTask,
                maxUnacknowledgedSplitsPerTask,
                splitsBalancingPolicy,
                optimizedLocalScheduling,
                new QueueSizeAdjuster(minPendingSplitsWeightPerTask, maxAdjustedPendingSplitsWeightPerTask));
    }

    @VisibleForTesting
    UniformNodeSelector(
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            long maxSplitsWeightPerNode,
            long minPendingSplitsWeightPerTask,
            int maxUnacknowledgedSplitsPerTask,
            SplitsBalancingPolicy splitsBalancingPolicy,
            boolean optimizedLocalScheduling,
            QueueSizeAdjuster queueSizeAdjuster)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxSplitsWeightPerNode = maxSplitsWeightPerNode;
        this.minPendingSplitsWeightPerTask = minPendingSplitsWeightPerTask;
        this.maxUnacknowledgedSplitsPerTask = maxUnacknowledgedSplitsPerTask;
        checkArgument(maxUnacknowledgedSplitsPerTask > 0, "maxUnacknowledgedSplitsPerTask must be > 0, found: %s", maxUnacknowledgedSplitsPerTask);
        this.splitsBalancingPolicy = requireNonNull(splitsBalancingPolicy, "splitsBalancingPolicy is null");
        this.optimizedLocalScheduling = optimizedLocalScheduling;
        this.queueSizeAdjuster = queueSizeAdjuster;
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
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeMap nodeMap = this.nodeMap.get().get();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);
        queueSizeAdjuster.update(existingTasks, assignmentStats);
        Set<InternalNode> blockedExactNodes = new HashSet<>();
        boolean splitWaitingForAnyNode = false;
        // splitsToBeRedistributed becomes true only when splits go through locality-based assignment
        boolean splitsToBeRedistributed = false;
        Set<Split> remainingSplits = new HashSet<>(splits.size());

        List<InternalNode> filteredNodes = filterNodes(nodeMap, includeCoordinator, ImmutableSet.of());
        ResettableRandomizedIterator<InternalNode> randomCandidates = new ResettableRandomizedIterator<>(filteredNodes);
        Set<InternalNode> schedulableNodes = new HashSet<>(filteredNodes);

        // optimizedLocalScheduling enables prioritized assignment of splits to local nodes when splits contain locality information
        if (optimizedLocalScheduling) {
            for (Split split : splits) {
                if (split.isRemotelyAccessible() && !split.getAddresses().isEmpty()) {
                    List<InternalNode> candidateNodes = selectExactNodes(nodeMap, split.getAddresses(), includeCoordinator);

                    Optional<InternalNode> chosenNode = candidateNodes.stream()
                            .filter(ownerNode -> assignmentStats.getTotalSplitsWeight(ownerNode) < maxSplitsWeightPerNode && assignmentStats.getUnacknowledgedSplitCountForStage(ownerNode) < maxUnacknowledgedSplitsPerTask)
                            .min(comparingLong(assignmentStats::getTotalSplitsWeight));

                    if (chosenNode.isPresent()) {
                        assignment.put(chosenNode.get(), split);
                        assignmentStats.addAssignedSplit(chosenNode.get(), split.getSplitWeight());
                        splitsToBeRedistributed = true;
                        continue;
                    }
                }
                remainingSplits.add(split);
            }
        }
        else {
            remainingSplits = splits;
        }

        for (Split split : remainingSplits) {
            randomCandidates.reset();

            List<InternalNode> candidateNodes;
            if (!split.isRemotelyAccessible()) {
                candidateNodes = selectExactNodes(nodeMap, split.getAddresses(), includeCoordinator);
            }
            else {
                candidateNodes = selectNodes(minCandidates, randomCandidates);
            }
            if (candidateNodes.isEmpty()) {
                log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getNodesByHost().keys());
                throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query");
            }

            InternalNode chosenNode = chooseNodeForSplit(assignmentStats, candidateNodes);
            if (chosenNode == null) {
                long minWeight = Long.MAX_VALUE;
                for (InternalNode node : candidateNodes) {
                    long queuedWeight = assignmentStats.getQueuedSplitsWeightForStage(node);
                    long adjustedMaxPendingSplitsWeightPerTask = queueSizeAdjuster.getAdjustedMaxPendingSplitsWeightPerTask(node.getNodeIdentifier());

                    if (queuedWeight <= minWeight && queuedWeight < adjustedMaxPendingSplitsWeightPerTask && assignmentStats.getUnacknowledgedSplitCountForStage(node) < maxUnacknowledgedSplitsPerTask) {
                        chosenNode = node;
                        minWeight = queuedWeight;
                    }
                    if (queuedWeight >= adjustedMaxPendingSplitsWeightPerTask) {
                        // Mark node for adjustment, since its queue is full, and we still have split to assign.
                        queueSizeAdjuster.scheduleAdjustmentForNode(node.getNodeIdentifier());
                    }
                }
            }
            if (chosenNode != null) {
                assignment.put(chosenNode, split);
                assignmentStats.addAssignedSplit(chosenNode, split.getSplitWeight());
            }
            else {
                candidateNodes.forEach(schedulableNodes::remove);
                if (split.isRemotelyAccessible()) {
                    splitWaitingForAnyNode = true;
                }
                // Exact node set won't matter, if a split is waiting for any node
                else if (!splitWaitingForAnyNode) {
                    blockedExactNodes.addAll(candidateNodes);
                }

                if (splitWaitingForAnyNode && schedulableNodes.isEmpty()) {
                    // All nodes assigned, no need to test if we can assign new split
                    break;
                }
            }
        }

        ListenableFuture<Void> blocked;
        if (splitWaitingForAnyNode) {
            blocked = toWhenHasSplitQueueSpaceFuture(existingTasks, calculateLowWatermark(minPendingSplitsWeightPerTask));
        }
        else {
            blocked = toWhenHasSplitQueueSpaceFuture(blockedExactNodes, existingTasks, calculateLowWatermark(minPendingSplitsWeightPerTask));
        }

        if (splitsToBeRedistributed) {
            equateDistribution(assignment, assignmentStats, nodeMap, includeCoordinator);
        }
        return new SplitPlacementResult(blocked, assignment);
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
    {
        // TODO: Implement split assignment adjustment based on how quick node is able to process splits. More information https://github.com/trinodb/trino/pull/15168
        return selectDistributionNodes(nodeMap.get().get(), nodeTaskMap, maxSplitsWeightPerNode, minPendingSplitsWeightPerTask, maxUnacknowledgedSplitsPerTask, splits, existingTasks, bucketNodeMap);
    }

    @Nullable
    private InternalNode chooseNodeForSplit(NodeAssignmentStats assignmentStats, List<InternalNode> candidateNodes)
    {
        InternalNode chosenNode = null;
        long minWeight = Long.MAX_VALUE;

        List<InternalNode> freeNodes = getFreeNodesForStage(assignmentStats, candidateNodes);
        switch (splitsBalancingPolicy) {
            case STAGE:
                for (InternalNode node : freeNodes) {
                    long queuedWeight = assignmentStats.getQueuedSplitsWeightForStage(node);
                    if (queuedWeight <= minWeight) {
                        chosenNode = node;
                        minWeight = queuedWeight;
                    }
                }
                break;
            case NODE:
                for (InternalNode node : freeNodes) {
                    long totalSplitsWeight = assignmentStats.getTotalSplitsWeight(node);
                    if (totalSplitsWeight <= minWeight) {
                        chosenNode = node;
                        minWeight = totalSplitsWeight;
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException("Unsupported split balancing policy " + splitsBalancingPolicy);
        }

        return chosenNode;
    }

    private List<InternalNode> getFreeNodesForStage(NodeAssignmentStats assignmentStats, List<InternalNode> nodes)
    {
        ImmutableList.Builder<InternalNode> freeNodes = ImmutableList.builder();
        for (InternalNode node : nodes) {
            if (assignmentStats.getTotalSplitsWeight(node) < maxSplitsWeightPerNode && assignmentStats.getUnacknowledgedSplitCountForStage(node) < maxUnacknowledgedSplitsPerTask) {
                freeNodes.add(node);
            }
        }
        return freeNodes.build();
    }

    /**
     * The method tries to make the distribution of splits more uniform. All nodes are arranged into a maxHeap and a minHeap
     * based on the number of splits that are assigned to them. Splits are redistributed, one at a time, from a maxNode to a
     * minNode until we have as uniform a distribution as possible.
     *
     * @param assignment the node-splits multimap after the first and the second stage
     * @param assignmentStats required to obtain info regarding splits assigned to a node outside the current batch of assignment
     * @param nodeMap to get a list of all nodes to which splits can be assigned
     */
    private void equateDistribution(Multimap<InternalNode, Split> assignment, NodeAssignmentStats assignmentStats, NodeMap nodeMap, boolean includeCoordinator)
    {
        if (assignment.isEmpty()) {
            return;
        }

        Collection<InternalNode> allNodes = nodeMap.getNodesByHostAndPort().values().stream()
                .filter(node -> includeCoordinator || !nodeMap.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                .collect(toImmutableList());

        if (allNodes.size() < 2) {
            return;
        }

        IndexedPriorityQueue<InternalNode> maxNodes = new IndexedPriorityQueue<>();
        for (InternalNode node : assignment.keySet()) {
            maxNodes.addOrUpdate(node, assignmentStats.getTotalSplitsWeight(node));
        }

        IndexedPriorityQueue<InternalNode> minNodes = new IndexedPriorityQueue<>();
        for (InternalNode node : allNodes) {
            minNodes.addOrUpdate(node, Long.MAX_VALUE - assignmentStats.getTotalSplitsWeight(node));
        }

        while (true) {
            if (maxNodes.isEmpty()) {
                return;
            }

            // fetch min and max node
            InternalNode maxNode = maxNodes.poll();
            InternalNode minNode = minNodes.poll();

            // Allow some degree of non uniformity when assigning splits to nodes. Usually data distribution
            // among nodes in a cluster won't be fully uniform (e.g. because hash function with non-uniform
            // distribution is used like consistent hashing). In such case it makes sense to assign splits to nodes
            // with data because of potential savings in network throughput and CPU time.
            // The difference of 5 between node with maximum and minimum splits is a tradeoff between ratio of
            // misassigned splits and assignment uniformity. Using larger numbers doesn't reduce the number of
            // misassigned splits greatly (in absolute values).
            if (assignmentStats.getTotalSplitsWeight(maxNode) - assignmentStats.getTotalSplitsWeight(minNode) <= SplitWeight.rawValueForStandardSplitCount(5)) {
                return;
            }

            // move split from max to min
            Split redistributed = redistributeSplit(assignment, maxNode, minNode, nodeMap.getNodesByHost());
            assignmentStats.removeAssignedSplit(maxNode, redistributed.getSplitWeight());
            assignmentStats.addAssignedSplit(minNode, redistributed.getSplitWeight());

            // add max back into maxNodes only if it still has assignments
            if (assignment.containsKey(maxNode)) {
                maxNodes.addOrUpdate(maxNode, assignmentStats.getTotalSplitsWeight(maxNode));
            }

            // Add or update both the Priority Queues with the updated node priorities
            maxNodes.addOrUpdate(minNode, assignmentStats.getTotalSplitsWeight(minNode));
            minNodes.addOrUpdate(minNode, Long.MAX_VALUE - assignmentStats.getTotalSplitsWeight(minNode));
            minNodes.addOrUpdate(maxNode, Long.MAX_VALUE - assignmentStats.getTotalSplitsWeight(maxNode));
        }
    }

    /**
     * The method selects and removes a split from the fromNode and assigns it to the toNode. There is an attempt to
     * redistribute a Non-local split if possible. This case is possible when there are multiple queries running
     * simultaneously. If a Non-local split cannot be found in the maxNode, any split is selected randomly and reassigned.
     */
    @VisibleForTesting
    public static Split redistributeSplit(Multimap<InternalNode, Split> assignment, InternalNode fromNode, InternalNode toNode, SetMultimap<InetAddress, InternalNode> nodesByHost)
    {
        Iterator<Split> splitIterator = assignment.get(fromNode).iterator();
        Split splitToBeRedistributed = null;
        while (splitIterator.hasNext()) {
            Split split = splitIterator.next();
            // Try to select non-local split for redistribution
            if (!split.getAddresses().isEmpty() && !isSplitLocal(split.getAddresses(), fromNode.getHostAndPort(), nodesByHost)) {
                splitToBeRedistributed = split;
                break;
            }
        }
        // Select any split if maxNode has no non-local splits in the current batch of assignment
        if (splitToBeRedistributed == null) {
            splitIterator = assignment.get(fromNode).iterator();
            splitToBeRedistributed = splitIterator.next();
        }
        splitIterator.remove();
        assignment.put(toNode, splitToBeRedistributed);
        return splitToBeRedistributed;
    }

    /**
     * Helper method to determine if a split is local to a node irrespective of whether splitAddresses contain port information or not
     */
    private static boolean isSplitLocal(List<HostAddress> splitAddresses, HostAddress nodeAddress, SetMultimap<InetAddress, InternalNode> nodesByHost)
    {
        for (HostAddress address : splitAddresses) {
            if (nodeAddress.equals(address)) {
                return true;
            }
            InetAddress inetAddress;
            try {
                inetAddress = address.toInetAddress();
            }
            catch (UnknownHostException e) {
                continue;
            }
            if (!address.hasPort()) {
                Set<InternalNode> localNodes = nodesByHost.get(inetAddress);
                return localNodes.stream()
                        .anyMatch(node -> node.getHostAndPort().equals(nodeAddress));
            }
        }
        return false;
    }

    static class QueueSizeAdjuster
    {
        private static final long SCALE_DOWN_INTERVAL = SECONDS.toNanos(1);
        private final Ticker ticker;
        private final Map<String, TaskAdjustmentInfo> taskAdjustmentInfos = new HashMap<>();
        private final Set<String> previousScheduleFullTasks = new HashSet<>();
        private final long minPendingSplitsWeightPerTask;
        private final long maxAdjustedPendingSplitsWeightPerTask;

        private QueueSizeAdjuster(long minPendingSplitsWeightPerTask, long maxAdjustedPendingSplitsWeightPerTask)
        {
            this(minPendingSplitsWeightPerTask, maxAdjustedPendingSplitsWeightPerTask, Ticker.systemTicker());
        }

        @VisibleForTesting
        QueueSizeAdjuster(long minPendingSplitsWeightPerTask, long maxAdjustedPendingSplitsWeightPerTask, Ticker ticker)
        {
            this.ticker = requireNonNull(ticker, "ticker is null");
            this.maxAdjustedPendingSplitsWeightPerTask = maxAdjustedPendingSplitsWeightPerTask;
            this.minPendingSplitsWeightPerTask = minPendingSplitsWeightPerTask;
        }

        public void update(List<RemoteTask> existingTasks, NodeAssignmentStats nodeAssignmentStats)
        {
            if (!isEnabled()) {
                return;
            }
            for (RemoteTask task : existingTasks) {
                String nodeId = task.getNodeId();
                TaskAdjustmentInfo nodeTaskAdjustmentInfo = taskAdjustmentInfos.computeIfAbsent(nodeId, key -> new TaskAdjustmentInfo(minPendingSplitsWeightPerTask));
                Optional<Long> lastAdjustmentTime = nodeTaskAdjustmentInfo.getLastAdjustmentNanos();

                if (previousScheduleFullTasks.contains(nodeId) && nodeAssignmentStats.getQueuedSplitsWeightForStage(nodeId) == 0) {
                    // even if we max out adjustment we want to move forward lastAdjustmentTime
                    nodeTaskAdjustmentInfo.setAdjustedMaxSplitsWeightPerTask(Math.min(maxAdjustedPendingSplitsWeightPerTask, nodeTaskAdjustmentInfo.getAdjustedMaxSplitsWeightPerTask() * 2));
                }
                else if (lastAdjustmentTime.isPresent() && (ticker.read() - lastAdjustmentTime.get()) >= SCALE_DOWN_INTERVAL) {
                    nodeTaskAdjustmentInfo.setAdjustedMaxSplitsWeightPerTask((long) Math.max(minPendingSplitsWeightPerTask, nodeTaskAdjustmentInfo.getAdjustedMaxSplitsWeightPerTask() / 1.5));
                }
            }
            previousScheduleFullTasks.clear();
        }

        public long getAdjustedMaxPendingSplitsWeightPerTask(String nodeId)
        {
            TaskAdjustmentInfo nodeTaskAdjustmentInfo = taskAdjustmentInfos.get(nodeId);

            return nodeTaskAdjustmentInfo != null ? nodeTaskAdjustmentInfo.getAdjustedMaxSplitsWeightPerTask() : minPendingSplitsWeightPerTask;
        }

        public void scheduleAdjustmentForNode(String nodeIdentifier)
        {
            if (!isEnabled()) {
                return;
            }

            previousScheduleFullTasks.add(nodeIdentifier);
        }

        private boolean isEnabled()
        {
            return maxAdjustedPendingSplitsWeightPerTask != minPendingSplitsWeightPerTask;
        }

        private class TaskAdjustmentInfo
        {
            private long adjustedMaxSplitsWeightPerTask;
            private Optional<Long> lastAdjustmentNanos;

            public TaskAdjustmentInfo(long adjustedMaxSplitsWeightPerTask)
            {
                this.adjustedMaxSplitsWeightPerTask = adjustedMaxSplitsWeightPerTask;
                this.lastAdjustmentNanos = Optional.empty();
            }

            public long getAdjustedMaxSplitsWeightPerTask()
            {
                return adjustedMaxSplitsWeightPerTask;
            }

            public void setAdjustedMaxSplitsWeightPerTask(long adjustedMaxSplitsWeightPerTask)
            {
                this.adjustedMaxSplitsWeightPerTask = adjustedMaxSplitsWeightPerTask;
                this.lastAdjustmentNanos = Optional.of(ticker.read());
            }

            public Optional<Long> getLastAdjustmentNanos()
            {
                return lastAdjustmentNanos;
            }
        }
    }
}
