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

import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.RemoteTask;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.execution.scheduler.NodeScheduler.calculateLowWatermark;
import static io.prestosql.execution.scheduler.NodeScheduler.randomizedNodes;
import static io.prestosql.execution.scheduler.NodeScheduler.selectExactNodes;
import static io.prestosql.execution.scheduler.NodeScheduler.selectNodes;
import static io.prestosql.execution.scheduler.NodeScheduler.toWhenHasSplitQueueSpaceFuture;
import static io.prestosql.spi.StandardErrorCode.NO_NODES_AVAILABLE;

public class OptimizedLocalNodeSelector
        extends SimpleNodeSelector
{
    private static final Logger log = Logger.get(OptimizedLocalNodeSelector.class);

    OptimizedLocalNodeSelector(
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            int maxSplitsPerNode,
            int maxPendingSplitsPerTask)
    {
        super(nodeManager, nodeTaskMap, includeCoordinator, nodeMap, minCandidates, maxSplitsPerNode, maxPendingSplitsPerTask);
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
    {
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeMap nodeMap = this.nodeMap.get().get();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        ResettableRandomizedIterator<InternalNode> randomCandidates = randomizedNodes(nodeMap, includeCoordinator, ImmutableSet.of());
        Set<InternalNode> blockedExactNodes = new HashSet<>();
        boolean splitWaitingForAnyNode = false;
        // splitsToBeRedistributed remains false for internal splits or if force-local-scheduling is true
        boolean splitsToBeRedistributed = false;

        //prioritize assignment of splits to local nodes when the local nodes have slots available in the first pass
        Set<Split> splitsToBeScheduled = new HashSet<>(splits);
        for (Split split : splits) {
            if (split.isRemotelyAccessible() && (split.getAddresses().size() > 0)
                    && split.getAddresses().stream().anyMatch(address -> !address.getHostText().equals("localhost"))
                    && !(CatalogName.isInternalSystemConnector(split.getCatalogName()) || split.getCatalogName().toString().equalsIgnoreCase("jmx"))) {
                splitsToBeRedistributed = true;
                List<InternalNode> candidateNodes;
                candidateNodes = selectExactNodes(nodeMap, split.getAddresses(), includeCoordinator);

                if (candidateNodes.isEmpty()) {
                    continue;
                }

                Optional<InternalNode> chosenNode = candidateNodes.stream()
                        .filter(ownerNode -> assignmentStats.getTotalSplitCount(ownerNode) < maxSplitsPerNode)
                        .min((node1, node2) -> Integer.compare(assignmentStats.getTotalSplitCount(node1), assignmentStats.getTotalSplitCount(node2)));

                if (chosenNode.isPresent()) {
                    assignment.put(chosenNode.get(), split);
                    assignmentStats.addAssignedSplit(chosenNode.get());
                    splitsToBeScheduled.remove(split);
                }
            }
        }
        splits = splitsToBeScheduled;

        // fall back to default behaviour for only those splits whose local node had no slots left in the second pass
        for (Split split : splits) {
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
                throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
            }

            InternalNode chosenNode = null;
            int min = Integer.MAX_VALUE;

            for (InternalNode node : candidateNodes) {
                int totalSplitCount = assignmentStats.getTotalSplitCount(node);
                if (totalSplitCount < min && totalSplitCount < maxSplitsPerNode) {
                    chosenNode = node;
                    min = totalSplitCount;
                }
            }
            if (chosenNode == null) {
                // min is guaranteed to be MAX_VALUE at this line
                for (InternalNode node : candidateNodes) {
                    int totalSplitCount = assignmentStats.getQueuedSplitCountForStage(node);
                    if (totalSplitCount < min && totalSplitCount < maxPendingSplitsPerTask) {
                        chosenNode = node;
                        min = totalSplitCount;
                    }
                }
            }
            if (chosenNode != null) {
                assignment.put(chosenNode, split);
                assignmentStats.addAssignedSplit(chosenNode);
            }
            else {
                if (split.isRemotelyAccessible()) {
                    splitWaitingForAnyNode = true;
                }
                // Exact node set won't matter, if a split is waiting for any node
                else if (!splitWaitingForAnyNode) {
                    blockedExactNodes.addAll(candidateNodes);
                }
            }
        }

        ListenableFuture<?> blocked;
        if (splitWaitingForAnyNode) {
            blocked = toWhenHasSplitQueueSpaceFuture(existingTasks, calculateLowWatermark(maxPendingSplitsPerTask));
        }
        else {
            blocked = toWhenHasSplitQueueSpaceFuture(blockedExactNodes, existingTasks, calculateLowWatermark(maxPendingSplitsPerTask));
        }

        if (splitsToBeRedistributed) {
            equateDistribution(assignment, assignmentStats);
        }

        return new SplitPlacementResult(blocked, assignment);
    }

    /**
     * Calculate the standard deviation of the distribution of splits over the worker nodes
     */
    private double stdDev(NodeAssignmentStats assignmentStats)
    {
        double[] array = new double[allNodes().size()];
        int i = 0;
        for (InternalNode node : allNodes()) {
            array[i] = assignmentStats.getTotalSplitCount(node);
            i++;
        }
        return new StandardDeviation(false).evaluate(array);
    }

    /**
     * The method tries to make the distribution of splits more uniform. It checks for the standard deviation and
     * redistributes splits from the node with maximum splits to the one with the minimum splits until the minimum
     * possible value of standard deviation is obtained
     * @param assignment the node-splits multimap after the first and the second stage
     * @param assignmentStats required to obtain info regarding splits assigned to a node outside the current batch of assignment
     * with no changes if it is empty or if force-local-scheduling is enabled
     */
    private void equateDistribution(Multimap<InternalNode, Split> assignment, NodeAssignmentStats assignmentStats)
    {
        if (assignment.isEmpty()) {
            return;
        }
        // prevStdDev initialized with max value to enter the loop
        double prevStdDev = Double.MAX_VALUE;
        double newStdDev = stdDev(assignmentStats);
        while (newStdDev < prevStdDev) {
            redistributeSplits(assignment, assignmentStats);
            prevStdDev = newStdDev;
            newStdDev = stdDev(assignmentStats);
        }
    }

    /**
     * The method selects and removes a split from the maxNode and assigns it to the minNode. There is an attempt to
     * redistribute a Non-local split. This case is possible when there are multiple queries running simultaneously.
     * If a Non-local split cannot be found in the maxNode, any split is selected and reassigned.
     */
    void redistributeSingleSplit(Multimap<InternalNode, Split> assignment, InternalNode maxNode, InternalNode minNode)
    {
        Iterator<Split> splitIterator = assignment.get(maxNode).iterator();
        Split splitToBeRedistributed = null;
        while (splitIterator.hasNext()) {
            Split split = splitIterator.next();
            // Try to select non-local split for redistribution
            if (!split.getAddresses().isEmpty() && !split.getAddresses().contains(HostAddress.fromString(maxNode.getHostAndPort().getHostText()))) {
                splitToBeRedistributed = split;
                break;
            }
        }
        // Select any split if maxNode has no non-local splits in the current batch of assignment
        if (splitToBeRedistributed == null) {
            splitIterator = assignment.get(maxNode).iterator();
            splitToBeRedistributed = splitIterator.next();
        }
        splitIterator.remove();
        assignment.put(minNode, splitToBeRedistributed);
    }

    /**
     * The method modifies the assignment multimap by transferring a split from a maxNode to a minNode. The maxNode is
     * the node which has the most total number of splits allotted to it while having atleast one split in the current
     * batch of assignment. The minNode has the least total number of splits assigned. If the difference in the number
     * of splits assigned to maxNode and minNode is greater than 1, there is an attempt to select a non-local split
     * assigned to maxNode.
     */
    private void redistributeSplits(Multimap<InternalNode, Split> assignment, NodeAssignmentStats assignmentStats)
    {
        int max = Integer.MIN_VALUE;
        InternalNode maxNode = null;
        int min = Integer.MAX_VALUE;
        InternalNode minNode = null;
        for (InternalNode node : allNodes()) {
            if ((assignmentStats.getTotalSplitCount(node) > max && assignment.containsKey(node)) && assignment.get(node).size() > 0) {
                max = assignmentStats.getTotalSplitCount(node);
                maxNode = node;
            }
            if (assignmentStats.getTotalSplitCount(node) < min) {
                min = assignmentStats.getTotalSplitCount(node);
                minNode = node;
            }
        }
        // If maxNode is not null and the difference between the number of splits between maxNode and minNode is greater than 1
        if (maxNode != null && (max - min) > 1) {
            redistributeSingleSplit(assignment, maxNode, minNode);
            assignmentStats.removeAssignedSplit(maxNode);
            assignmentStats.addAssignedSplit(minNode);
        }
    }
}
