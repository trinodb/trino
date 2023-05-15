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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.trino.Session;
import io.trino.execution.NodeTaskMap;
import io.trino.execution.RemoteTask;
import io.trino.metadata.InternalNode;
import io.trino.metadata.Split;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.CatalogHandle;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.whenAnyCompleteCancelOthers;
import static java.lang.Math.addExact;
import static java.util.Objects.requireNonNull;

public class NodeScheduler
{
    private final NodeSelectorFactory nodeSelectorFactory;

    @Inject
    public NodeScheduler(NodeSelectorFactory nodeSelectorFactory)
    {
        this.nodeSelectorFactory = requireNonNull(nodeSelectorFactory, "nodeSelectorFactory is null");
    }

    public NodeSelector createNodeSelector(Session session, Optional<CatalogHandle> catalogHandle)
    {
        return nodeSelectorFactory.createNodeSelector(requireNonNull(session, "session is null"), requireNonNull(catalogHandle, "catalogHandle is null"));
    }

    public static List<InternalNode> getAllNodes(NodeMap nodeMap, boolean includeCoordinator)
    {
        return nodeMap.getNodesByHostAndPort().values().stream()
                .filter(node -> includeCoordinator || !nodeMap.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                .collect(toImmutableList());
    }

    public static List<InternalNode> selectNodes(int limit, Iterator<InternalNode> candidates)
    {
        checkArgument(limit > 0, "limit must be at least 1");

        List<InternalNode> selected = new ArrayList<>(limit);
        while (selected.size() < limit && candidates.hasNext()) {
            selected.add(candidates.next());
        }

        return selected;
    }

    public static ResettableRandomizedIterator<InternalNode> randomizedNodes(NodeMap nodeMap, boolean includeCoordinator, Set<InternalNode> excludedNodes)
    {
        return new ResettableRandomizedIterator<>(filterNodes(nodeMap, includeCoordinator, excludedNodes));
    }

    public static List<InternalNode> filterNodes(NodeMap nodeMap, boolean includeCoordinator, Set<InternalNode> excludedNodes)
    {
        return nodeMap.getNodesByHostAndPort().values().stream()
                .filter(node -> includeCoordinator || !nodeMap.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                .filter(node -> !excludedNodes.contains(node))
                .collect(toImmutableList());
    }

    public static List<InternalNode> selectExactNodes(NodeMap nodeMap, List<HostAddress> hosts, boolean includeCoordinator)
    {
        Set<InternalNode> chosen = new LinkedHashSet<>();
        Set<String> coordinatorIds = nodeMap.getCoordinatorNodeIds();

        for (HostAddress host : hosts) {
            nodeMap.getNodesByHostAndPort().get(host).stream()
                    .filter(node -> includeCoordinator || !coordinatorIds.contains(node.getNodeIdentifier()))
                    .forEach(chosen::add);

            InetAddress address;
            try {
                address = host.toInetAddress();
            }
            catch (UnknownHostException e) {
                // skip hosts that don't resolve
                continue;
            }

            // consider a split with a host without a port as being accessible by all nodes in that host
            if (!host.hasPort()) {
                nodeMap.getNodesByHost().get(address).stream()
                        .filter(node -> includeCoordinator || !coordinatorIds.contains(node.getNodeIdentifier()))
                        .forEach(chosen::add);
            }
        }

        // if the chosen set is empty and the host is the coordinator, force pick the coordinator
        if (chosen.isEmpty() && !includeCoordinator) {
            for (HostAddress host : hosts) {
                // In the code below, before calling `chosen::add`, it could have been checked that
                // `coordinatorIds.contains(node.getNodeIdentifier())`. But checking the condition isn't necessary
                // because every node satisfies it. Otherwise, `chosen` wouldn't have been empty.

                chosen.addAll(nodeMap.getNodesByHostAndPort().get(host));

                InetAddress address;
                try {
                    address = host.toInetAddress();
                }
                catch (UnknownHostException e) {
                    // skip hosts that don't resolve
                    continue;
                }

                // consider a split with a host without a port as being accessible by all nodes in that host
                if (!host.hasPort()) {
                    chosen.addAll(nodeMap.getNodesByHost().get(address));
                }
            }
        }

        return ImmutableList.copyOf(chosen);
    }

    public static SplitPlacementResult selectDistributionNodes(
            NodeMap nodeMap,
            NodeTaskMap nodeTaskMap,
            long maxSplitsWeightPerNode,
            long minPendingSplitsWeightPerTask,
            int maxUnacknowledgedSplitsPerTask,
            Set<Split> splits,
            List<RemoteTask> existingTasks,
            BucketNodeMap bucketNodeMap)
    {
        Multimap<InternalNode, Split> assignments = HashMultimap.create();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        Set<InternalNode> blockedNodes = new HashSet<>();
        for (Split split : splits) {
            // node placement is forced by the bucket to node map
            InternalNode node = bucketNodeMap.getAssignedNode(split);
            SplitWeight splitWeight = split.getSplitWeight();

            // if node is full, don't schedule now, which will push back on the scheduling of splits
            if (canAssignSplitToDistributionNode(assignmentStats, node, maxSplitsWeightPerNode, minPendingSplitsWeightPerTask, maxUnacknowledgedSplitsPerTask, splitWeight)) {
                assignments.put(node, split);
                assignmentStats.addAssignedSplit(node, splitWeight);
            }
            else {
                blockedNodes.add(node);
            }
        }

        ListenableFuture<Void> blocked = toWhenHasSplitQueueSpaceFuture(blockedNodes, existingTasks, calculateLowWatermark(minPendingSplitsWeightPerTask));
        return new SplitPlacementResult(blocked, ImmutableMultimap.copyOf(assignments));
    }

    private static boolean canAssignSplitToDistributionNode(NodeAssignmentStats assignmentStats, InternalNode node, long maxSplitsWeightPerNode, long minPendingSplitsWeightPerTask, int maxUnacknowledgedSplitsPerTask, SplitWeight splitWeight)
    {
        return assignmentStats.getUnacknowledgedSplitCountForStage(node) < maxUnacknowledgedSplitsPerTask &&
                (canAssignSplitBasedOnWeight(assignmentStats.getTotalSplitsWeight(node), maxSplitsWeightPerNode, splitWeight) ||
                        canAssignSplitBasedOnWeight(assignmentStats.getQueuedSplitsWeightForStage(node), minPendingSplitsWeightPerTask, splitWeight));
    }

    public static boolean canAssignSplitBasedOnWeight(long currentWeight, long weightLimit, SplitWeight splitWeight)
    {
        // Nodes or tasks that are configured to accept any splits (ie: weightLimit > 0) should always accept at least one split when
        // empty (ie: currentWeight == 0) to ensure that forward progress can be made if split weights are huge
        return addExact(currentWeight, splitWeight.getRawValue()) <= weightLimit || (currentWeight == 0 && weightLimit > 0);
    }

    public static long calculateLowWatermark(long minPendingSplitsWeightPerTask)
    {
        return (long) Math.ceil(minPendingSplitsWeightPerTask * 0.5);
    }

    public static ListenableFuture<Void> toWhenHasSplitQueueSpaceFuture(Set<InternalNode> blockedNodes, List<RemoteTask> existingTasks, long weightSpaceThreshold)
    {
        if (blockedNodes.isEmpty()) {
            return immediateVoidFuture();
        }
        Map<String, RemoteTask> nodeToTaskMap = new HashMap<>();
        for (RemoteTask task : existingTasks) {
            nodeToTaskMap.put(task.getNodeId(), task);
        }
        List<ListenableFuture<Void>> blockedFutures = blockedNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .map(nodeToTaskMap::get)
                .filter(Objects::nonNull)
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(weightSpaceThreshold))
                .collect(toImmutableList());
        if (blockedFutures.isEmpty()) {
            return immediateVoidFuture();
        }
        return asVoid(whenAnyCompleteCancelOthers(blockedFutures));
    }

    public static ListenableFuture<Void> toWhenHasSplitQueueSpaceFuture(List<RemoteTask> existingTasks, long weightSpaceThreshold)
    {
        if (existingTasks.isEmpty()) {
            return immediateVoidFuture();
        }
        List<ListenableFuture<Void>> stateChangeFutures = existingTasks.stream()
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(weightSpaceThreshold))
                .collect(toImmutableList());
        return asVoid(whenAnyCompleteCancelOthers(stateChangeFutures));
    }

    private static <T> ListenableFuture<Void> asVoid(ListenableFuture<T> future)
    {
        return Futures.transform(future, v -> null, directExecutor());
    }
}
