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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.MemoryInfo;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.NodeState;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import org.assertj.core.util.VisibleForTesting;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.scheduler.FallbackToFullNodePartitionMemoryEstimator.FULL_NODE_MEMORY;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.lang.Math.max;
import static java.lang.Thread.currentThread;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.function.Predicate.not;

/**
 * Node allocation service which allocates nodes for tasks in two modes:
 * - binpacking tasks into nodes
 * - reserving full node for a task
 *
 * The mode selection is handled by {@link FallbackToFullNodePartitionMemoryEstimator}. Initially the task is assigned default memory requirements,
 * and binpacking mode of node allocation is used.
 * If task execution fails due to out-of-memory error next time full node is requested for the task.
 *
 * It is possible to configure limit of how many full nodes can be assigned for a give query in the same time.
 * Limit may be expressed as absolute value and as fraction of all the nodes in the cluster.
 */
@ThreadSafe
public class FullNodeCapableNodeAllocatorService
        implements NodeAllocatorService
{
    private static final Logger log = Logger.get(FullNodeCapableNodeAllocatorService.class);

    @VisibleForTesting
    static final int PROCESS_PENDING_ACQUIRES_DELAY_SECONDS = 5;

    private final InternalNodeManager nodeManager;
    private final Supplier<Map<String, Optional<MemoryInfo>>> workerMemoryInfoSupplier;
    private final int maxAbsoluteFullNodesPerQuery;
    private final double maxFractionFullNodesPerQuery;

    private final List<PendingAcquire> sharedPendingAcquires = new LinkedList<>();
    private final Map<InternalNode, PendingAcquire> fullNodePendingAcquires = new HashMap<>();
    private final Deque<PendingAcquire> detachedFullNodePendingAcquires = new ArrayDeque<>();

    private final ConcurrentMap<InternalNode, Long> sharedAllocatedMemory = new ConcurrentHashMap<>();
    private final Set<InternalNode> allocatedFullNodes = new HashSet<>();

    private final Multimap<QueryId, InternalNode> fullNodesByQueryId = HashMultimap.create(); // both assigned pending and allocated

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2, daemonThreadsNamed("bin-packing-node-allocator"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Semaphore processSemaphore = new Semaphore(0);
    private final ConcurrentMap<String, Long> nodePoolSizes = new ConcurrentHashMap<>();
    private final AtomicLong maxNodePoolSize = new AtomicLong(FULL_NODE_MEMORY.toBytes());
    private final boolean scheduleOnCoordinator;

    @Inject
    public FullNodeCapableNodeAllocatorService(
            InternalNodeManager nodeManager,
            ClusterMemoryManager clusterMemoryManager,
            NodeSchedulerConfig config)
    {
        this(nodeManager,
                requireNonNull(clusterMemoryManager, "clusterMemoryManager is null")::getWorkerMemoryInfo,
                config.getMaxAbsoluteFullNodesPerQuery(),
                config.getMaxFractionFullNodesPerQuery(),
                config.isIncludeCoordinator());
    }

    @VisibleForTesting
    FullNodeCapableNodeAllocatorService(
            InternalNodeManager nodeManager,
            Supplier<Map<String, Optional<MemoryInfo>>> workerMemoryInfoSupplier,
            int maxAbsoluteFullNodesPerQuery,
            double maxFractionFullNodesPerQuery,
            boolean scheduleOnCoordinator)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.workerMemoryInfoSupplier = requireNonNull(workerMemoryInfoSupplier, "workerMemoryInfoSupplier is null");
        this.maxAbsoluteFullNodesPerQuery = maxAbsoluteFullNodesPerQuery;
        this.maxFractionFullNodesPerQuery = maxFractionFullNodesPerQuery;
        this.scheduleOnCoordinator = scheduleOnCoordinator;
    }

    private void refreshNodePoolSizes()
    {
        Map<String, Optional<MemoryInfo>> workerMemoryInfo = workerMemoryInfoSupplier.get();
        for (String key : nodePoolSizes.keySet()) {
            if (!workerMemoryInfo.containsKey(key)) {
                nodePoolSizes.remove(key);
            }
        }

        long tmpMaxNodePoolSize = 0;
        for (Map.Entry<String, Optional<MemoryInfo>> entry : workerMemoryInfo.entrySet()) {
            Optional<MemoryInfo> memoryInfo = entry.getValue();
            if (memoryInfo.isEmpty()) {
                continue;
            }
            long nodePoolSize = memoryInfo.get().getPool().getMaxBytes();
            nodePoolSizes.put(entry.getKey(), nodePoolSize);
            tmpMaxNodePoolSize = max(tmpMaxNodePoolSize, nodePoolSize);
        }
        if (tmpMaxNodePoolSize == 0) {
            tmpMaxNodePoolSize = FULL_NODE_MEMORY.toBytes();
        }
        maxNodePoolSize.set(tmpMaxNodePoolSize);
    }

    private Optional<Long> getNodePoolSize(InternalNode internalNode)
    {
        return Optional.ofNullable(nodePoolSizes.get(internalNode.getNodeIdentifier()));
    }

    @PostConstruct
    public void start()
    {
        if (started.compareAndSet(false, true)) {
            executor.schedule(() -> {
                while (!stopped.get()) {
                    try {
                        // pending acquires are processed when node is released (semaphore is bumped) and periodically (every couple seconds)
                        // in case node list in cluster have changed.
                        processSemaphore.tryAcquire(PROCESS_PENDING_ACQUIRES_DELAY_SECONDS, TimeUnit.SECONDS);
                        processSemaphore.drainPermits();
                        processPendingAcquires();
                    }
                    catch (InterruptedException e) {
                        currentThread().interrupt();
                    }
                    catch (Exception e) {
                        // ignore to avoid getting unscheduled
                        log.warn(e, "Error updating nodes");
                    }
                }
            }, 0, TimeUnit.SECONDS);
        }

        refreshNodePoolSizes();
        executor.scheduleWithFixedDelay(this::refreshNodePoolSizes, 1, 1, TimeUnit.SECONDS);
    }

    @VisibleForTesting
    void wakeupProcessPendingAcquires()
    {
        processSemaphore.release();
    }

    @VisibleForTesting
    void processPendingAcquires()
    {
        processFullNodePendingAcquires();
        processSharedPendingAcquires();
    }

    private void processSharedPendingAcquires()
    {
        Map<PendingAcquire, InternalNode> assignedNodes = new IdentityHashMap<>();
        Map<PendingAcquire, RuntimeException> failures = new IdentityHashMap<>();
        synchronized (this) {
            Iterator<PendingAcquire> iterator = sharedPendingAcquires.iterator();
            while (iterator.hasNext()) {
                PendingAcquire pendingAcquire = iterator.next();
                if (pendingAcquire.getFuture().isCancelled()) {
                    iterator.remove();
                    continue;
                }
                if (pendingAcquire.getNodeRequirements().getMemory().toBytes() > maxNodePoolSize.get()) {
                    // nodes in the cluster shrank and what used to be a request for a shared node now is a request for full node
                    iterator.remove();
                    detachedFullNodePendingAcquires.add(pendingAcquire);
                    continue;
                }

                try {
                    Candidates candidates = selectCandidates(pendingAcquire.getNodeRequirements());
                    if (candidates.isEmpty()) {
                        throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                    }

                    Optional<InternalNode> node = tryAcquireSharedNode(candidates, pendingAcquire.getMemoryLease());
                    if (node.isPresent()) {
                        iterator.remove();
                        assignedNodes.put(pendingAcquire, node.get());
                    }
                }
                catch (RuntimeException e) {
                    iterator.remove();
                    failures.put(pendingAcquire, e);
                }
            }
        }

        // complete futures outside of synchronized section
        checkState(!Thread.holdsLock(this), "Cannot complete node futures under lock");
        assignedNodes.forEach((pendingAcquire, node) -> {
            SettableFuture<InternalNode> future = pendingAcquire.getFuture();
            future.set(node);
            if (future.isCancelled()) {
                releaseSharedNode(node, pendingAcquire.getMemoryLease());
            }
        });

        failures.forEach((pendingAcquire, failure) -> {
            SettableFuture<InternalNode> future = pendingAcquire.getFuture();
            future.setException(failure);
        });
    }

    private void processFullNodePendingAcquires()
    {
        Map<PendingAcquire, InternalNode> assignedNodes = new IdentityHashMap<>();
        Map<PendingAcquire, RuntimeException> failures = new IdentityHashMap<>();

        synchronized (this) {
            Iterator<PendingAcquire> detachedIterator = detachedFullNodePendingAcquires.iterator();
            while (detachedIterator.hasNext()) {
                PendingAcquire pendingAcquire = detachedIterator.next();
                try {
                    if (pendingAcquire.getFuture().isCancelled()) {
                        // discard cancelled detached pendingAcquire
                        detachedIterator.remove();
                        continue;
                    }

                    Candidates currentCandidates = selectCandidates(pendingAcquire.getNodeRequirements());
                    if (currentCandidates.isEmpty()) {
                        throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                    }
                    Optional<InternalNode> target = findTargetPendingFullNode(pendingAcquire.getQueryId(), currentCandidates);
                    if (target.isEmpty()) {
                        // leave pendingAcquire as pending
                        continue;
                    }

                    // move pendingAcquire to fullNodePendingAcquires
                    fullNodePendingAcquires.put(target.get(), pendingAcquire);
                    fullNodesByQueryId.put(pendingAcquire.getQueryId(), target.get());

                    detachedIterator.remove();
                }
                catch (RuntimeException e) {
                    failures.put(pendingAcquire, e);
                    detachedIterator.remove();
                }
            }

            Set<InternalNode> nodes = ImmutableSet.copyOf(fullNodePendingAcquires.keySet());
            for (InternalNode reservedNode : nodes) {
                PendingAcquire pendingAcquire = fullNodePendingAcquires.get(reservedNode);
                if (pendingAcquire.getFuture().isCancelled()) {
                    // discard cancelled pendingAcquire with target node
                    fullNodePendingAcquires.remove(reservedNode);
                    verify(fullNodesByQueryId.remove(pendingAcquire.getQueryId(), reservedNode));
                    continue;
                }
                try {
                    Candidates currentCandidates = selectCandidates(pendingAcquire.getNodeRequirements());
                    if (currentCandidates.isEmpty()) {
                        throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                    }

                    if (sharedAllocatedMemory.getOrDefault(reservedNode, 0L) > 0 || allocatedFullNodes.contains(reservedNode)) {
                        // reserved node is still used - opportunistic check if maybe there is some other empty, not waited for node available
                        Optional<InternalNode> opportunisticNode = currentCandidates.getCandidates().stream()
                                .filter(node -> !fullNodePendingAcquires.containsKey(node))
                                .filter(node -> !allocatedFullNodes.contains(node))
                                .filter(node -> sharedAllocatedMemory.getOrDefault(node, 0L) == 0)
                                .findFirst();

                        if (opportunisticNode.isPresent()) {
                            fullNodePendingAcquires.remove(reservedNode);
                            verify(fullNodesByQueryId.remove(pendingAcquire.getQueryId(), reservedNode));
                            allocatedFullNodes.add(opportunisticNode.get());
                            verify(fullNodesByQueryId.put(pendingAcquire.getQueryId(), opportunisticNode.get()));
                            assignedNodes.put(pendingAcquire, opportunisticNode.get());
                        }
                        continue;
                    }

                    if (!currentCandidates.getCandidates().contains(reservedNode)) {
                        // current candidate is gone; move pendingAcquire to detached state
                        detachedFullNodePendingAcquires.add(pendingAcquire);
                        fullNodePendingAcquires.remove(reservedNode);
                        verify(fullNodesByQueryId.remove(pendingAcquire.getQueryId(), reservedNode));
                        // trigger one more round of processing immediately
                        wakeupProcessPendingAcquires();
                        continue;
                    }

                    // we are good acquiring reserved full node
                    allocatedFullNodes.add(reservedNode);
                    fullNodePendingAcquires.remove(reservedNode);
                    assignedNodes.put(pendingAcquire, reservedNode);
                }
                catch (RuntimeException e) {
                    failures.put(pendingAcquire, e);
                    fullNodePendingAcquires.remove(reservedNode);
                    fullNodesByQueryId.remove(pendingAcquire.getQueryId(), reservedNode);
                }
            }
        }

        // complete futures outside of synchronized section
        checkState(!Thread.holdsLock(this), "Cannot complete node futures under lock");
        assignedNodes.forEach((pendingAcquire, node) -> {
            SettableFuture<InternalNode> future = pendingAcquire.getFuture();
            future.set(node);
            if (future.isCancelled()) {
                releaseFullNode(node, pendingAcquire.getQueryId());
            }
        });

        failures.forEach((pendingAcquire, failure) -> {
            SettableFuture<InternalNode> future = pendingAcquire.getFuture();
            future.setException(failure);
        });
    }

    @PreDestroy
    public void stop()
    {
        stopped.set(true);
        executor.shutdownNow();
    }

    public synchronized Optional<InternalNode> tryAcquire(NodeRequirements requirements, Candidates candidates, QueryId queryId)
    {
        if (isFullNode(requirements)) { // todo
            return tryAcquireFullNode(candidates, queryId);
        }
        return tryAcquireSharedNode(candidates, requirements.getMemory().toBytes());
    }

    @VisibleForTesting
    synchronized Set<InternalNode> getPendingFullNodes()
    {
        return ImmutableSet.copyOf(fullNodePendingAcquires.keySet());
    }

    private synchronized Optional<InternalNode> tryAcquireFullNode(Candidates candidates, QueryId queryId)
    {
        Collection<InternalNode> queryFullNodes = fullNodesByQueryId.get(queryId);

        if (fullNodesCountExceeded(queryFullNodes.size(), candidates.getAllNodesCount())) {
            return Optional.empty();
        }

        // select nodes which are not used nor waited for
        Optional<InternalNode> selectedNode = candidates.getCandidates().stream()
                .filter(node -> getNodePoolSize(node).isPresent()) // filter out nodes without memory pool information
                .filter(node -> sharedAllocatedMemory.getOrDefault(node, 0L) == 0)
                .filter(node -> !allocatedFullNodes.contains(node))
                .filter(node -> !fullNodePendingAcquires.containsKey(node))
                .findFirst();

        selectedNode.ifPresent(node -> {
            allocatedFullNodes.add(node);
            fullNodesByQueryId.put(queryId, node);
        });

        return selectedNode;
    }

    private boolean fullNodesCountExceeded(int currentCount, int candidatesCount)
    {
        long threshold = Integer.min(maxAbsoluteFullNodesPerQuery, (int) (candidatesCount * maxFractionFullNodesPerQuery));
        return currentCount >= threshold;
    }

    private synchronized Optional<InternalNode> tryAcquireSharedNode(Candidates candidates, long memoryLease)
    {
        Optional<InternalNode> selectedNode = candidates.getCandidates().stream()
                .filter(node -> getNodePoolSize(node).map(poolSize -> poolSize - sharedAllocatedMemory.getOrDefault(node, 0L) >= memoryLease).orElse(false)) // not enough memory on the node
                .filter(node -> !allocatedFullNodes.contains(node)) // node is used exclusively
                .filter(node -> !fullNodePendingAcquires.containsKey(node)) // flushing node to get exclusive use
                .min(comparing(node -> sharedAllocatedMemory.getOrDefault(node, 0L)));

        selectedNode.ifPresent(node -> sharedAllocatedMemory.merge(node, memoryLease, Long::sum));

        return selectedNode;
    }

    private synchronized PendingAcquire registerPendingAcquire(NodeRequirements requirements, Candidates candidates, QueryId queryId)
    {
        PendingAcquire pendingAcquire = new PendingAcquire(requirements, queryId);
        if (isFullNode(requirements)) {
            Optional<InternalNode> targetNode = findTargetPendingFullNode(queryId, candidates);

            if (targetNode.isEmpty()) {
                detachedFullNodePendingAcquires.add(pendingAcquire);
            }
            else {
                verify(!fullNodePendingAcquires.containsKey(targetNode.get()));
                verify(!fullNodesByQueryId.get(queryId).contains(targetNode.get()));
                fullNodePendingAcquires.put(targetNode.get(), pendingAcquire);
                fullNodesByQueryId.put(queryId, targetNode.get());
            }
        }
        else {
            sharedPendingAcquires.add(pendingAcquire);
        }
        return pendingAcquire;
    }

    private Optional<InternalNode> findTargetPendingFullNode(QueryId queryId, Candidates candidates)
    {
        // nodes which are used by are reserved for full-node use for give query
        Collection<InternalNode> queryFullNodes = fullNodesByQueryId.get(queryId);
        if (fullNodesCountExceeded(queryFullNodes.size(), candidates.getAllNodesCount())) {
            return Optional.empty();
        }
        return candidates.getCandidates().stream()
                .filter(not(queryFullNodes::contains))
                .filter(not(fullNodePendingAcquires::containsKey))
                .min(Comparator.comparing(node -> sharedAllocatedMemory.getOrDefault(node, 0L)));
    }

    private synchronized void releaseFullNode(InternalNode node, QueryId queryId)
    {
        verify(allocatedFullNodes.remove(node), "no %s node in allocatedFullNodes", node);
        verify(fullNodesByQueryId.remove(queryId, node), "no %s/%s pair in fullNodesByQueryId", queryId, node);
        wakeupProcessPendingAcquires();
    }

    private synchronized void releaseSharedNode(InternalNode node, long memoryLease)
    {
        sharedAllocatedMemory.compute(node, (key, value) -> {
            verify(value != null && value >= memoryLease, "invalid memory allocation record %s for node %s", value, key);
            long newValue = value - memoryLease;
            if (newValue > 0) {
                return newValue;
            }
            return null; // delete entry
        });
        wakeupProcessPendingAcquires();
    }

    @Override
    public NodeAllocator getNodeAllocator(Session session)
    {
        return new FullNodeCapableNodeAllocator(session);
    }

    private Candidates selectCandidates(NodeRequirements requirements)
    {
        Set<InternalNode> allNodes = getAllNodes(requirements.getCatalogName());
        return new Candidates(
                allNodes.size(),
                allNodes.stream()
                        .filter(node -> {
                            // Allow using coordinator if explicitly requested
                            if (requirements.getAddresses().contains(node.getHostAndPort())) {
                                return true;
                            }
                            if (requirements.getAddresses().isEmpty()) {
                                return scheduleOnCoordinator || !node.isCoordinator();
                            }
                            return false;
                        })
                        .collect(toImmutableList()));
    }

    private static class Candidates
    {
        private final int allNodesCount;
        private final List<InternalNode> candidates;

        public Candidates(int allNodesCount, List<InternalNode> candidates)
        {
            this.allNodesCount = allNodesCount;
            this.candidates = candidates;
        }

        public int getAllNodesCount()
        {
            return allNodesCount;
        }

        public List<InternalNode> getCandidates()
        {
            return candidates;
        }

        public boolean isEmpty()
        {
            return candidates.isEmpty();
        }
    }

    private Set<InternalNode> getAllNodes(Optional<CatalogName> catalogName)
    {
        Set<InternalNode> activeNodes;
        if (catalogName.isPresent()) {
            activeNodes = nodeManager.getActiveConnectorNodes(catalogName.get());
        }
        else {
            activeNodes = nodeManager.getNodes(NodeState.ACTIVE);
        }
        return activeNodes;
    }

    private class FullNodeCapableNodeAllocator
            implements NodeAllocator
    {
        @GuardedBy("this")
        private final Session session;

        public FullNodeCapableNodeAllocator(Session session)
        {
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public NodeLease acquire(NodeRequirements requirements)
        {
            Candidates candidates = selectCandidates(requirements);
            if (candidates.isEmpty()) {
                throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query");
            }

            QueryId queryId = session.getQueryId();

            Optional<InternalNode> selectedNode = tryAcquire(requirements, candidates, queryId);

            if (selectedNode.isPresent()) {
                return new FullNodeCapableNodeLease(
                        immediateFuture(nodeInfoForNode(selectedNode.get())),
                        requirements.getMemory().toBytes(),
                        isFullNode(requirements),
                        queryId);
            }

            PendingAcquire pendingAcquire = registerPendingAcquire(requirements, candidates, queryId);
            return new FullNodeCapableNodeLease(
                    transform(pendingAcquire.getFuture(), this::nodeInfoForNode, directExecutor()),
                    requirements.getMemory().toBytes(),
                    isFullNode(requirements),
                    queryId);
        }

        private NodeInfo nodeInfoForNode(InternalNode node)
        {
            // todo set memory limit properly
            return NodeInfo.unlimitedMemoryNode(node);
        }

        @Override
        public void close()
        {
            // nothing to do here. leases should be released by the calling party.
            // TODO would be great to be able to validate if it actually happened but close() is called from SqlQueryScheduler code
            //      and that can be done before all leases are yet returned from running (soon to be failed) tasks.
        }
    }

    private static class PendingAcquire
    {
        private final NodeRequirements nodeRequirements;
        private final SettableFuture<InternalNode> future;
        private final QueryId queryId;

        private PendingAcquire(NodeRequirements nodeRequirements, QueryId queryId)
        {
            this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.future = SettableFuture.create();
        }

        public NodeRequirements getNodeRequirements()
        {
            return nodeRequirements;
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public SettableFuture<InternalNode> getFuture()
        {
            return future;
        }

        public long getMemoryLease()
        {
            return nodeRequirements.getMemory().toBytes();
        }
    }

    private class FullNodeCapableNodeLease
            implements NodeAllocator.NodeLease
    {
        private final ListenableFuture<NodeInfo> node;
        private final AtomicBoolean released = new AtomicBoolean();
        private final long memoryLease;
        private final boolean fullNode;
        private final QueryId queryId;

        private FullNodeCapableNodeLease(ListenableFuture<NodeInfo> node, long memoryLease, boolean fullNode, QueryId queryId)
        {
            this.node = requireNonNull(node, "node is null");
            this.memoryLease = memoryLease;
            this.fullNode = fullNode;
            this.queryId = requireNonNull(queryId, "queryId is null");
        }

        @Override
        public ListenableFuture<NodeInfo> getNode()
        {
            return node;
        }

        @Override
        public void release()
        {
            if (released.compareAndSet(false, true)) {
                node.cancel(true);
                if (node.isDone() && !node.isCancelled()) {
                    if (fullNode) {
                        releaseFullNode(getFutureValue(node).getNode(), queryId);
                    }
                    else {
                        releaseSharedNode(getFutureValue(node).getNode(), memoryLease);
                    }
                }
            }
            else {
                throw new IllegalStateException("Node " + node + " already released");
            }
        }
    }

    private boolean isFullNode(NodeRequirements requirements)
    {
        return requirements.getMemory().toBytes() >= maxNodePoolSize.get();
    }
}
