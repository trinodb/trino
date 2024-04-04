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
package io.trino.execution.scheduler.faulttolerant;

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.execution.TaskId;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.MemoryInfo;
import io.trino.memory.MemoryManagerConfig;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.InternalNodeManager.NodesSnapshot;
import io.trino.spi.TrinoException;
import io.trino.spi.memory.MemoryPoolInfo;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.assertj.core.util.VisibleForTesting;

import java.time.Duration;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.EAGER_SPECULATIVE;
import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.SPECULATIVE;
import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.STANDARD;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.lang.Math.max;
import static java.lang.Thread.currentThread;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class BinPackingNodeAllocatorService
        implements NodeAllocatorService, NodeAllocator
{
    private static final Logger log = Logger.get(BinPackingNodeAllocatorService.class);

    @VisibleForTesting
    static final int PROCESS_PENDING_ACQUIRES_DELAY_SECONDS = 5;

    private final InternalNodeManager nodeManager;
    private final Supplier<Map<String, Optional<MemoryInfo>>> workerMemoryInfoSupplier;

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(2, daemonThreadsNamed("bin-packing-node-allocator"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Semaphore processSemaphore = new Semaphore(0);
    private final AtomicReference<Map<String, MemoryPoolInfo>> nodePoolMemoryInfos = new AtomicReference<>(ImmutableMap.of());
    private final boolean scheduleOnCoordinator;
    private final DataSize taskRuntimeMemoryEstimationOverhead;
    private final DataSize eagerSpeculativeTasksNodeMemoryOvercommit;
    private final Ticker ticker;

    private final Deque<PendingAcquire> pendingAcquires = new ConcurrentLinkedDeque<>();
    private final Set<BinPackingNodeLease> fulfilledAcquires = newConcurrentHashSet();
    private final Duration allowedNoMatchingNodePeriod;

    @Inject
    public BinPackingNodeAllocatorService(
            InternalNodeManager nodeManager,
            ClusterMemoryManager clusterMemoryManager,
            NodeSchedulerConfig nodeSchedulerConfig,
            MemoryManagerConfig memoryManagerConfig)
    {
        this(nodeManager,
                clusterMemoryManager::getWorkerMemoryInfo,
                nodeSchedulerConfig.isIncludeCoordinator(),
                Duration.ofMillis(nodeSchedulerConfig.getAllowedNoMatchingNodePeriod().toMillis()),
                memoryManagerConfig.getFaultTolerantExecutionTaskRuntimeMemoryEstimationOverhead(),
                memoryManagerConfig.getFaultTolerantExecutionEagerSpeculativeTasksNodeMemoryOvercommit(),
                Ticker.systemTicker());
    }

    @VisibleForTesting
    BinPackingNodeAllocatorService(
            InternalNodeManager nodeManager,
            Supplier<Map<String, Optional<MemoryInfo>>> workerMemoryInfoSupplier,
            boolean scheduleOnCoordinator,
            Duration allowedNoMatchingNodePeriod,
            DataSize taskRuntimeMemoryEstimationOverhead,
            DataSize eagerSpeculativeTasksNodeMemoryOvercommit,
            Ticker ticker)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.workerMemoryInfoSupplier = requireNonNull(workerMemoryInfoSupplier, "workerMemoryInfoSupplier is null");
        this.scheduleOnCoordinator = scheduleOnCoordinator;
        this.allowedNoMatchingNodePeriod = requireNonNull(allowedNoMatchingNodePeriod, "allowedNoMatchingNodePeriod is null");
        this.taskRuntimeMemoryEstimationOverhead = requireNonNull(taskRuntimeMemoryEstimationOverhead, "taskRuntimeMemoryEstimationOverhead is null");
        this.eagerSpeculativeTasksNodeMemoryOvercommit = eagerSpeculativeTasksNodeMemoryOvercommit;
        this.ticker = requireNonNull(ticker, "ticker is null");
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

        refreshNodePoolMemoryInfos();
        executor.scheduleWithFixedDelay(() -> {
            try {
                refreshNodePoolMemoryInfos();
            }
            catch (Throwable e) {
                // ignore to avoid getting unscheduled
                log.error(e, "Unexpected error while refreshing node pool memory infos");
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        stopped.set(true);
        executor.shutdownNow();
    }

    @VisibleForTesting
    void refreshNodePoolMemoryInfos()
    {
        ImmutableMap.Builder<String, MemoryPoolInfo> newNodePoolMemoryInfos = ImmutableMap.builder();

        Map<String, Optional<MemoryInfo>> workerMemoryInfos = workerMemoryInfoSupplier.get();
        long maxNodePoolSizeBytes = -1;
        for (Map.Entry<String, Optional<MemoryInfo>> entry : workerMemoryInfos.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }
            MemoryPoolInfo poolInfo = entry.getValue().get().getPool();
            newNodePoolMemoryInfos.put(entry.getKey(), poolInfo);
            maxNodePoolSizeBytes = Math.max(poolInfo.getMaxBytes(), maxNodePoolSizeBytes);
        }
        nodePoolMemoryInfos.set(newNodePoolMemoryInfos.buildOrThrow());
    }

    @VisibleForTesting
    synchronized void processPendingAcquires()
    {
        // Process EAGER_SPECULATIVE first; it increases the chance that tasks which have potential to end query early get scheduled to worker nodes.
        // Even though EAGER_SPECULATIVE tasks depend on upstream STANDARD tasks this logic will not lead to deadlock.
        // When processing STANDARD acquires below, we will ignore EAGER_SPECULATIVE (and SPECULATIVE) tasks when assessing if node has enough resources for processing task.
        processPendingAcquires(EAGER_SPECULATIVE);
        processPendingAcquires(STANDARD);
        boolean hasNonSpeculativePendingAcquires = pendingAcquires.stream().anyMatch(pendingAcquire -> !pendingAcquire.isSpeculative());
        if (!hasNonSpeculativePendingAcquires) {
            processPendingAcquires(SPECULATIVE);
        }
    }

    private void processPendingAcquires(TaskExecutionClass executionClass)
    {
        // synchronized only for sake manual triggering in test code. In production code it should only be called by single thread
        Iterator<PendingAcquire> iterator = pendingAcquires.iterator();

        BinPackingSimulation simulation = new BinPackingSimulation(
                nodeManager.getActiveNodesSnapshot(),
                nodePoolMemoryInfos.get(),
                fulfilledAcquires,
                scheduleOnCoordinator,
                taskRuntimeMemoryEstimationOverhead,
                executionClass == EAGER_SPECULATIVE ? eagerSpeculativeTasksNodeMemoryOvercommit : DataSize.ofBytes(0),
                executionClass == STANDARD); // if we are processing non-speculative pending acquires we are ignoring speculative acquired ones

        while (iterator.hasNext()) {
            PendingAcquire pendingAcquire = iterator.next();

            if (pendingAcquire.getFuture().isCancelled()) {
                // request aborted
                iterator.remove();
                continue;
            }

            if (pendingAcquire.getExecutionClass() != executionClass) {
                continue;
            }

            BinPackingSimulation.ReserveResult result = simulation.tryReserve(pendingAcquire);
            switch (result.getStatus()) {
                case RESERVED:
                    InternalNode reservedNode = result.getNode();
                    fulfilledAcquires.add(pendingAcquire.getLease());
                    pendingAcquire.getFuture().set(reservedNode);
                    if (pendingAcquire.getFuture().isCancelled()) {
                        // completing future was unsuccessful - request was cancelled in the meantime
                        fulfilledAcquires.remove(pendingAcquire.getLease());

                        // run once again when we are done
                        wakeupProcessPendingAcquires();
                    }
                    iterator.remove();
                    break;
                case NONE_MATCHING:
                    Duration noMatchingNodePeriod = pendingAcquire.markNoMatchingNodeFound();

                    if (noMatchingNodePeriod.compareTo(allowedNoMatchingNodePeriod) <= 0) {
                        // wait some more time
                        break;
                    }

                    pendingAcquire.getFuture().setException(new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query"));
                    iterator.remove();
                    break;
                case NOT_ENOUGH_RESOURCES_NOW:
                    pendingAcquire.resetNoMatchingNodeFound();
                    break; // nothing to be done
                default:
                    throw new IllegalArgumentException("unknown status: " + result.getStatus());
            }
        }
    }

    private void wakeupProcessPendingAcquires()
    {
        processSemaphore.release();
    }

    @Override
    public NodeAllocator getNodeAllocator(Session session)
    {
        return this;
    }

    @Override
    public NodeLease acquire(NodeRequirements nodeRequirements, DataSize memoryRequirement, TaskExecutionClass executionClass)
    {
        BinPackingNodeLease nodeLease = new BinPackingNodeLease(memoryRequirement.toBytes(), executionClass);
        PendingAcquire pendingAcquire = new PendingAcquire(nodeRequirements, nodeLease, ticker);
        pendingAcquires.add(pendingAcquire);
        wakeupProcessPendingAcquires();
        return nodeLease;
    }

    @Override
    public void close()
    {
        // nothing to do here. leases should be released by the calling party.
        // TODO would be great to be able to validate if it actually happened but close() is called from SqlQueryScheduler code
        //      and that can be done before all leases are yet returned from running (soon to be failed) tasks.
    }

    private static class PendingAcquire
    {
        private final NodeRequirements nodeRequirements;
        private final BinPackingNodeLease lease;
        private final Stopwatch noMatchingNodeStopwatch;

        private PendingAcquire(NodeRequirements nodeRequirements, BinPackingNodeLease lease, Ticker ticker)
        {
            this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
            this.lease = requireNonNull(lease, "lease is null");
            this.noMatchingNodeStopwatch = Stopwatch.createUnstarted(ticker);
        }

        public NodeRequirements getNodeRequirements()
        {
            return nodeRequirements;
        }

        public BinPackingNodeLease getLease()
        {
            return lease;
        }

        public SettableFuture<InternalNode> getFuture()
        {
            return lease.getNodeSettableFuture();
        }

        public long getMemoryLease()
        {
            return lease.getMemoryLease();
        }

        public Duration markNoMatchingNodeFound()
        {
            if (!noMatchingNodeStopwatch.isRunning()) {
                noMatchingNodeStopwatch.start();
            }
            return noMatchingNodeStopwatch.elapsed();
        }

        public void resetNoMatchingNodeFound()
        {
            noMatchingNodeStopwatch.reset();
        }

        public boolean isSpeculative()
        {
            return lease.isSpeculative();
        }

        public TaskExecutionClass getExecutionClass()
        {
            return lease.getExecutionClass();
        }
    }

    private class BinPackingNodeLease
            implements NodeAllocator.NodeLease
    {
        private final SettableFuture<InternalNode> node = SettableFuture.create();
        private final AtomicBoolean released = new AtomicBoolean();
        private final AtomicLong memoryLease;
        private final AtomicReference<TaskId> taskId = new AtomicReference<>();
        private final AtomicReference<TaskExecutionClass> executionClass;

        private BinPackingNodeLease(long memoryLease, TaskExecutionClass executionClass)
        {
            this.memoryLease = new AtomicLong(memoryLease);
            requireNonNull(executionClass, "executionClass is null");
            this.executionClass = new AtomicReference<>(executionClass);
        }

        @Override
        public ListenableFuture<InternalNode> getNode()
        {
            return node;
        }

        InternalNode getAssignedNode()
        {
            try {
                return Futures.getDone(node);
            }
            catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        SettableFuture<InternalNode> getNodeSettableFuture()
        {
            return node;
        }

        @Override
        public void attachTaskId(TaskId taskId)
        {
            if (!this.taskId.compareAndSet(null, taskId)) {
                throw new IllegalStateException("cannot attach taskId " + taskId + "; already attached to " + this.taskId.get());
            }
        }

        @Override
        public void setExecutionClass(TaskExecutionClass newExecutionClass)
        {
            TaskExecutionClass changedFrom = this.executionClass.getAndUpdate(oldExecutionClass -> {
                checkArgument(oldExecutionClass.canTransitionTo(newExecutionClass), "cannot change execution class from %s to %s", oldExecutionClass, newExecutionClass);
                return newExecutionClass;
            });

            if (changedFrom != newExecutionClass) {
                wakeupProcessPendingAcquires();
            }
        }

        public boolean isSpeculative()
        {
            return executionClass.get().isSpeculative();
        }

        public TaskExecutionClass getExecutionClass()
        {
            return executionClass.get();
        }

        public Optional<TaskId> getAttachedTaskId()
        {
            return Optional.ofNullable(this.taskId.get());
        }

        @Override
        public void setMemoryRequirement(DataSize memoryRequirement)
        {
            long newBytes = memoryRequirement.toBytes();
            long previousBytes = memoryLease.getAndSet(newBytes);
            if (newBytes < previousBytes) {
                wakeupProcessPendingAcquires();
            }
        }

        public long getMemoryLease()
        {
            return memoryLease.get();
        }

        @Override
        public void release()
        {
            if (released.compareAndSet(false, true)) {
                node.cancel(true);
                if (node.isDone() && !node.isCancelled()) {
                    checkState(fulfilledAcquires.remove(this), "node lease %s not found in fulfilledAcquires %s", this, fulfilledAcquires);
                    wakeupProcessPendingAcquires();
                }
            }
            else {
                throw new IllegalStateException("Node " + node + " already released");
            }
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("node", node)
                    .add("released", released)
                    .add("memoryLease", memoryLease)
                    .add("taskId", taskId)
                    .add("executionClass", executionClass)
                    .toString();
        }
    }

    private static class BinPackingSimulation
    {
        private final NodesSnapshot nodesSnapshot;
        private final List<InternalNode> allNodesSorted;
        private final boolean ignoreAcquiredSpeculative;
        private final Map<String, Long> nodesRemainingMemory;
        private final Map<String, Long> nodesRemainingMemoryRuntimeAdjusted;
        private final Map<String, Long> speculativeMemoryReserved;

        private final Map<String, MemoryPoolInfo> nodeMemoryPoolInfos;
        private final boolean scheduleOnCoordinator;

        public BinPackingSimulation(
                NodesSnapshot nodesSnapshot,
                Map<String, MemoryPoolInfo> nodeMemoryPoolInfos,
                Set<BinPackingNodeLease> fulfilledAcquires,
                boolean scheduleOnCoordinator,
                DataSize taskRuntimeMemoryEstimationOverhead,
                DataSize nodeMemoryOvercommit,
                boolean ignoreAcquiredSpeculative)
        {
            this.nodesSnapshot = requireNonNull(nodesSnapshot, "nodesSnapshot is null");
            // use same node ordering for each simulation
            this.allNodesSorted = nodesSnapshot.getAllNodes().stream()
                    .sorted(comparing(InternalNode::getNodeIdentifier))
                    .collect(toImmutableList());

            this.ignoreAcquiredSpeculative = ignoreAcquiredSpeculative;

            requireNonNull(nodeMemoryPoolInfos, "nodeMemoryPoolInfos is null");
            this.nodeMemoryPoolInfos = ImmutableMap.copyOf(nodeMemoryPoolInfos);

            this.scheduleOnCoordinator = scheduleOnCoordinator;

            Map<String, Map<String, Long>> realtimeTasksMemoryPerNode = new HashMap<>();
            for (InternalNode node : nodesSnapshot.getAllNodes()) {
                MemoryPoolInfo memoryPoolInfo = nodeMemoryPoolInfos.get(node.getNodeIdentifier());
                if (memoryPoolInfo == null) {
                    realtimeTasksMemoryPerNode.put(node.getNodeIdentifier(), ImmutableMap.of());
                    continue;
                }
                realtimeTasksMemoryPerNode.put(node.getNodeIdentifier(), memoryPoolInfo.getTaskMemoryReservations());
            }

            Map<String, Long> preReservedMemory = new HashMap<>();
            speculativeMemoryReserved = new HashMap<>();
            SetMultimap<String, BinPackingNodeLease> fulfilledAcquiresByNode = HashMultimap.create();
            for (BinPackingNodeLease fulfilledAcquire : fulfilledAcquires) {
                InternalNode node = fulfilledAcquire.getAssignedNode();
                long memoryLease = fulfilledAcquire.getMemoryLease();
                if (ignoreAcquiredSpeculative && fulfilledAcquire.isSpeculative()) {
                    speculativeMemoryReserved.merge(node.getNodeIdentifier(), memoryLease, Long::sum);
                }
                else {
                    fulfilledAcquiresByNode.put(node.getNodeIdentifier(), fulfilledAcquire);
                    preReservedMemory.merge(node.getNodeIdentifier(), memoryLease, Long::sum);
                }
            }

            nodesRemainingMemory = new HashMap<>();
            for (InternalNode node : nodesSnapshot.getAllNodes()) {
                MemoryPoolInfo memoryPoolInfo = nodeMemoryPoolInfos.get(node.getNodeIdentifier());
                if (memoryPoolInfo == null) {
                    nodesRemainingMemory.put(node.getNodeIdentifier(), 0L);
                    continue;
                }
                long nodeReservedMemory = preReservedMemory.getOrDefault(node.getNodeIdentifier(), 0L);
                nodesRemainingMemory.put(node.getNodeIdentifier(), max(memoryPoolInfo.getMaxBytes() + nodeMemoryOvercommit.toBytes() - nodeReservedMemory, 0L));
            }

            nodesRemainingMemoryRuntimeAdjusted = new HashMap<>();
            for (InternalNode node : nodesSnapshot.getAllNodes()) {
                MemoryPoolInfo memoryPoolInfo = nodeMemoryPoolInfos.get(node.getNodeIdentifier());
                if (memoryPoolInfo == null) {
                    nodesRemainingMemoryRuntimeAdjusted.put(node.getNodeIdentifier(), 0L);
                    continue;
                }

                Map<String, Long> realtimeNodeMemory = realtimeTasksMemoryPerNode.get(node.getNodeIdentifier());
                Set<BinPackingNodeLease> nodeFulfilledAcquires = fulfilledAcquiresByNode.get(node.getNodeIdentifier());

                long nodeUsedMemoryRuntimeAdjusted = 0;
                for (BinPackingNodeLease lease : nodeFulfilledAcquires) {
                    long realtimeTaskMemory = 0;
                    if (lease.getAttachedTaskId().isPresent()) {
                        realtimeTaskMemory = realtimeNodeMemory.getOrDefault(lease.getAttachedTaskId().get().toString(), 0L);
                        realtimeTaskMemory += taskRuntimeMemoryEstimationOverhead.toBytes();
                    }
                    long reservedTaskMemory = lease.getMemoryLease();
                    nodeUsedMemoryRuntimeAdjusted += max(realtimeTaskMemory, reservedTaskMemory);
                }

                // if globally reported memory usage of node is greater than computed one lets use that.
                // it can be greater if there are tasks executed on cluster which do not have task retries enabled.
                nodeUsedMemoryRuntimeAdjusted = max(nodeUsedMemoryRuntimeAdjusted, memoryPoolInfo.getReservedBytes());
                nodesRemainingMemoryRuntimeAdjusted.put(node.getNodeIdentifier(), max(memoryPoolInfo.getMaxBytes() + nodeMemoryOvercommit.toBytes() - nodeUsedMemoryRuntimeAdjusted, 0L));
            }
        }

        public ReserveResult tryReserve(PendingAcquire acquire)
        {
            NodeRequirements requirements = acquire.getNodeRequirements();
            Optional<Set<InternalNode>> catalogNodes = requirements.getCatalogHandle().map(nodesSnapshot::getConnectorNodes);

            List<InternalNode> candidates = allNodesSorted.stream()
                    .filter(node -> catalogNodes.isEmpty() || catalogNodes.get().contains(node))
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
                    .collect(toImmutableList());

            if (candidates.isEmpty()) {
                return ReserveResult.NONE_MATCHING;
            }

            Comparator<InternalNode> comparator = comparing(node -> nodesRemainingMemoryRuntimeAdjusted.get(node.getNodeIdentifier()));
            if (ignoreAcquiredSpeculative) {
                comparator = resolveTiesWithSpeculativeMemory(comparator);
            }
            InternalNode selectedNode = candidates.stream()
                    .max(comparator)
                    .orElseThrow();

            // result of acquire.getMemoryLease() can change; store memory as a variable, so we have consistent value through this method.
            long memoryRequirements = acquire.getMemoryLease();

            if (nodesRemainingMemoryRuntimeAdjusted.get(selectedNode.getNodeIdentifier()) >= memoryRequirements || isNodeEmpty(selectedNode.getNodeIdentifier())) {
                // there is enough unreserved memory on the node
                // OR
                // there is not enough memory available on the node but the node is empty so we cannot to better anyway

                // todo: currant logic does not handle heterogenous clusters best. There is a chance that there is a larger node in the cluster but
                //       with less memory available right now, hence that one was not selected as a candidate.
                // mark memory reservation
                subtractFromRemainingMemory(selectedNode.getNodeIdentifier(), memoryRequirements);
                return ReserveResult.reserved(selectedNode);
            }

            // If selected node cannot be used right now, select best one ignoring runtime memory usage and reserve space there
            // for later use. This is important from algorithm liveliness perspective. If we did not reserve space for a task which
            // is too big to be scheduled right now, it could be starved by smaller tasks coming later.
            Comparator<InternalNode> fallbackComparator = comparing(node -> nodesRemainingMemory.get(node.getNodeIdentifier()));
            if (ignoreAcquiredSpeculative) {
                fallbackComparator = resolveTiesWithSpeculativeMemory(fallbackComparator);
            }
            InternalNode fallbackNode = candidates.stream()
                    .max(fallbackComparator)
                    .orElseThrow();
            subtractFromRemainingMemory(fallbackNode.getNodeIdentifier(), memoryRequirements);
            return ReserveResult.NOT_ENOUGH_RESOURCES_NOW;
        }

        private Comparator<InternalNode> resolveTiesWithSpeculativeMemory(Comparator<InternalNode> comparator)
        {
            return comparator.thenComparing(node -> -speculativeMemoryReserved.getOrDefault(node.getNodeIdentifier(), 0L));
        }

        private void subtractFromRemainingMemory(String nodeIdentifier, long memoryLease)
        {
            nodesRemainingMemoryRuntimeAdjusted.compute(
                    nodeIdentifier,
                    (key, free) -> max(free - memoryLease, 0));
            nodesRemainingMemory.compute(
                    nodeIdentifier,
                    (key, free) -> max(free - memoryLease, 0));
        }

        private boolean isNodeEmpty(String nodeIdentifier)
        {
            return nodeMemoryPoolInfos.containsKey(nodeIdentifier)
                    && nodesRemainingMemory.get(nodeIdentifier).equals(nodeMemoryPoolInfos.get(nodeIdentifier).getMaxBytes());
        }

        public enum ReservationStatus
        {
            NONE_MATCHING,
            NOT_ENOUGH_RESOURCES_NOW,
            RESERVED
        }

        public static class ReserveResult
        {
            public static final ReserveResult NONE_MATCHING = new ReserveResult(ReservationStatus.NONE_MATCHING, Optional.empty());
            public static final ReserveResult NOT_ENOUGH_RESOURCES_NOW = new ReserveResult(ReservationStatus.NOT_ENOUGH_RESOURCES_NOW, Optional.empty());

            public static ReserveResult reserved(InternalNode node)
            {
                return new ReserveResult(ReservationStatus.RESERVED, Optional.of(node));
            }

            private final ReservationStatus status;
            private final Optional<InternalNode> node;

            private ReserveResult(ReservationStatus status, Optional<InternalNode> node)
            {
                this.status = requireNonNull(status, "status is null");
                this.node = requireNonNull(node, "node is null");
                checkArgument(node.isPresent() == (status == ReservationStatus.RESERVED), "node must be set iff status is RESERVED");
            }

            public ReservationStatus getStatus()
            {
                return status;
            }

            public InternalNode getNode()
            {
                return node.orElseThrow(() -> new IllegalStateException("node not set"));
            }
        }
    }
}
