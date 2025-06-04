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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.execution.TaskId;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.memory.ClusterMemoryManager;
import io.trino.memory.MemoryInfo;
import io.trino.memory.MemoryManagerConfig;
import io.trino.metadata.InternalNode;
import io.trino.metadata.InternalNodeManager;
import io.trino.metadata.InternalNodeManager.NodesSnapshot;
import io.trino.spi.HostAddress;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.memory.MemoryPoolInfo;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.assertj.core.util.VisibleForTesting;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
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
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.EAGER_SPECULATIVE;
import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.SPECULATIVE;
import static io.trino.execution.scheduler.faulttolerant.TaskExecutionClass.STANDARD;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.lang.Math.max;
import static java.lang.Thread.currentThread;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

@ThreadSafe
public class BinPackingNodeAllocatorService
        implements NodeAllocatorService
{
    private static final Logger log = Logger.get(BinPackingNodeAllocatorService.class);

    @VisibleForTesting
    static final int PROCESS_PENDING_ACQUIRES_DELAY_SECONDS = 5;

    private final InternalNodeManager nodeManager;
    private final Supplier<Map<String, Optional<MemoryInfo>>> workerMemoryInfoSupplier;

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(3, daemonThreadsNamed("bin-packing-node-allocator"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final Semaphore processSemaphore = new Semaphore(0);
    private final AtomicReference<Map<String, MemoryPoolInfo>> nodePoolMemoryInfos = new AtomicReference<>(ImmutableMap.of());
    private final boolean scheduleOnCoordinator;
    private final DataSize taskRuntimeMemoryEstimationOverhead;
    private final DataSize eagerSpeculativeTasksNodeMemoryOvercommit;
    private final Ticker ticker;

    private final ConcurrentNavigableMap<QueryId, Deque<PendingAcquire>> pendingAcquires = new ConcurrentSkipListMap<>(Ordering.natural().onResultOf(QueryId::getId));
    private final Set<BinPackingNodeLease> fulfilledAcquires = newConcurrentHashSet();
    private final Duration allowedNoMatchingNodePeriod;
    private final Duration exhaustedNodeWaitPeriod;
    private final boolean optimizedLocalScheduling;

    private final StatsHolder stats = new StatsHolder();
    private final CounterStat processCalls = new CounterStat();
    private final CounterStat processPending = new CounterStat();
    private Optional<QueryId> startingQueryId = Optional.empty();

    @Inject
    public BinPackingNodeAllocatorService(
            InternalNodeManager nodeManager,
            ClusterMemoryManager clusterMemoryManager,
            NodeSchedulerConfig nodeSchedulerConfig,
            MemoryManagerConfig memoryManagerConfig)
    {
        this(nodeManager,
                clusterMemoryManager::getAllNodesMemoryInfo,
                nodeSchedulerConfig.isIncludeCoordinator(),
                Duration.ofMillis(nodeSchedulerConfig.getAllowedNoMatchingNodePeriod().toMillis()),
                Duration.ofMillis(nodeSchedulerConfig.getExhaustedNodeWaitPeriod().toMillis()),
                nodeSchedulerConfig.getOptimizedLocalScheduling(),
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
            Duration exhaustedNodeWaitPeriod,
            boolean optimizedLocalScheduling,
            DataSize taskRuntimeMemoryEstimationOverhead,
            DataSize eagerSpeculativeTasksNodeMemoryOvercommit,
            Ticker ticker)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.workerMemoryInfoSupplier = requireNonNull(workerMemoryInfoSupplier, "workerMemoryInfoSupplier is null");
        this.scheduleOnCoordinator = scheduleOnCoordinator;
        this.allowedNoMatchingNodePeriod = requireNonNull(allowedNoMatchingNodePeriod, "allowedNoMatchingNodePeriod is null");
        this.exhaustedNodeWaitPeriod = requireNonNull(exhaustedNodeWaitPeriod, "exhaustedNodeWaitPeriod is null");
        this.optimizedLocalScheduling = optimizedLocalScheduling;
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
                    catch (Throwable e) {
                        // ignore to avoid getting unscheduled
                        log.error(e, "Error processing pending acquires");
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

        executor.scheduleWithFixedDelay(() -> {
            try {
                updateStats();
            }
            catch (Throwable e) {
                // ignore to avoid getting unscheduled
                log.error(e, "Unexpected error while updating stats");
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
        // synchronized only for sake manual triggering in test code. In production code it should only be called by single thread
        processCalls.update(1);
        // Process EAGER_SPECULATIVE first; it increases the chance that tasks which have potential to end query early get scheduled to worker nodes.
        // Even though EAGER_SPECULATIVE tasks depend on upstream STANDARD tasks this logic will not lead to deadlock.
        // When processing STANDARD acquires below, we will ignore EAGER_SPECULATIVE (and SPECULATIVE) tasks when assessing if node has enough resources for processing task.
        processPendingAcquires(EAGER_SPECULATIVE);
        processPendingAcquires(STANDARD);
        boolean hasNonSpeculativePendingAcquires = pendingAcquires.values().stream().flatMap(Collection::stream).anyMatch(pendingAcquire -> !pendingAcquire.isSpeculative());
        if (!hasNonSpeculativePendingAcquires) {
            processPendingAcquires(SPECULATIVE);
        }
    }

    private void processPendingAcquires(TaskExecutionClass executionClass)
    {
        Iterator<PendingAcquire> iterator = pendingAcquiresIterator(startingQueryId);

        BinPackingSimulation simulation = new BinPackingSimulation(
                nodeManager.getActiveNodesSnapshot(),
                nodePoolMemoryInfos.get(),
                fulfilledAcquires,
                scheduleOnCoordinator,
                optimizedLocalScheduling,
                taskRuntimeMemoryEstimationOverhead,
                executionClass == EAGER_SPECULATIVE ? eagerSpeculativeTasksNodeMemoryOvercommit : DataSize.ofBytes(0),
                executionClass == STANDARD,  // if we are processing non-speculative pending acquires we are ignoring speculative acquired ones
                exhaustedNodeWaitPeriod);

        boolean allReservedSoFar = true;
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

            processPending.update(1);
            BinPackingSimulation.ReserveResult result = simulation.tryReserve(pendingAcquire);
            pendingAcquire.setLastReservationStatus(result.getStatus());

            if (result.getStatus() != BinPackingSimulation.ReservationStatus.RESERVED && allReservedSoFar) {
                allReservedSoFar = false;
                startingQueryId = Optional.of(pendingAcquire.getQueryId());
            }

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

    private record QueryPendingAcquires(
            QueryId queryId,
            Iterator<PendingAcquire> iterator)
    {
        private QueryPendingAcquires
        {
            requireNonNull(queryId, "queryId is null");
            requireNonNull(iterator, "iterator is null");
        }
    }

    private Iterator<PendingAcquire> pendingAcquiresIterator(Optional<QueryId> startingQueryId)
    {
        if (pendingAcquires.isEmpty()) {
            return List.<PendingAcquire>of().iterator();
        }

        List<QueryPendingAcquires> iterators = pendingAcquires.entrySet().stream()
                .map(entry -> new QueryPendingAcquires(entry.getKey(), entry.getValue().iterator()))
                .collect(toCollection(ArrayList::new));

        if (iterators.isEmpty()) {
            return ImmutableList.<PendingAcquire>of().iterator();
        }

        int startingIteratorIndex = 0;
        if (startingQueryId.isPresent()) {
            startingIteratorIndex = -1;
            for (int i = 0; i < iterators.size(); i++) {
                if (iterators.get(i).queryId().equals(startingQueryId.get())) {
                    startingIteratorIndex = i;
                    break;
                }
            }
            if (startingIteratorIndex == -1) {
                startingIteratorIndex = ThreadLocalRandom.current().nextInt(iterators.size());
            }
        }

        int finalStartingIteratorIndex = startingIteratorIndex;

        return new Iterator<>()
        {
            int currentIterator = finalStartingIteratorIndex;
            int removeIterator = -1;

            @Override
            public boolean hasNext()
            {
                while (!iterators.isEmpty()) {
                    Iterator<PendingAcquire> iterator = iterators.get(currentIterator).iterator();
                    if (!iterator.hasNext()) {
                        dropCurrentIterator();
                        continue;
                    }
                    return true;
                }
                return false;
            }

            @Override
            public PendingAcquire next()
            {
                while (!iterators.isEmpty()) {
                    Iterator<PendingAcquire> iterator = iterators.get(currentIterator).iterator();
                    if (!iterator.hasNext()) {
                        dropCurrentIterator();
                        continue;
                    }
                    removeIterator = currentIterator;
                    currentIterator++;
                    currentIterator = currentIterator % iterators.size();
                    return iterator.next();
                }
                throw new NoSuchElementException();
            }

            private void dropCurrentIterator()
            {
                iterators.remove(currentIterator);
                if (currentIterator >= iterators.size()) {
                    currentIterator = 0;
                }
            }

            @Override
            public void remove()
            {
                checkState(removeIterator != -1, "next() not called or already removed");
                iterators.get(removeIterator).iterator().remove();
                removeIterator = -1;
            }
        };
    }

    private void wakeupProcessPendingAcquires()
    {
        processSemaphore.release();
    }

    @Override
    public NodeAllocator getNodeAllocator(Session session)
    {
        return new NodeAllocator() {
            @Override
            public NodeLease acquire(NodeRequirements nodeRequirements, DataSize memoryRequirement, TaskExecutionClass executionClass)
            {
                return BinPackingNodeAllocatorService.this.acquire(nodeRequirements, memoryRequirement, executionClass, session.getQueryId());
            }

            @Override
            public void close()
            {
                pendingAcquires.remove(session.getQueryId());
            }
        };
    }

    public NodeAllocator.NodeLease acquire(NodeRequirements nodeRequirements, DataSize memoryRequirement, TaskExecutionClass executionClass, QueryId queryId)
    {
        BinPackingNodeLease nodeLease = new BinPackingNodeLease(memoryRequirement.toBytes(), executionClass, nodeRequirements);
        PendingAcquire pendingAcquire = new PendingAcquire(nodeRequirements, nodeLease, queryId, ticker);
        Deque<PendingAcquire> requesterPendingAcquires = pendingAcquires.computeIfAbsent(queryId, _ -> new ConcurrentLinkedDeque<>());
        requesterPendingAcquires.add(pendingAcquire);
        wakeupProcessPendingAcquires();
        return nodeLease;
    }

    @Managed
    @Nested
    public StatsHolder getStats()
    {
        // it is required that @Managed annotated method returns same object instance every time;
        // we return mutable wrapper while we keep Stats object immutable.
        return stats;
    }

    @Managed
    @Nested
    public CounterStat getProcessCalls()
    {
        return processCalls;
    }

    @Managed
    @Nested
    public CounterStat getProcessPending()
    {
        return processPending;
    }

    private void updateStats()
    {
        long pendingStandardNoneMatching = 0;
        long pendingStandardNotEnoughResources = 0;
        long pendingStandardUnknown = 0;
        long pendingSpeculativeNoneMatching = 0;
        long pendingSpeculativeNotEnoughResources = 0;
        long pendingSpeculativeUnknown = 0;
        long pendingEagerSpeculativeNoneMatching = 0;
        long pendingEagerSpeculativeNotEnoughResources = 0;
        long pendingEagerSpeculativeUnknown = 0;
        long fulfilledStandard = 0;
        long fulfilledSpeculative = 0;
        long fulfilledEagerSpeculative = 0;

        for (Iterator<PendingAcquire> it = pendingAcquiresIterator(Optional.empty()); it.hasNext(); ) {
            PendingAcquire acquire = it.next();
            switch (acquire.getExecutionClass()) {
                case STANDARD -> {
                    switch (acquire.getLastReservationStatus()) {
                        case NONE_MATCHING -> pendingStandardNoneMatching++;
                        case NOT_ENOUGH_RESOURCES_NOW -> pendingStandardNotEnoughResources++;
                        case UNKNOWN -> pendingStandardUnknown++;
                        case RESERVED -> {} // reserved in the meantime
                    }
                }
                case SPECULATIVE -> {
                    switch (acquire.getLastReservationStatus()) {
                        case NONE_MATCHING -> pendingSpeculativeNoneMatching++;
                        case NOT_ENOUGH_RESOURCES_NOW -> pendingSpeculativeNotEnoughResources++;
                        case UNKNOWN -> pendingSpeculativeUnknown++;
                        case RESERVED -> {} // reserved in the meantime
                    }
                }
                case EAGER_SPECULATIVE -> {
                    switch (acquire.getLastReservationStatus()) {
                        case NONE_MATCHING -> pendingEagerSpeculativeNoneMatching++;
                        case NOT_ENOUGH_RESOURCES_NOW -> pendingEagerSpeculativeNotEnoughResources++;
                        case UNKNOWN -> pendingEagerSpeculativeUnknown++;
                        case RESERVED -> {} // reserved in the meantime
                    }
                }
            }
        }

        Multimap<InternalNode, BinPackingNodeLease> acquiresByNode = HashMultimap.create();
        for (BinPackingNodeLease acquire : fulfilledAcquires) {
            switch (acquire.getExecutionClass()) {
                case STANDARD -> fulfilledStandard++;
                case SPECULATIVE -> fulfilledSpeculative++;
                case EAGER_SPECULATIVE -> fulfilledEagerSpeculative++;
            }
            acquiresByNode.put(acquire.getAssignedNode(), acquire);
        }

        DistributionStat fulfilledByNodeCountDistribution = new DistributionStat();
        DistributionStat fulfilledByNodeMemoryDistribution = new DistributionStat();
        acquiresByNode.asMap().values().forEach(nodeAcquires -> {
            fulfilledByNodeCountDistribution.add(nodeAcquires.size());
            fulfilledByNodeMemoryDistribution.add(nodeAcquires.stream().mapToLong(BinPackingNodeLease::getMemoryLease).sum());
        });

        stats.updateStats(new Stats(
                pendingStandardNoneMatching,
                pendingStandardNotEnoughResources,
                pendingStandardUnknown,
                pendingSpeculativeNoneMatching,
                pendingSpeculativeNotEnoughResources,
                pendingSpeculativeUnknown,
                pendingEagerSpeculativeNoneMatching,
                pendingEagerSpeculativeNotEnoughResources,
                pendingEagerSpeculativeUnknown,
                fulfilledStandard,
                fulfilledSpeculative,
                fulfilledEagerSpeculative,
                fulfilledByNodeCountDistribution,
                fulfilledByNodeMemoryDistribution));
    }

    private static class PendingAcquire
    {
        private final NodeRequirements nodeRequirements;
        private final BinPackingNodeLease lease;
        private final QueryId queryId;
        private final Stopwatch noMatchingNodeStopwatch;
        private final Stopwatch notEnoughResourcesStopwatch;

        private volatile BinPackingSimulation.ReservationStatus lastReservationStatus = BinPackingSimulation.ReservationStatus.UNKNOWN;

        private PendingAcquire(NodeRequirements nodeRequirements, BinPackingNodeLease lease, QueryId queryId, Ticker ticker)
        {
            this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
            this.lease = requireNonNull(lease, "lease is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.noMatchingNodeStopwatch = Stopwatch.createUnstarted(ticker);
            this.notEnoughResourcesStopwatch = Stopwatch.createStarted(ticker);
        }

        public NodeRequirements getNodeRequirements()
        {
            return nodeRequirements;
        }

        public BinPackingNodeLease getLease()
        {
            return lease;
        }

        public QueryId getQueryId()
        {
            return queryId;
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

        public Duration getNotEnoughResourcesPeriod()
        {
            return notEnoughResourcesStopwatch.elapsed();
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

        public BinPackingSimulation.ReservationStatus getLastReservationStatus()
        {
            return lastReservationStatus;
        }

        public void setLastReservationStatus(BinPackingSimulation.ReservationStatus lastReservationStatus)
        {
            requireNonNull(lastReservationStatus, "lastReservationStatus is null");
            this.lastReservationStatus = lastReservationStatus;
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
        private final NodeRequirements nodeRequirements;

        private BinPackingNodeLease(long memoryLease, TaskExecutionClass executionClass, NodeRequirements nodeRequirements)
        {
            this.memoryLease = new AtomicLong(memoryLease);
            requireNonNull(executionClass, "executionClass is null");
            this.executionClass = new AtomicReference<>(executionClass);
            this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
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
                    .add("nodeRequirements", nodeRequirements)
                    .toString();
        }
    }

    private static class BinPackingSimulation
    {
        private final List<InternalNode> allNodesSorted;
        private final List<InternalNode> workerNodesSorted;
        private final Multimap<HostAddress, InternalNode> allNodesByAddress;
        private final boolean ignoreAcquiredSpeculative;
        private final Map<String, Long> nodesRemainingMemory;
        private final Set<String> nodesWithoutMemory;
        private final Map<String, Long> nodesRemainingMemoryRuntimeAdjusted;
        private final Map<String, Long> speculativeMemoryReserved;
        private final NonEvictableLoadingCache<CatalogHandle, List<InternalNode>> catalogNodes;
        private final NonEvictableLoadingCache<CatalogHandle, List<InternalNode>> catalogWorkerNodes;

        private final Map<String, MemoryPoolInfo> nodeMemoryPoolInfos;
        private final boolean scheduleOnCoordinator;
        private final boolean optimizedLocalScheduling;
        private final Duration exhaustedNodeWaitPeriod;

        public BinPackingSimulation(
                NodesSnapshot nodesSnapshot,
                Map<String, MemoryPoolInfo> nodeMemoryPoolInfos,
                Set<BinPackingNodeLease> fulfilledAcquires,
                boolean scheduleOnCoordinator,
                boolean optimizedLocalScheduling,
                DataSize taskRuntimeMemoryEstimationOverhead,
                DataSize nodeMemoryOvercommit,
                boolean ignoreAcquiredSpeculative,
                Duration exhaustedNodeWaitPeriod)
        {
            requireNonNull(nodesSnapshot, "nodesSnapshot is null");
            // use same node ordering for each simulation
            this.allNodesSorted = nodesSnapshot.getAllNodes().stream()
                    .sorted(comparing(InternalNode::getNodeIdentifier))
                    .collect(toImmutableList());

            this.workerNodesSorted = allNodesSorted.stream().filter(node -> !node.isCoordinator()).collect(toImmutableList());

            this.catalogNodes = buildNonEvictableCache(
                    CacheBuilder.newBuilder(),
                    CacheLoader.from(catalogHandle -> {
                        List<InternalNode> nodes = new ArrayList<>(allNodesSorted);
                        nodes.retainAll(nodesSnapshot.getConnectorNodes(catalogHandle));
                        return nodes;
                    }));

            this.catalogWorkerNodes = buildNonEvictableCache(
                    CacheBuilder.newBuilder(),
                    CacheLoader.from(catalogHandle -> {
                        List<InternalNode> nodes = new ArrayList<>(workerNodesSorted);
                        nodes.retainAll(nodesSnapshot.getConnectorNodes(catalogHandle));
                        return nodes;
                    }));

            allNodesByAddress = Multimaps.index(nodesSnapshot.getAllNodes(), InternalNode::getHostAndPort);

            this.ignoreAcquiredSpeculative = ignoreAcquiredSpeculative;

            requireNonNull(nodeMemoryPoolInfos, "nodeMemoryPoolInfos is null");
            this.nodeMemoryPoolInfos = ImmutableMap.copyOf(nodeMemoryPoolInfos);

            this.scheduleOnCoordinator = scheduleOnCoordinator;
            this.optimizedLocalScheduling = optimizedLocalScheduling;
            this.exhaustedNodeWaitPeriod = exhaustedNodeWaitPeriod;

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
            nodesWithoutMemory = new HashSet<>();

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
                // it can be greater if tasks exceeded the memory region assigned for them or there are
                // non FTE tasks running on cluster.
                nodeUsedMemoryRuntimeAdjusted = max(nodeUsedMemoryRuntimeAdjusted, memoryPoolInfo.getReservedBytes());
                nodesRemainingMemoryRuntimeAdjusted.put(node.getNodeIdentifier(), max(memoryPoolInfo.getMaxBytes() + nodeMemoryOvercommit.toBytes() - nodeUsedMemoryRuntimeAdjusted, 0L));
            }
        }

        public ReserveResult tryReserve(PendingAcquire acquire)
        {
            NodeRequirements requirements = acquire.getNodeRequirements();

            List<InternalNode> candidates;
            Optional<HostAddress> address = requirements.getAddress();
            if (address.isPresent() && (optimizedLocalScheduling || !requirements.isRemotelyAccessible())) {
                Collection<InternalNode> preferred = allNodesByAddress.get(address.get());
                if ((!preferred.isEmpty() && acquire.getNotEnoughResourcesPeriod().compareTo(exhaustedNodeWaitPeriod) < 0) || !requirements.isRemotelyAccessible()) {
                    // use preferred node if available
                    candidates = getCandidatesWithCoordinator(requirements).stream().filter(preferred::contains).collect(toImmutableList());
                }
                else {
                    // use all nodes if we do not have preferences or waited to long
                    candidates = scheduleOnCoordinator ? getCandidatesWithCoordinator(requirements) : getCandidatesExceptCoordinator(requirements);
                }
            }
            else {
                // standard candidates
                candidates = scheduleOnCoordinator ? getCandidatesWithCoordinator(requirements) : getCandidatesExceptCoordinator(requirements);
            }

            if (candidates.isEmpty()) {
                return ReserveResult.NONE_MATCHING;
            }

            candidates = candidates.stream().filter(node -> !nodesWithoutMemory.contains(node.getNodeIdentifier())).collect(toImmutableList());
            if (candidates.isEmpty()) {
                return ReserveResult.NOT_ENOUGH_RESOURCES_NOW;
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

        private List<InternalNode> getCandidatesExceptCoordinator(NodeRequirements requirements)
        {
            return requirements.getCatalogHandle()
                    .map(catalogWorkerNodes::getUnchecked)
                    .orElse(workerNodesSorted);
        }

        private List<InternalNode> getCandidatesWithCoordinator(NodeRequirements requirements)
        {
            return requirements.getCatalogHandle()
                    .map(catalogNodes::getUnchecked)
                    .orElse(allNodesSorted);
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
            if (nodesRemainingMemory.get(nodeIdentifier) == 0) {
                nodesWithoutMemory.add(nodeIdentifier);
            }
        }

        private boolean isNodeEmpty(String nodeIdentifier)
        {
            return nodeMemoryPoolInfos.containsKey(nodeIdentifier)
                    && nodesRemainingMemory.get(nodeIdentifier).equals(nodeMemoryPoolInfos.get(nodeIdentifier).getMaxBytes());
        }

        public enum ReservationStatus
        {
            UNKNOWN,
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

    public static class StatsHolder
    {
        private final AtomicReference<Stats> statsReference = new AtomicReference<>(Stats.ZERO);

        public void updateStats(Stats stats)
        {
            statsReference.set(stats);
        }

        @Managed
        public long getPendingStandardNoneMatching()
        {
            return statsReference.get().pendingStandardNoneMatching();
        }

        @Managed
        public long getPendingStandardNotEnoughResources()
        {
            return statsReference.get().pendingStandardNotEnoughResources();
        }

        @Managed
        public long getPendingStandardUnknown()
        {
            return statsReference.get().pendingStandardUnknown();
        }

        @Managed
        public long getPendingSpeculativeNoneMatching()
        {
            return statsReference.get().pendingSpeculativeNoneMatching();
        }

        @Managed
        public long getPendingSpeculativeNotEnoughResources()
        {
            return statsReference.get().pendingSpeculativeNotEnoughResources();
        }

        @Managed
        public long getPendingSpeculativeUnknown()
        {
            return statsReference.get().pendingSpeculativeUnknown();
        }

        @Managed
        public long getPendingEagerSpeculativeNoneMatching()
        {
            return statsReference.get().pendingEagerSpeculativeNoneMatching();
        }

        @Managed
        public long getPendingEagerSpeculativeNotEnoughResources()
        {
            return statsReference.get().pendingEagerSpeculativeNotEnoughResources();
        }

        @Managed
        public long getPendingEagerSpeculativeUnknown()
        {
            return statsReference.get().pendingEagerSpeculativeUnknown();
        }

        @Managed
        public long getFulfilledStandard()
        {
            return statsReference.get().fulfilledStandard();
        }

        @Managed
        public long getFulfilledSpeculative()
        {
            return statsReference.get().fulfilledSpeculative();
        }

        @Managed
        public long getFulfilledEagerSpeculative()
        {
            return statsReference.get().fulfilledEagerSpeculative();
        }

        @Managed
        public DistributionStat getFulfilledByNodeCountDistribution()
        {
            return statsReference.get().fulfilledByNodeCountDistribution();
        }

        @Managed
        public DistributionStat getFulfilledByNodeMemoryDistribution()
        {
            return statsReference.get().fulfilledByNodeMemoryDistribution();
        }
    }

    private record Stats(
            long pendingStandardNoneMatching,
            long pendingStandardNotEnoughResources,
            long pendingStandardUnknown,
            long pendingSpeculativeNoneMatching,
            long pendingSpeculativeNotEnoughResources,
            long pendingSpeculativeUnknown,
            long pendingEagerSpeculativeNoneMatching,
            long pendingEagerSpeculativeNotEnoughResources,
            long pendingEagerSpeculativeUnknown,
            long fulfilledStandard,
            long fulfilledSpeculative,
            long fulfilledEagerSpeculative,
            DistributionStat fulfilledByNodeCountDistribution,
            DistributionStat fulfilledByNodeMemoryDistribution)
    {
        static final Stats ZERO = new Stats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, new DistributionStat(), new DistributionStat());

        private Stats
        {
            requireNonNull(fulfilledByNodeCountDistribution, "fulfilledByNodeCountDistribution is null");
            requireNonNull(fulfilledByNodeMemoryDistribution, "fulfilledByNodeMemoryDistribution is null");
        }
    }
}
