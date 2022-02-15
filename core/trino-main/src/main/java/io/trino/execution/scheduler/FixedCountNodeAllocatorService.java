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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.metadata.InternalNode;
import io.trino.spi.TrinoException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.execution.scheduler.NodeInfo.unlimitedMemoryNode;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

/**
 * A simplistic node allocation service which only limits number of allocations per node within each
 * {@link FixedCountNodeAllocator} instance. Each allocator will allow each node to be acquired up to {@link FixedCountNodeAllocatorService#MAXIMUM_ALLOCATIONS_PER_NODE}
 * times at the same time.
 */
@ThreadSafe
public class FixedCountNodeAllocatorService
        implements NodeAllocatorService
{
    private static final Logger log = Logger.get(FixedCountNodeAllocatorService.class);

    // Single FixedCountNodeAllocator will allow for at most MAXIMUM_ALLOCATIONS_PER_NODE.
    // If we reach this state subsequent calls to acquire will return blocked lease.
    private static final int MAXIMUM_ALLOCATIONS_PER_NODE = 1; // TODO make configurable?

    private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, daemonThreadsNamed("fixed-count-node-allocator"));
    private final NodeScheduler nodeScheduler;

    private final Set<FixedCountNodeAllocator> allocators = newConcurrentHashSet();
    private final AtomicBoolean started = new AtomicBoolean();

    @Inject
    public FixedCountNodeAllocatorService(NodeScheduler nodeScheduler)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
    }

    @PostConstruct
    public void start()
    {
        if (!started.compareAndSet(false, true)) {
            // already started
            return;
        }
        executor.scheduleWithFixedDelay(() -> {
            try {
                updateNodes();
            }
            catch (Throwable e) {
                // ignore to avoid getting unscheduled
                log.warn(e, "Error updating nodes");
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdownNow();
    }

    @VisibleForTesting
    void updateNodes()
    {
        allocators.forEach(FixedCountNodeAllocator::updateNodes);
    }

    @Override
    public NodeAllocator getNodeAllocator(Session session)
    {
        requireNonNull(session, "session is null");
        return getNodeAllocator(session, MAXIMUM_ALLOCATIONS_PER_NODE);
    }

    @VisibleForTesting
    NodeAllocator getNodeAllocator(Session session, int maximumAllocationsPerNode)
    {
        FixedCountNodeAllocator allocator = new FixedCountNodeAllocator(session, maximumAllocationsPerNode);
        allocators.add(allocator);
        return allocator;
    }

    private class FixedCountNodeAllocator
            implements NodeAllocator
    {
        private final Session session;
        private final int maximumAllocationsPerNode;

        @GuardedBy("this")
        private final Map<Optional<CatalogName>, NodeSelector> nodeSelectorCache = new HashMap<>();

        @GuardedBy("this")
        private final Map<InternalNode, Integer> allocationCountMap = new HashMap<>();

        @GuardedBy("this")
        private final List<PendingAcquire> pendingAcquires = new LinkedList<>();

        public FixedCountNodeAllocator(
                Session session,
                int maximumAllocationsPerNode)
        {
            this.session = requireNonNull(session, "session is null");
            this.maximumAllocationsPerNode = maximumAllocationsPerNode;
        }

        @Override
        public synchronized ListenableFuture<NodeInfo> acquire(NodeRequirements requirements)
        {
            try {
                Optional<InternalNode> node = tryAcquireNode(requirements);
                if (node.isPresent()) {
                    return immediateFuture(unlimitedMemoryNode(node.get()));
                }
            }
            catch (RuntimeException e) {
                return immediateFailedFuture(e);
            }

            SettableFuture<NodeInfo> future = SettableFuture.create();
            PendingAcquire pendingAcquire = new PendingAcquire(requirements, future);
            pendingAcquires.add(pendingAcquire);

            return future;
        }

        @Override
        public void release(NodeInfo node)
        {
            releaseNodeInternal(node.getNode());
            processPendingAcquires();
        }

        public void updateNodes()
        {
            processPendingAcquires();
        }

        private synchronized Optional<InternalNode> tryAcquireNode(NodeRequirements requirements)
        {
            NodeSelector nodeSelector = nodeSelectorCache.computeIfAbsent(requirements.getCatalogName(), catalogName -> nodeScheduler.createNodeSelector(session, catalogName));

            List<InternalNode> nodes = nodeSelector.allNodes();
            if (nodes.isEmpty()) {
                throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query");
            }

            List<InternalNode> nodesMatchingRequirements = nodes.stream()
                    .filter(node -> requirements.getAddresses().isEmpty() || requirements.getAddresses().contains(node.getHostAndPort()))
                    .collect(toImmutableList());

            if (nodesMatchingRequirements.isEmpty()) {
                throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query");
            }

            Optional<InternalNode> selectedNode = nodesMatchingRequirements.stream()
                    .filter(node -> allocationCountMap.getOrDefault(node, 0) < maximumAllocationsPerNode)
                    .min(comparing(node -> allocationCountMap.getOrDefault(node, 0)));

            if (selectedNode.isEmpty()) {
                return Optional.empty();
            }

            allocationCountMap.compute(selectedNode.get(), (key, value) -> value == null ? 1 : value + 1);
            return selectedNode;
        }

        private synchronized void releaseNodeInternal(InternalNode node)
        {
            int allocationCount = allocationCountMap.compute(node, (key, value) -> value == null ? 0 : value - 1);
            checkState(allocationCount >= 0, "allocation count for node %s is expected to be greater than or equal to zero: %s", node, allocationCount);
        }

        private void processPendingAcquires()
        {
            verify(!Thread.holdsLock(this));

            IdentityHashMap<PendingAcquire, InternalNode> assignedNodes = new IdentityHashMap<>();
            IdentityHashMap<PendingAcquire, RuntimeException> failures = new IdentityHashMap<>();
            synchronized (this) {
                Iterator<PendingAcquire> iterator = pendingAcquires.iterator();
                while (iterator.hasNext()) {
                    PendingAcquire pendingAcquire = iterator.next();
                    if (pendingAcquire.getFuture().isCancelled()) {
                        iterator.remove();
                        continue;
                    }
                    try {
                        Optional<InternalNode> node = tryAcquireNode(pendingAcquire.getNodeRequirements());
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

            // set futures outside of critical section
            assignedNodes.forEach((pendingAcquire, node) -> {
                SettableFuture<NodeInfo> future = pendingAcquire.getFuture();
                future.set(unlimitedMemoryNode(node));
                if (future.isCancelled()) {
                    releaseNodeInternal(node);
                }
            });

            failures.forEach((pendingAcquire, failure) -> {
                SettableFuture<NodeInfo> future = pendingAcquire.getFuture();
                future.setException(failure);
            });
        }

        @Override
        public synchronized void close()
        {
            allocators.remove(this);
        }
    }

    private static class PendingAcquire
    {
        private final NodeRequirements nodeRequirements;
        private final SettableFuture<NodeInfo> future;

        private PendingAcquire(NodeRequirements nodeRequirements, SettableFuture<NodeInfo> future)
        {
            this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
            this.future = requireNonNull(future, "future is null");
        }

        public NodeRequirements getNodeRequirements()
        {
            return nodeRequirements;
        }

        public SettableFuture<NodeInfo> getFuture()
        {
            return future;
        }
    }
}
