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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.metadata.InternalNode;
import io.trino.spi.TrinoException;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;

public class FixedCountNodeAllocator
        implements NodeAllocator
{
    private final NodeScheduler nodeScheduler;

    private final Session session;
    private final int maximumAllocationsPerNode;

    @GuardedBy("this")
    private final Map<Optional<CatalogName>, NodeSelector> nodeSelectorCache = new HashMap<>();

    @GuardedBy("this")
    private final Map<InternalNode, Integer> allocationCountMap = new HashMap<>();

    @GuardedBy("this")
    private final LinkedList<PendingAcquire> pendingAcquires = new LinkedList<>();

    public FixedCountNodeAllocator(
            NodeScheduler nodeScheduler,
            Session session,
            int maximumAllocationsPerNode)
    {
        this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
        this.session = requireNonNull(session, "session is null");
        this.maximumAllocationsPerNode = maximumAllocationsPerNode;
    }

    @Override
    public synchronized ListenableFuture<InternalNode> acquire(NodeRequirements requirements)
    {
        try {
            Optional<InternalNode> node = tryAcquireNode(requirements);
            if (node.isPresent()) {
                return immediateFuture(node.get());
            }
        }
        catch (RuntimeException e) {
            return immediateFailedFuture(e);
        }

        SettableFuture<InternalNode> future = SettableFuture.create();
        PendingAcquire pendingAcquire = new PendingAcquire(requirements, future);
        pendingAcquires.add(pendingAcquire);

        return future;
    }

    @Override
    public void release(InternalNode node)
    {
        releaseNodeInternal(node);
        processPendingAcquires();
    }

    @Override
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

        assignedNodes.forEach((pendingAcquire, node) -> {
            SettableFuture<InternalNode> future = pendingAcquire.getFuture();
            future.set(node);
            if (future.isCancelled()) {
                releaseNodeInternal(node);
            }
        });

        failures.forEach((pendingAcquire, failure) -> {
            SettableFuture<InternalNode> future = pendingAcquire.getFuture();
            future.setException(failure);
        });
    }

    @Override
    public synchronized void close()
    {
    }

    private static class PendingAcquire
    {
        private final NodeRequirements nodeRequirements;
        private final SettableFuture<InternalNode> future;

        private PendingAcquire(NodeRequirements nodeRequirements, SettableFuture<InternalNode> future)
        {
            this.nodeRequirements = requireNonNull(nodeRequirements, "nodeRequirements is null");
            this.future = requireNonNull(future, "future is null");
        }

        public NodeRequirements getNodeRequirements()
        {
            return nodeRequirements;
        }

        public SettableFuture<InternalNode> getFuture()
        {
            return future;
        }
    }
}
