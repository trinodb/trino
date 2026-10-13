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
package io.trino.execution;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.execution.scheduler.NodeSchedulerConfig;
import io.trino.node.AllNodes;
import io.trino.node.InternalNode;
import io.trino.node.InternalNodeManager;
import io.trino.spi.TrinoException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ClusterSizeMonitor
{
    private final InternalNodeManager nodeManager;
    private final boolean includeCoordinator;
    private final ScheduledExecutorService executor;

    private final Consumer<AllNodes> listener = this::updateAllNodes;

    // keyed by node group, with an empty key for unrestricted queries
    @GuardedBy("this")
    private Map<Optional<String>, Integer> currentCounts = ImmutableMap.of();

    @GuardedBy("this")
    private final Set<MinNodesFuture> futures = new HashSet<>();

    @Inject
    public ClusterSizeMonitor(InternalNodeManager nodeManager, NodeSchedulerConfig nodeSchedulerConfig)
    {
        this(nodeManager,
                nodeSchedulerConfig.isIncludeCoordinator());
    }

    public ClusterSizeMonitor(
            InternalNodeManager nodeManager,
            boolean includeCoordinator)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.includeCoordinator = includeCoordinator;
        this.executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("node-monitor-%s"));
    }

    @PostConstruct
    public void start()
    {
        nodeManager.addNodeChangeListener(listener);
        updateAllNodes(nodeManager.getAllNodes());
    }

    @PreDestroy
    public void stop()
    {
        nodeManager.removeNodeChangeListener(listener);
        executor.shutdown();
    }

    /**
     * Returns a listener that completes when the minimum number of workers for the cluster has been met.
     * Note: caller should not add a listener using the direct executor, as this can delay the
     * notifications for other listeners.
     */
    public synchronized ListenableFuture<Void> waitForMinimumWorkers(Optional<String> nodeGroup, int executionMinCount, Duration executionMaxWait)
    {
        checkArgument(executionMinCount > 0, "executionMinCount should be greater than 0");
        requireNonNull(nodeGroup, "nodeGroup is null");
        requireNonNull(executionMaxWait, "executionMaxWait is null");

        if (currentCount(nodeGroup) >= executionMinCount) {
            return immediateVoidFuture();
        }

        SettableFuture<Void> future = SettableFuture.create();
        MinNodesFuture minNodesFuture = new MinNodesFuture(nodeGroup, executionMinCount, future);
        futures.add(minNodesFuture);

        // if future does not finish in wait period, complete with an exception
        ScheduledFuture<?> timeoutTask = executor.schedule(
                () -> {
                    synchronized (this) {
                        future.setException(new TrinoException(
                                GENERIC_INSUFFICIENT_RESOURCES,
                                format(
                                        "Insufficient active worker nodes%s. Waited %s for at least %s workers, but only %s workers are active",
                                        nodeGroup.map(" in node group '%s'"::formatted).orElse(""),
                                        executionMaxWait,
                                        executionMinCount,
                                        currentCount(nodeGroup))));
                    }
                },
                executionMaxWait.toMillis(),
                MILLISECONDS);

        // remove future if finished (e.g., canceled, timed out)
        future.addListener(() -> {
            timeoutTask.cancel(true);
            removeFuture(minNodesFuture);
        }, executor);

        return future;
    }

    @GuardedBy("this")
    private int currentCount(Optional<String> nodeGroup)
    {
        return currentCounts.getOrDefault(nodeGroup, 0);
    }

    private synchronized void removeFuture(MinNodesFuture minNodesFuture)
    {
        futures.remove(minNodesFuture);
    }

    private synchronized void updateAllNodes(AllNodes allNodes)
    {
        // recomputed from the whole snapshot; listeners can deliver it out of order or twice
        Set<InternalNode> schedulableNodes = includeCoordinator
                ? allNodes.activeNodes()
                : Sets.difference(allNodes.activeNodes(), allNodes.activeCoordinators());

        Map<Optional<String>, Integer> counts = new HashMap<>();
        counts.put(Optional.empty(), schedulableNodes.size());
        for (InternalNode node : schedulableNodes) {
            for (String nodeGroup : node.getNodeGroups()) {
                counts.merge(Optional.of(nodeGroup), 1, Integer::sum);
            }
        }
        currentCounts = ImmutableMap.copyOf(counts);

        ImmutableList.Builder<SettableFuture<Void>> listenersBuilder = ImmutableList.builder();
        futures.removeIf(minNodesFuture -> {
            if (minNodesFuture.executionMinCount() > currentCount(minNodesFuture.nodeGroup())) {
                return false;
            }
            listenersBuilder.add(minNodesFuture.future());
            return true;
        });
        List<SettableFuture<Void>> listeners = listenersBuilder.build();
        executor.submit(() -> listeners.forEach(listener -> listener.set(null)));
    }

    /**
     * Workers each node group can run a query on. A node in several groups counts in each.
     */
    @Managed
    public synchronized String getActiveWorkerCountByNodeGroup()
    {
        Map<String, Integer> countByNodeGroup = new TreeMap<>();
        currentCounts.forEach((nodeGroup, count) -> nodeGroup.ifPresent(group -> countByNodeGroup.put(group, count)));
        return Joiner.on(", ").withKeyValueSeparator("=").join(countByNodeGroup);
    }

    /**
     * Highest worker count any query is waiting for, across all node groups.
     */
    @Managed
    public synchronized int getRequiredWorkers()
    {
        return futures.stream()
                .mapToInt(MinNodesFuture::executionMinCount)
                .max()
                .orElse(0);
    }

    private record MinNodesFuture(Optional<String> nodeGroup, int executionMinCount, SettableFuture<Void> future)
    {
        MinNodesFuture
        {
            requireNonNull(nodeGroup, "nodeGroup is null");
            requireNonNull(future, "future is null");
        }
    }
}
